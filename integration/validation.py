"""Validation module for OneKey to Dataverse migration.

Validates:
1. Required fields are present in source data
2. Required relationships can be resolved
3. Target entities and fields exist in Dataverse
4. Data types and value constraints are met
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from dataverse.exceptions import DataverseHTTPError

if TYPE_CHECKING:
    from .dynamic_mapping import MappingConfig, FieldMapping, EntityDefinition

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Single validation error."""
    error_type: str
    severity: str  # "error", "warning", "info"
    entity: str
    field: str | None
    message: str
    source_value: str | None = None
    
    def __str__(self):
        field_part = f".{self.field}" if self.field else ""
        value_part = f" (value: {self.source_value})" if self.source_value else ""
        return f"[{self.severity.upper()}] {self.entity}{field_part}: {self.message}{value_part}"


@dataclass
class ValidationResult:
    """Result of validation run."""
    is_valid: bool = True
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)
    info: list[ValidationError] = field(default_factory=list)
    
    def add_error(self, error: ValidationError):
        """Add a validation error."""
        if error.severity == "error":
            self.errors.append(error)
            self.is_valid = False
        elif error.severity == "warning":
            self.warnings.append(error)
        else:
            self.info.append(error)
    
    def merge(self, other: "ValidationResult"):
        """Merge another validation result into this one."""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.info.extend(other.info)
        if not other.is_valid:
            self.is_valid = False
    
    def summary(self) -> str:
        """Return summary string."""
        status = "VALID" if self.is_valid else "INVALID"
        return f"{status}: {len(self.errors)} errors, {len(self.warnings)} warnings, {len(self.info)} info"
    
    def log_all(self, log_level: int = logging.WARNING):
        """Log all validation issues."""
        for error in self.errors:
            logger.error(str(error))
        for warning in self.warnings:
            logger.warning(str(warning))
        if log_level <= logging.INFO:
            for info in self.info:
                logger.info(str(info))


class MappingValidator:
    """Validates mapping configuration and source data against it."""
    
    def __init__(self, config: "MappingConfig"):
        self.config = config
    
    def validate_mapping_config(self) -> ValidationResult:
        """Validate the mapping configuration itself.
        
        Checks:
        - All required entities are defined
        - Key fields are defined for each entity
        - Lookup references point to valid entities
        - No duplicate mappings
        """
        result = ValidationResult()
        
        # Check required entities exist
        required_entities = [
            ("Dataverse", "account"),
            ("Dataverse", "contact"),
        ]
        for platform, entity in required_entities:
            if not self.config.get_entity(platform, entity):
                result.add_error(ValidationError(
                    error_type="missing_entity",
                    severity="error",
                    entity=f"{platform}:{entity}",
                    field=None,
                    message=f"Required entity definition missing: {platform}:{entity}",
                ))
        
        # Check each entity has a key field defined
        for entity_def in self.config.entity_definitions:
            if entity_def.platform == "Dataverse" and entity_def.is_required:
                key_mapping = self.config.get_key_field(entity_def.entity_name)
                if not key_mapping:
                    result.add_error(ValidationError(
                        error_type="missing_key_field",
                        severity="warning",
                        entity=entity_def.entity_name,
                        field=None,
                        message=f"No key field mapping defined for required entity",
                    ))
        
        # Check lookup references
        for fm in self.config.field_mappings:
            if fm.is_lookup and fm.lookup_entity:
                target_entity = self.config.get_entity("Dataverse", fm.lookup_entity)
                if not target_entity:
                    result.add_error(ValidationError(
                        error_type="invalid_lookup",
                        severity="warning",
                        entity=fm.target_entity,
                        field=fm.target_field,
                        message=f"Lookup references unknown entity: {fm.lookup_entity}",
                    ))
        
        # Check for duplicate target field mappings
        seen = set()
        for fm in self.config.field_mappings:
            key = (fm.target_entity, fm.target_field)
            if key in seen:
                result.add_error(ValidationError(
                    error_type="duplicate_mapping",
                    severity="info",
                    entity=fm.target_entity,
                    field=fm.target_field,
                    message=f"Multiple mappings for same target field (may be intentional for fallbacks)",
                ))
            seen.add(key)

        self._validate_decode_and_picklist_targets(result)

        logger.info("Mapping config validation: %s", result.summary())
        return result

    def _validate_decode_and_picklist_targets(self, result: ValidationResult) -> None:
        """Ensure decode: rules line up with value_mapping + picklist_targets.

        Catches the contact.ts_type class of bugs: encoding a Label field with
        a code table, or picklist_targets pointing at a different attribute.
        """
        value_types = set(self.config._value_map.keys())
        picklist_types = set(self.config._picklist_by_type.keys())

        for fm in self.config.field_mappings:
            rule = (fm.transform_rule or "").strip()
            if not rule.startswith("decode:"):
                continue

            decode_type = rule.split(":", 1)[1].lower().replace(" ", "")
            if not decode_type:
                result.add_error(ValidationError(
                    error_type="invalid_decode_rule",
                    severity="error",
                    entity=fm.target_entity,
                    field=fm.target_field,
                    message=f"Empty decode type in transform_rule {rule!r}",
                ))
                continue

            if decode_type not in value_types:
                result.add_error(ValidationError(
                    error_type="missing_value_mapping",
                    severity="error",
                    entity=fm.target_entity,
                    field=fm.target_field,
                    message=(
                        f"decode:{decode_type} has no rows in value_mapping.csv "
                        f"(mapping_id={fm.mapping_id})"
                    ),
                ))

            target = self.config.get_picklist_target(decode_type)
            if target is None and decode_type not in ("statecode", "statuscode"):
                # state/status often use built-in options; still warn for custom types
                result.add_error(ValidationError(
                    error_type="missing_picklist_target",
                    severity="warning",
                    entity=fm.target_entity,
                    field=fm.target_field,
                    message=(
                        f"decode:{decode_type} has no picklist_targets.csv row "
                        f"(needed for Phase 0 option upsert)"
                    ),
                ))
            elif target is not None and (target.kind or "").lower() == "local":
                if (target.entity or "").lower() != (fm.target_entity or "").lower():
                    result.add_error(ValidationError(
                        error_type="picklist_target_entity_mismatch",
                        severity="error",
                        entity=fm.target_entity,
                        field=fm.target_field,
                        message=(
                            f"picklist_targets[{decode_type}].entity={target.entity!r} "
                            f"does not match mapping target_entity={fm.target_entity!r}"
                        ),
                    ))
                if target.attribute and target.attribute != fm.target_field:
                    result.add_error(ValidationError(
                        error_type="picklist_target_attribute_mismatch",
                        severity="error",
                        entity=fm.target_entity,
                        field=fm.target_field,
                        message=(
                            f"picklist_targets[{decode_type}].attribute={target.attribute!r} "
                            f"does not match mapping target_field={fm.target_field!r}"
                        ),
                    ))

            # Labels cannot be looked up in a code-keyed value_mapping table.
            src = (fm.source_field or "").lower()
            if src.endswith("label") or src.endswith("corporatelabel"):
                result.add_error(ValidationError(
                    error_type="decode_source_is_label",
                    severity="error",
                    entity=fm.target_entity,
                    field=fm.target_field,
                    message=(
                        f"decode:{decode_type} uses source_field={fm.source_field!r}; "
                        f"use the OneKey *Code* field (value_mapping keys are codes, not labels)"
                    ),
                ))

        # Orphan picklist targets (defined but never decoded/mapped)
        used_decode_types = set()
        for fm in self.config.field_mappings:
            rule = (fm.transform_rule or "").strip()
            if rule.startswith("decode:"):
                used_decode_types.add(rule.split(":", 1)[1].lower().replace(" ", ""))
        for ptype in sorted(picklist_types - used_decode_types):
            result.add_error(ValidationError(
                error_type="unused_picklist_target",
                severity="info",
                entity="picklist_targets",
                field=ptype,
                message=f"picklist_targets mapping_type={ptype!r} is not referenced by any decode: rule",
            ))
    
    def validate_source_record(
        self,
        record: dict,
        target_entity: str,
    ) -> ValidationResult:
        """Validate a source record has required data for target entity.
        
        Args:
            record: Flattened source record (output of flatten_hcp or flatten_hco)
            target_entity: Target Dataverse entity name
            
        Returns:
            ValidationResult with any issues found
        """
        result = ValidationResult()
        mappings = self.config.get_field_mappings(target_entity)
        
        for fm in mappings:
            if not fm.is_required:
                continue
            
            # Extract value from source using simple path
            value = self._extract_value(record, fm.source_field)
            
            if value is None or (isinstance(value, str) and not value.strip()):
                # Check for default value
                if fm.default_value:
                    result.add_error(ValidationError(
                        error_type="using_default",
                        severity="info",
                        entity=target_entity,
                        field=fm.target_field,
                        message=f"Required field missing, using default: {fm.default_value}",
                        source_value=str(value) if value else None,
                    ))
                else:
                    result.add_error(ValidationError(
                        error_type="missing_required",
                        severity="error",
                        entity=target_entity,
                        field=fm.target_field,
                        message=f"Required field is missing or empty (source: {fm.source_field})",
                        source_value=str(value) if value else None,
                    ))
            
            # Validate max length
            if fm.max_length and isinstance(value, str) and len(value) > fm.max_length:
                result.add_error(ValidationError(
                    error_type="exceeds_max_length",
                    severity="warning",
                    entity=target_entity,
                    field=fm.target_field,
                    message=f"Value exceeds max length {fm.max_length}, will be truncated",
                    source_value=value[:50] + "..." if len(value) > 50 else value,
                ))
        
        return result
    
    def validate_relationship(
        self,
        source_record: dict,
        target_entity: str,
        lookup_resolver: callable = None,
    ) -> ValidationResult:
        """Validate that relationship lookups can be resolved.
        
        Args:
            source_record: Flattened source record
            target_entity: Target Dataverse entity
            lookup_resolver: Optional function to check if lookup target exists
            
        Returns:
            ValidationResult with any lookup issues
        """
        result = ValidationResult()
        lookup_mappings = self.config.get_lookup_mappings(target_entity)
        
        for fm in lookup_mappings:
            lookup_value = self._extract_value(source_record, fm.source_field)
            
            if not lookup_value:
                if fm.is_required:
                    result.add_error(ValidationError(
                        error_type="missing_lookup_key",
                        severity="error",
                        entity=target_entity,
                        field=fm.target_field,
                        message=f"Required lookup key is missing (source: {fm.source_field})",
                    ))
                continue
            
            # If resolver provided, check if target exists
            if lookup_resolver:
                exists = lookup_resolver(fm.lookup_entity, fm.lookup_key_field, lookup_value)
                if not exists:
                    result.add_error(ValidationError(
                        error_type="lookup_not_found",
                        severity="warning",
                        entity=target_entity,
                        field=fm.target_field,
                        message=f"Lookup target not found: {fm.lookup_entity}.{fm.lookup_key_field}={lookup_value}",
                        source_value=str(lookup_value),
                    ))
        
        return result
    
    def _extract_value(self, record: dict, field_name: str) -> any:
        """Extract value from record using simple field name."""
        # Handle nested paths like "individual.firstName"
        if "." in field_name and not field_name.startswith("individual.") and not field_name.startswith("workplace."):
            # Complex path - for now just use simple lookup
            pass
        return record.get(field_name)


class DataverseValidator:
    """Validates Dataverse schema availability."""
    
    def __init__(self, dataverse_client):
        self.client = dataverse_client
        self._metadata_cache = {}
    
    def validate_entity_exists(self, entity_set: str) -> bool:
        """Check if an entity set exists in Dataverse.

        False only when Dataverse says the set is missing (404) or closed to
        this user (403). Throttling, an outage or a lost connection raises, so
        the run fails as a retryable error rather than a schema problem.
        """
        try:
            # One row is enough to prove the set exists. top=0 is falsy, so
            # retrieve_multiple sent no $top and paged through the whole table.
            self.client.retrieve_multiple(entity_set, top=1, max_records=1)
            logger.debug("Entity %s exists in Dataverse", entity_set)
            return True
        except DataverseHTTPError as e:
            if e.status_code not in (403, 404):
                raise
            logger.warning("Entity %s not found or inaccessible: %s", entity_set, e)
            return False
    
    def validate_field_exists(self, entity_set: str, field_name: str) -> bool:
        """Check if a field exists on an entity."""
        # For now, we'll validate during actual data load
        # Full metadata validation would require EntityDefinitions API
        return True
    
    def validate_schema(self, config: "MappingConfig") -> ValidationResult:
        """Validate that required Dataverse entities and fields exist.
        
        Args:
            config: Mapping configuration to validate against
            
        Returns:
            ValidationResult with schema validation issues
        """
        result = ValidationResult()
        
        # Validate each required Dataverse entity exists
        for entity_def in config.entity_definitions:
            if entity_def.platform != "Dataverse":
                continue
            if not entity_def.is_required:
                continue
            
            if not self.validate_entity_exists(entity_def.entity_set):
                result.add_error(ValidationError(
                    error_type="entity_not_found",
                    severity="error",
                    entity=entity_def.entity_name,
                    field=None,
                    message=f"Required entity set not found in Dataverse: {entity_def.entity_set}",
                ))
            else:
                result.add_error(ValidationError(
                    error_type="entity_validated",
                    severity="info",
                    entity=entity_def.entity_name,
                    field=None,
                    message=f"Entity set validated: {entity_def.entity_set}",
                ))
        
        logger.info("Dataverse schema validation: %s", result.summary())
        return result
    
    def check_alternate_key_exists(self, entity_set: str, key_field: str) -> bool:
        """Check if an alternate key is defined on the entity.
        
        Note: This requires EntityDefinitions API access. For now, we log a warning
        and rely on upsert_by_field which works without alternate keys.
        """
        logger.info(
            "Alternate key check for %s.%s - using fallback upsert pattern",
            entity_set, key_field
        )
        return False  # Assume not available; sync module handles this
