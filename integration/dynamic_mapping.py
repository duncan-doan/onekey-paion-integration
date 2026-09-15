"""Dynamic field mapping loaded from CSV configuration files.

This module replaces hardcoded mapping with CSV-driven configuration,
enabling non-developers to modify mappings without code changes.

CSV Files:
    config/entity_mapping.csv      - Field-level mapping rules
    config/relationship_mapping.csv - Entity relationship definitions
    config/entity_definition.csv   - Entity metadata for validation
    config/value_mapping.csv       - Picklist/code value transformations
"""

from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Default config directory relative to this file
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


@dataclass
class FieldMapping:
    """Single field mapping rule from source to target."""
    mapping_id: str
    source_platform: str
    source_entity: str
    source_field: str
    source_field_path: str
    target_platform: str
    target_entity: str
    target_field: str
    target_field_type: str
    max_length: int | None
    is_required: bool
    is_key: bool
    is_lookup: bool
    lookup_entity: str | None
    lookup_key_field: str | None
    transform_rule: str
    default_value: str | None
    notes: str


@dataclass
class RelationshipMapping:
    """Entity relationship definition."""
    relationship_id: str
    relationship_name: str
    source_entity: str
    source_field: str
    target_entity: str
    target_field: str
    relationship_type: str
    lookup_key_field: str
    is_required: bool
    load_order: int
    notes: str
    description: str


@dataclass
class EntityDefinition:
    """Entity metadata for validation."""
    entity_id: str
    platform: str
    entity_name: str
    entity_set: str
    primary_key: str
    alternate_key: str  # "Yes" or "No"
    alternate_key_field: str | None
    is_required: bool
    load_order: int
    parent_entity: str | None
    description: str


@dataclass
class ValueMapping:
    """Value transformation rule for picklists and codes."""
    mapping_id: str
    mapping_type: str
    source_code: str
    source_value: str
    target_value: str
    target_label: str
    notes: str


@dataclass
class FieldConstraint:
    """Field constraint for validation."""
    constraint_id: str
    platform: str
    entity: str
    field: str
    constraint_type: str  # "max_length", "required", "pattern"
    constraint_value: str
    error_action: str  # "truncate", "error", "warn"
    notes: str


@dataclass
class MappingConfig:
    """Complete mapping configuration loaded from CSVs."""
    field_mappings: list[FieldMapping] = field(default_factory=list)
    relationship_mappings: list[RelationshipMapping] = field(default_factory=list)
    entity_definitions: list[EntityDefinition] = field(default_factory=list)
    value_mappings: list[ValueMapping] = field(default_factory=list)
    field_constraints: list[FieldConstraint] = field(default_factory=list)
    
    # Indexes for fast lookup
    _field_by_target: dict[str, list[FieldMapping]] = field(default_factory=dict)
    _value_map: dict[str, dict[str, str]] = field(default_factory=dict)
    _entity_by_name: dict[str, EntityDefinition] = field(default_factory=dict)
    _constraints_by_field: dict[str, list[FieldConstraint]] = field(default_factory=dict)
    
    def build_indexes(self):
        """Build lookup indexes after loading."""
        # Field mappings by target entity
        self._field_by_target = {}
        for fm in self.field_mappings:
            key = fm.target_entity.lower()
            self._field_by_target.setdefault(key, []).append(fm)
        
        # Value mappings by type - use source_code as key
        self._value_map = {}
        for vm in self.value_mappings:
            self._value_map.setdefault(vm.mapping_type, {})[vm.source_code] = vm.target_value
        
        # Entity definitions by name
        self._entity_by_name = {
            f"{ed.platform}:{ed.entity_name}".lower(): ed
            for ed in self.entity_definitions
        }
        
        # Field constraints by entity.field
        self._constraints_by_field = {}
        for fc in self.field_constraints:
            key = f"{fc.entity}.{fc.field}".lower()
            self._constraints_by_field.setdefault(key, []).append(fc)
        
        logger.info(
            "Mapping indexes built: %d field mappings, %d value maps, %d entities, %d constraints",
            len(self.field_mappings), len(self._value_map), len(self._entity_by_name),
            len(self.field_constraints)
        )
    
    def get_field_mappings(self, target_entity: str) -> list[FieldMapping]:
        """Get all field mappings for a target entity."""
        return self._field_by_target.get(target_entity.lower(), [])
    
    def get_value(self, mapping_type: str, source_value: str, default: str = None) -> str | None:
        """Lookup a value transformation."""
        return self._value_map.get(mapping_type, {}).get(str(source_value), default)
    
    def get_entity(self, platform: str, entity_name: str) -> EntityDefinition | None:
        """Get entity definition by platform and name."""
        return self._entity_by_name.get(f"{platform}:{entity_name}".lower())
    
    def get_required_fields(self, target_entity: str) -> list[FieldMapping]:
        """Get required field mappings for a target entity."""
        return [fm for fm in self.get_field_mappings(target_entity) if fm.is_required]
    
    def get_key_field(self, target_entity: str) -> FieldMapping | None:
        """Get the key field mapping for a target entity."""
        for fm in self.get_field_mappings(target_entity):
            if fm.is_key:
                return fm
        return None
    
    def get_lookup_mappings(self, target_entity: str) -> list[FieldMapping]:
        """Get lookup field mappings for a target entity."""
        return [fm for fm in self.get_field_mappings(target_entity) if fm.is_lookup]
    
    def get_max_length(self, entity: str, field: str) -> int | None:
        """Get max length constraint for a field."""
        key = f"{entity}.{field}".lower()
        for fc in self._constraints_by_field.get(key, []):
            if fc.constraint_type == "max_length":
                try:
                    return int(fc.constraint_value)
                except ValueError:
                    pass
        return None
    
    def is_field_required(self, entity: str, field: str) -> bool:
        """Check if a field is required."""
        key = f"{entity}.{field}".lower()
        for fc in self._constraints_by_field.get(key, []):
            if fc.constraint_type == "required":
                return fc.constraint_value.lower() == "true"
        return False


def _parse_bool(value: str) -> bool:
    """Parse boolean from CSV string."""
    return value.lower() in ("true", "1", "yes", "y")


def _parse_int(value: str, default: int = None) -> int | None:
    """Parse integer from CSV string."""
    if not value or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def load_field_mappings(csv_path: Path) -> list[FieldMapping]:
    """Load field mappings from CSV file."""
    mappings = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip comment rows
            if row.get("mapping_id", "").startswith("#"):
                continue
            if not row.get("mapping_id"):
                continue
            
            mappings.append(FieldMapping(
                mapping_id=row["mapping_id"],
                source_platform=row["source_platform"],
                source_entity=row["source_entity"],
                source_field=row["source_field"],
                source_field_path=row["source_field_path"],
                target_platform=row["target_platform"],
                target_entity=row["target_entity"],
                target_field=row["target_field"],
                target_field_type=row["target_field_type"],
                max_length=_parse_int(row.get("max_length", "")),
                is_required=_parse_bool(row.get("is_required", "false")),
                is_key=_parse_bool(row.get("is_key", "false")),
                is_lookup=_parse_bool(row.get("is_lookup", "false")),
                lookup_entity=row.get("lookup_entity") or None,
                lookup_key_field=row.get("lookup_key_field") or None,
                transform_rule=row.get("transform_rule", "direct"),
                default_value=row.get("default_value") or None,
                notes=row.get("notes", ""),
            ))
    
    logger.info("Loaded %d field mappings from %s", len(mappings), csv_path)
    return mappings


def load_relationship_mappings(csv_path: Path) -> list[RelationshipMapping]:
    """Load relationship mappings from CSV file."""
    mappings = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("relationship_id", "").startswith("#"):
                continue
            if not row.get("relationship_id"):
                continue
            
            mappings.append(RelationshipMapping(
                relationship_id=row["relationship_id"],
                relationship_name=row["relationship_name"],
                source_entity=row["source_entity"],
                source_field=row["source_field"],
                target_entity=row["target_entity"],
                target_field=row["target_field"],
                relationship_type=row["relationship_type"],
                lookup_key_field=row["lookup_key_field"],
                is_required=_parse_bool(row.get("is_required", "false")),
                load_order=_parse_int(row.get("load_order", "0"), 0),
                notes=row.get("notes", ""),
                description=row.get("description", ""),
            ))
    
    logger.info("Loaded %d relationship mappings from %s", len(mappings), csv_path)
    return mappings


def load_entity_definitions(csv_path: Path) -> list[EntityDefinition]:
    """Load entity definitions from CSV file."""
    definitions = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("entity_id", "").startswith("#"):
                continue
            if not row.get("entity_id"):
                continue
            
            definitions.append(EntityDefinition(
                entity_id=row["entity_id"],
                platform=row["platform"],
                entity_name=row["entity_name"],
                entity_set=row.get("entity_set", ""),
                primary_key=row.get("primary_key", ""),
                alternate_key=row.get("alternate_key", "No"),
                alternate_key_field=row.get("alternate_key_field") or None,
                is_required=_parse_bool(row.get("is_required", "false")),
                load_order=_parse_int(row.get("load_order", "0"), 0),
                parent_entity=row.get("parent_entity") or None,
                description=row.get("description", ""),
            ))
    
    logger.info("Loaded %d entity definitions from %s", len(definitions), csv_path)
    return definitions


def load_value_mappings(csv_path: Path) -> list[ValueMapping]:
    """Load value mappings from CSV file."""
    mappings = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("mapping_id", "").startswith("#"):
                continue
            if not row.get("mapping_id"):
                continue
            
            mappings.append(ValueMapping(
                mapping_id=row["mapping_id"],
                mapping_type=row["mapping_type"],
                source_code=row.get("source_code", ""),
                source_value=row.get("source_value", ""),
                target_value=row.get("target_value", ""),
                target_label=row.get("target_label", ""),
                notes=row.get("notes", ""),
            ))
    
    logger.info("Loaded %d value mappings from %s", len(mappings), csv_path)
    return mappings


def load_field_constraints(csv_path: Path) -> list[FieldConstraint]:
    """Load field constraints from CSV file."""
    constraints = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("constraint_id", "").startswith("#"):
                continue
            if not row.get("constraint_id"):
                continue
            
            constraints.append(FieldConstraint(
                constraint_id=row["constraint_id"],
                platform=row["platform"],
                entity=row["entity"],
                field=row["field"],
                constraint_type=row["constraint_type"],
                constraint_value=row["constraint_value"],
                error_action=row.get("error_action", "warn"),
                notes=row.get("notes", ""),
            ))
    
    logger.info("Loaded %d field constraints from %s", len(constraints), csv_path)
    return constraints


def load_mapping_config(config_dir: Path = None) -> MappingConfig:
    """Load complete mapping configuration from CSV files.
    
    Args:
        config_dir: Directory containing CSV files. Defaults to ../config.
        
    Returns:
        MappingConfig with all mappings loaded and indexed.
    """
    config_dir = config_dir or CONFIG_DIR
    config_dir = Path(config_dir)
    
    if not config_dir.exists():
        raise FileNotFoundError(f"Config directory not found: {config_dir}")
    
    config = MappingConfig()
    
    # Load each CSV file
    entity_mapping_path = config_dir / "entity_mapping.csv"
    if entity_mapping_path.exists():
        config.field_mappings = load_field_mappings(entity_mapping_path)
    else:
        logger.warning("entity_mapping.csv not found at %s", entity_mapping_path)
    
    relationship_path = config_dir / "relationship_mapping.csv"
    if relationship_path.exists():
        config.relationship_mappings = load_relationship_mappings(relationship_path)
    else:
        logger.warning("relationship_mapping.csv not found at %s", relationship_path)
    
    entity_def_path = config_dir / "entity_definition.csv"
    if entity_def_path.exists():
        config.entity_definitions = load_entity_definitions(entity_def_path)
    else:
        logger.warning("entity_definition.csv not found at %s", entity_def_path)
    
    value_mapping_path = config_dir / "value_mapping.csv"
    if value_mapping_path.exists():
        config.value_mappings = load_value_mappings(value_mapping_path)
    else:
        logger.warning("value_mapping.csv not found at %s", value_mapping_path)
    
    constraint_path = config_dir / "field_constraints.csv"
    if constraint_path.exists():
        config.field_constraints = load_field_constraints(constraint_path)
    else:
        logger.warning("field_constraints.csv not found at %s", constraint_path)
    
    # Build lookup indexes
    config.build_indexes()
    
    return config
