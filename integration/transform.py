"""Dynamic data transformation using CSV mapping configuration.

Applies transform rules from entity_mapping.csv to convert
OneKey records into Dataverse payloads.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .dynamic_mapping import FieldMapping, MappingConfig

logger = logging.getLogger(__name__)


class TransformEngine:
    """Transforms OneKey records to Dataverse payloads using mapping config."""
    
    def __init__(self, config: "MappingConfig"):
        self.config = config
    
    def transform_hco(
        self,
        source: dict,
        include_address: bool = False,
        parent_account_id: str = None,
    ) -> dict:
        """Transform OneKey HCO (flattened) to Dataverse Account payload.
        
        Args:
            source: Flattened HCO record from onekey.parse.flatten_hco
            include_address: Whether to include address fields
            parent_account_id: GUID of parent account for hierarchy
            
        Returns:
            dict: Dataverse Web API payload for account
        """
        payload = {}
        description_parts = []
        
        mappings = self.config.get_field_mappings("account")
        
        for fm in mappings:
            # Skip address fields if not requested
            if not include_address and fm.target_field.startswith("address1_"):
                continue
            
            # Extract source value
            value = self._extract_value(source, fm.source_field)
            
            # Apply transform rule
            transformed = self._apply_transform(value, fm, source)
            
            if transformed is None:
                continue
            
            # Handle description aggregation
            if fm.target_field == "description" and fm.transform_rule.startswith("template:"):
                if transformed:
                    description_parts.append(transformed)
                continue
            
            # Apply max length
            if fm.max_length and isinstance(transformed, str):
                transformed = transformed[:fm.max_length]
            
            payload[fm.target_field] = transformed
        
        # Build description from parts
        if description_parts:
            existing = payload.get("description", "")
            all_parts = ([existing] if existing else []) + description_parts
            payload["description"] = "\n".join(p for p in all_parts if p)
        
        # Handle parent account binding
        if parent_account_id:
            payload["parentaccountid@odata.bind"] = f"/accounts({parent_account_id})"
        
        # Account name is mandatory; honour mapping default when still empty.
        if not (payload.get("name") or "").strip():
            name_mapping = self.config.get_field_mappings("account")
            default_name = next(
                (fm.default_value for fm in name_mapping if fm.target_field == "name"),
                None,
            )
            payload["name"] = (default_name or "noname")[:160]
        
        # Remove None values
        return {k: v for k, v in payload.items() if v is not None}
    
    def transform_hcp(
        self,
        source: dict,
        include_address: bool = False,
        account_id: str = None,
    ) -> dict:
        """Transform OneKey HCP (flattened) to Dataverse Contact payload.
        
        Args:
            source: Flattened HCP record from onekey.parse.flatten_hcp
            include_address: Whether to include address fields
            account_id: GUID of parent account to link
            
        Returns:
            dict: Dataverse Web API payload for contact
        """
        payload = {}
        description_parts = []
        jobtitle_parts = []
        
        mappings = self.config.get_field_mappings("contact")
        
        for fm in mappings:
            # Skip address fields if not requested
            if not include_address and fm.target_field.startswith("address1_"):
                continue
            
            # Skip lookup field - handled separately
            if fm.target_field == "parentcustomerid":
                continue
            
            # Extract source value
            value = self._extract_value(source, fm.source_field)
            
            # Apply transform rule
            transformed = self._apply_transform(value, fm, source)
            
            if transformed is None:
                continue
            
            # Handle description aggregation
            if fm.target_field == "description" and fm.transform_rule.startswith("template:"):
                if transformed:
                    description_parts.append(transformed)
                continue
            
            # Handle jobtitle aggregation (append specialty to type)
            if fm.target_field == "jobtitle":
                if fm.transform_rule.startswith("append:"):
                    if transformed:
                        jobtitle_parts.append(("append", transformed))
                else:
                    jobtitle_parts.insert(0, ("base", transformed))
                continue
            
            # Apply max length
            if fm.max_length and isinstance(transformed, str):
                transformed = transformed[:fm.max_length]
            
            payload[fm.target_field] = transformed
        
        # Build jobtitle from parts
        if jobtitle_parts:
            base = next((v for t, v in jobtitle_parts if t == "base"), None)
            appendages = [v for t, v in jobtitle_parts if t == "append"]
            if base:
                jobtitle = base
                for a in appendages:
                    jobtitle = f"{jobtitle} - {a}"
                payload["jobtitle"] = jobtitle[:100]  # Max length
        
        # Build description from parts
        if description_parts:
            existing = payload.get("description", "")
            all_parts = ([existing] if existing else []) + description_parts
            payload["description"] = "\n".join(p for p in all_parts if p)
        
        # Handle account binding
        if account_id:
            payload["parentcustomerid_account@odata.bind"] = f"/accounts({account_id})"
        
        # Ensure lastname has a value (mandatory field)
        if not payload.get("lastname"):
            fallback = source.get("individualEid") or "Unknown"
            payload["lastname"] = fallback[:50]
            logger.warning(
                "Contact %s missing lastname, using fallback: %s",
                source.get("individualEid"), fallback
            )
        
        # Remove None values
        return {k: v for k, v in payload.items() if v is not None}
    
    def _extract_value(self, source: dict, field_name: str) -> Any:
        """Extract value from source record.
        
        Handles simple field names and nested paths.
        """
        if not field_name:
            return None
        
        # Handle nested paths like "individual.firstName"
        # For now, flatten_hcp/flatten_hco already flattens these
        # So we just do direct lookup
        
        # Remove entity prefix if present (for clarity in CSV)
        clean_name = field_name
        for prefix in ["individual.", "workplace.", "activity.", "address."]:
            if clean_name.startswith(prefix):
                clean_name = clean_name[len(prefix):]
                break
        
        return source.get(clean_name) or source.get(field_name)
    
    def _apply_transform(
        self,
        value: Any,
        mapping: "FieldMapping",
        source: dict,
    ) -> Any:
        """Apply transform rule to a value.
        
        Transform rules:
        - direct: Pass through as-is
        - coalesce:field: Use value, or fallback to another field
        - fallback_for:field: Only used if primary field is empty (skip otherwise)
        - map:A=1|B=2: Map source value to target value
        - template:{value}: Format string with {value} placeholder
        - decode:TYPE: Decode using value_mapping.csv
        - append: - {value}: Append to another field's value
        - or:field: Boolean OR with another field
        """
        rule = mapping.transform_rule or "direct"
        
        if rule == "direct":
            return value
        
        if rule.startswith("coalesce:"):
            if value is not None and (not isinstance(value, str) or value.strip()):
                return value
            fallback_field = rule.split(":", 1)[1]
            fallback = self._extract_value(source, fallback_field)
            if fallback is not None and (not isinstance(fallback, str) or fallback.strip()):
                return fallback
            return mapping.default_value
        
        if rule.startswith("fallback_for:"):
            # This field is a fallback - only return if checking for the primary
            # The primary field's coalesce rule will pick this up
            return None
        
        if rule.startswith("map:"):
            if value is None:
                return mapping.default_value
            map_str = rule.split(":", 1)[1]
            value_map = {}
            for pair in map_str.split("|"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    value_map[k] = v
            return value_map.get(str(value), mapping.default_value)
        
        if rule.startswith("template:"):
            if value is None:
                return None
            template = rule.split(":", 1)[1]
            return template.replace("{value}", str(value))
        
        if rule.startswith("decode:"):
            if value is None:
                return mapping.default_value
            decode_type = rule.split(":", 1)[1].lower()
            # Use value mapping lookup
            decoded = self.config.get_value(decode_type, str(value))
            return decoded or mapping.default_value
        
        if rule.startswith("append:"):
            # Value will be appended to another field
            if value is None:
                return None
            return str(value)
        
        if rule.startswith("or:"):
            other_field = rule.split(":", 1)[1]
            other_value = self._extract_value(source, other_field)
            return bool(value) or bool(other_value)

        if rule.startswith("constant:"):
            return rule.split(":", 1)[1]

        if rule.startswith("fallback_for:"):
            # Secondary mapping: only used when primary field is empty (handled by caller)
            return value

        # Lookups are resolved by DynamicSyncEngine before transform; skip here.
        if rule in ("lookup_by_key", "lookup"):
            return None
        
        # Unknown rule - pass through
        logger.warning("Unknown transform rule: %s", rule)
        return value
    
    def get_key_value(self, source: dict, target_entity: str) -> str | None:
        """Get the key field value for upsert matching.
        
        Args:
            source: Flattened source record
            target_entity: Target Dataverse entity ("account" or "contact")
            
        Returns:
            Key value for matching, or None if not found
        """
        key_mapping = self.config.get_key_field(target_entity)
        if not key_mapping:
            logger.error("No key field defined for %s", target_entity)
            return None
        
        value = self._extract_value(source, key_mapping.source_field)
        if value and key_mapping.max_length:
            value = str(value)[:key_mapping.max_length]
        
        return value


def create_transform_engine(config: "MappingConfig") -> TransformEngine:
    """Create a transform engine with the given configuration."""
    return TransformEngine(config)
