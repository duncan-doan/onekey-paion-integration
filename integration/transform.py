"""Dynamic data transformation using CSV mapping configuration.

Applies transform rules from entity_mapping.csv to convert
OneKey records into Dataverse payloads.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .picklist_gaps import PicklistGapCollector

if TYPE_CHECKING:
    from .dynamic_mapping import FieldMapping, MappingConfig

logger = logging.getLogger(__name__)


class TransformEngine:
    """Transforms OneKey records to Dataverse payloads using mapping config."""
    
    def __init__(self, config: "MappingConfig"):
        self.config = config
        self.picklist_gaps = PicklistGapCollector()
        self._current_entity: str = ""
        self._current_eid: str = ""
    
    def begin_record(self, target_entity: str, source_eid: str | None = None) -> None:
        """Set context used when recording picklist gaps for the next transform."""
        self._current_entity = target_entity or ""
        self._current_eid = source_eid or ""

    def transform_hco(
        self,
        source: dict,
        parent_account_id: str = None,
        **_deprecated,
    ) -> dict:
        """Transform OneKey HCO (flattened) to Dataverse Account payload.

        Address fields go to ``customeraddress`` (never ``address1_*``).
        Parent hierarchy is normally applied in a separate phase; pass
        ``parent_account_id`` only when binding inline.
        """
        if "include_address" in _deprecated:
            logger.debug("include_address is ignored; use customeraddress phase")
        self.begin_record("account", source.get("workplaceEid"))
        payload = {}
        description_parts = []

        mappings = self.config.get_field_mappings("account")

        for fm in mappings:
            if fm.target_field.startswith("address1_"):
                continue
            if fm.target_field == "parentaccountid":
                continue

            value = self._extract_value(source, fm.source_field)
            transformed = self._apply_transform(value, fm, source)

            if transformed is None:
                continue

            if fm.target_field == "description" and fm.transform_rule.startswith("template:"):
                if transformed:
                    description_parts.append(transformed)
                continue

            if fm.max_length and isinstance(transformed, str):
                transformed = transformed[:fm.max_length]

            payload[fm.target_field] = transformed

        if description_parts:
            existing = payload.get("description", "")
            all_parts = ([existing] if existing else []) + description_parts
            payload["description"] = "\n".join(p for p in all_parts if p)

        if parent_account_id:
            payload["parentaccountid@odata.bind"] = f"/accounts({parent_account_id})"

        if not (payload.get("name") or "").strip():
            name_mapping = self.config.get_field_mappings("account")
            default_name = next(
                (fm.default_value for fm in name_mapping if fm.target_field == "name"),
                None,
            )
            payload["name"] = (default_name or "noname")[:160]

        return {k: v for k, v in payload.items() if v is not None}

    def transform_hcp(
        self,
        source: dict,
        account_id: str = None,
        **_deprecated,
    ) -> dict:
        """Transform OneKey HCP (flattened) to Dataverse Contact payload.

        Address fields go to ``customeraddress`` (never ``address1_*``).
        """
        if "include_address" in _deprecated:
            logger.debug("include_address is ignored; use customeraddress phase")
        self.begin_record("contact", source.get("individualEid"))
        payload = {}
        description_parts = []
        jobtitle_parts = []

        mappings = self.config.get_field_mappings("contact")

        for fm in mappings:
            if fm.target_field.startswith("address1_"):
                continue
            if fm.target_field == "parentcustomerid":
                continue

            value = self._extract_value(source, fm.source_field)
            transformed = self._apply_transform(value, fm, source)

            if transformed is None:
                continue

            if fm.target_field == "description" and fm.transform_rule.startswith("template:"):
                if transformed:
                    description_parts.append(transformed)
                continue

            if fm.target_field == "jobtitle":
                if fm.transform_rule.startswith("append:"):
                    if transformed:
                        jobtitle_parts.append(("append", transformed))
                else:
                    jobtitle_parts.insert(0, ("base", transformed))
                continue

            if fm.max_length and isinstance(transformed, str):
                transformed = transformed[:fm.max_length]

            payload[fm.target_field] = transformed

        if jobtitle_parts:
            base = next((v for t, v in jobtitle_parts if t == "base"), None)
            appendages = [v for t, v in jobtitle_parts if t == "append"]
            if base:
                jobtitle = base
                for a in appendages:
                    jobtitle = f"{jobtitle} - {a}"
                payload["jobtitle"] = jobtitle[:100]

        if description_parts:
            existing = payload.get("description", "")
            all_parts = ([existing] if existing else []) + description_parts
            payload["description"] = "\n".join(p for p in all_parts if p)

        if account_id:
            payload["parentcustomerid_account@odata.bind"] = f"/accounts({account_id})"

        if not payload.get("lastname"):
            fallback = source.get("individualEid") or "Unknown"
            payload["lastname"] = fallback[:50]
            logger.warning(
                "Contact %s missing lastname, using fallback: %s",
                source.get("individualEid"), fallback
            )

        return {k: v for k, v in payload.items() if v is not None}

    def transform_address(
        self,
        source: dict,
        parent_account_id: str = None,
        slot: str = "P_1",
    ) -> dict | None:
        """Transform one HCO–address link to a Dataverse ``customeraddress`` payload.

        OneKey Address↔Workplace is N-N (a physical address can attach to many
        HCOs). Dataverse keeps Account→CustomerAddress as 1-n, so a shared
        OneKey address is **duplicated** once per HCO. Upsert identity is the
        composite key::

            cr4ec_externalid = ``{workplaceEid} - {addressEid}``

        Args:
            source: Flattened address row (e.g. from :func:`onekey.parse.flatten_hco`)
                with ``workplaceEid`` and ``addressEid`` / ``address_addressEid``.
            parent_account_id: GUID of the parent ``account`` for ``parentid`` bind.
            slot: Which ``workplaceAddresses`` slot mappings to apply (default ``P_1``).

        Returns:
            Web API payload, or ``None`` when either OneKey id is missing.
        """
        self.begin_record("customeraddress", source.get("workplaceEid"))
        workplace_eid = self._address_workplace_eid(source)
        address_eid = self._address_eid(source)
        if not workplace_eid or not address_eid:
            logger.debug(
                "Skipping address transform: workplaceEid=%r addressEid=%r",
                workplace_eid,
                address_eid,
            )
            return None

        normalized = dict(source)
        normalized["workplaceEid"] = workplace_eid
        normalized["addressEid"] = address_eid
        normalized.setdefault("address_addressEid", address_eid)

        payload: dict[str, Any] = {}
        key_parts: list[tuple[str, str]] = []
        slot_token = f"workplaceAddresses.{slot}"

        for fm in self.config.get_field_mappings("customeraddress"):
            path = fm.source_field_path or ""
            # Skip other address slots so P_2/P_3/P_4 do not overwrite P_1.
            if "workplaceAddresses." in path and slot_token not in path:
                continue

            if fm.target_field == "parentid":
                continue

            value = self._extract_value(normalized, fm.source_field)
            # Flattened HCO rows use address_* for the primary address eid.
            if value is None and fm.source_field == "addressEid":
                value = address_eid
            if value is None and fm.source_field == "workplaceEid":
                value = workplace_eid

            transformed = self._apply_transform(value, fm, normalized)
            if transformed is None:
                continue

            if fm.target_field == "cr4ec_externalid":
                kind = "append" if (fm.transform_rule or "").startswith("append:") else "base"
                # workplaceEid base rows are repeated per address slot without a
                # slot token in source_field_path — keep only the first base.
                if kind == "base" and any(k == "base" for k, _ in key_parts):
                    continue
                key_parts.append((kind, str(transformed)))
                continue

            # Non-slot target fields (name, line1, …) may also repeat across
            # ADR blocks that omitted the slot in the path; prefer first write.
            if fm.target_field in payload and "workplaceAddresses." not in path:
                continue

            if fm.max_length and isinstance(transformed, str):
                transformed = transformed[: fm.max_length]

            payload[fm.target_field] = transformed

        composed = self._compose_address_external_id_from_parts(key_parts, workplace_eid, address_eid)
        key_max = next(
            (
                fm.max_length
                for fm in self.config.get_field_mappings("customeraddress")
                if fm.target_field == "cr4ec_externalid" and fm.max_length
            ),
            100,
        )
        payload["cr4ec_externalid"] = composed[:key_max]

        if parent_account_id:
            payload["parentid_account@odata.bind"] = f"/accounts({parent_account_id})"

        # name is required on customeraddress; prefer building, then street/city.
        if not (payload.get("name") or "").strip():
            fallback_name = (
                normalized.get("address_buildingLabel")
                or normalized.get("buildingLabel")
                or payload.get("line1")
                or payload.get("city")
                or composed
            )
            payload["name"] = str(fallback_name)[:250]

        # Picklist codes arrive as strings from value_mapping.csv.
        type_code = payload.get("addresstypecode")
        if isinstance(type_code, str) and type_code.isdigit():
            payload["addresstypecode"] = int(type_code)

        return {k: v for k, v in payload.items() if v is not None}

    @staticmethod
    def _address_workplace_eid(source: dict) -> str | None:
        value = source.get("workplaceEid")
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        return str(value).strip()

    @staticmethod
    def _address_eid(source: dict) -> str | None:
        value = source.get("addressEid") or source.get("address_addressEid")
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        return str(value).strip()

    @staticmethod
    def _compose_address_external_id_from_parts(
        key_parts: list[tuple[str, str]],
        workplace_eid: str,
        address_eid: str,
    ) -> str:
        """Build ``{workplaceEid} - {addressEid}`` from mapping parts or fallbacks."""
        base = next((v for kind, v in key_parts if kind == "base"), None)
        appendages = [v for kind, v in key_parts if kind == "append"]
        if base and appendages:
            key = base
            for part in appendages:
                key = f"{key} - {part}"
            return key
        if base and not appendages:
            return f"{base} - {address_eid}"
        return f"{workplace_eid} - {address_eid}"

    def compose_address_external_id(self, source: dict) -> str | None:
        """Return the composite address upsert key, or None if ids are incomplete."""
        workplace_eid = self._address_workplace_eid(source)
        address_eid = self._address_eid(source)
        if not workplace_eid or not address_eid:
            return None
        return f"{workplace_eid} - {address_eid}"
    
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
            mapped = value_map.get(str(value))
            if mapped is not None:
                return mapped
            if mapping.default_value is not None:
                return mapping.default_value
            self.picklist_gaps.record(
                f"map:{mapping.target_field}",
                str(value),
                target_field=mapping.target_field,
                target_entity=self._current_entity or mapping.target_entity,
                source_eid=self._current_eid,
                transform_rule=rule,
            )
            return None
        
        if rule.startswith("template:"):
            if value is None:
                return None
            template = rule.split(":", 1)[1]
            return template.replace("{value}", str(value))
        
        if rule.startswith("decode:"):
            if value is None:
                return mapping.default_value
            decode_type = rule.split(":", 1)[1].lower().replace(" ", "")
            decoded = self.config.get_value(decode_type, str(value))
            if decoded is not None:
                return decoded
            if mapping.default_value is not None:
                return mapping.default_value
            # Quarantine: omit field; do not invent option integers at runtime.
            self.picklist_gaps.record(
                decode_type,
                str(value),
                target_field=mapping.target_field,
                target_entity=self._current_entity or mapping.target_entity,
                source_eid=self._current_eid,
                transform_rule=rule,
            )
            return None
        
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
            target_entity: Target Dataverse entity ("account", "contact", or
                "customeraddress")
            
        Returns:
            Key value for matching, or None if not found
        """
        if target_entity.lower() == "customeraddress":
            composed = self.compose_address_external_id(source)
            if not composed:
                return None
            key_mapping = self.config.get_key_field(target_entity)
            max_length = key_mapping.max_length if key_mapping and key_mapping.max_length else 100
            return composed[:max_length]

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
