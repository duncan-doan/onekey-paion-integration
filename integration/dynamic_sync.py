"""Dynamic sync engine using CSV-based mapping configuration.

This module orchestrates the full data migration and delta sync
from OneKey to Dataverse with:
- Dynamic field mapping from CSV
- Comprehensive validation
- Detailed logging
- Relationship resolution
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .dynamic_mapping import load_mapping_config, MappingConfig
from .transform import TransformEngine
from .validation import MappingValidator, DataverseValidator, ValidationResult
from .sync_logger import SyncLogger, setup_logging
from onekey.parse import flatten_hcp, flatten_hco

if TYPE_CHECKING:
    from onekey import OneKeyClient
    from dataverse import DataverseClient

logger = logging.getLogger(__name__)


class DynamicSyncEngine:
    """Orchestrates OneKey to Dataverse synchronization.
    
    Features:
    - CSV-driven field mapping (no hardcoded transforms)
    - Pre-sync validation of schema and mappings
    - Depth-first HCO hierarchy loading
    - HCP loading with account relationship resolution
    - Comprehensive logging at every step
    """
    
    def __init__(
        self,
        onekey_client: "OneKeyClient",
        dataverse_client: "DataverseClient",
        config: MappingConfig = None,
        sync_logger: SyncLogger = None,
    ):
        self.onekey = onekey_client
        self.dataverse = dataverse_client
        self.config = config or load_mapping_config()
        self.transformer = TransformEngine(self.config)
        self.mapping_validator = MappingValidator(self.config)
        self.dataverse_validator = DataverseValidator(dataverse_client)
        self.sync_logger = sync_logger or SyncLogger()
        
        # Cache for account lookups (workplaceEid -> accountid)
        self._account_cache: dict[str, str] = {}
    
    def validate_before_sync(self) -> ValidationResult:
        """Run all validation checks before starting sync.
        
        Returns:
            Combined ValidationResult from all checks
        """
        self.sync_logger.log_phase("PRE-SYNC VALIDATION")
        
        result = ValidationResult()
        
        # Validate mapping configuration
        mapping_result = self.mapping_validator.validate_mapping_config()
        result.merge(mapping_result)
        self.sync_logger.log_validation_result(mapping_result, "Mapping Config")
        
        # Validate Dataverse schema
        schema_result = self.dataverse_validator.validate_schema(self.config)
        result.merge(schema_result)
        self.sync_logger.log_validation_result(schema_result, "Dataverse Schema")
        
        logger.info("Pre-sync validation: %s", result.summary())
        return result
    
    def sync_full(
        self,
        codebases: list[str],
        include_address: bool = False,
        max_records: int = None,
        dry_run: bool = False,
    ) -> dict:
        """Run full data synchronization.
        
        Args:
            codebases: List of OneKey codebases to sync (e.g., ["WUK", "WDE"])
            include_address: Whether to sync address fields
            max_records: Limit records per entity type (for testing)
            dry_run: If True, validate and log but don't write
            
        Returns:
            Summary dict with counts and any errors
        """
        self.sync_logger.start_sync("full", codebases)
        
        # Pre-sync validation
        validation = self.validate_before_sync()
        if not validation.is_valid:
            logger.error("Pre-sync validation failed. Aborting sync.")
            self.sync_logger.end_sync(success=False)
            return {"success": False, "validation": validation.summary()}
        
        results = {
            "success": True,
            "codebases": codebases,
            "accounts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
            "contacts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
            "errors": [],
        }
        
        for codebase in codebases:
            self.sync_logger.log_phase(f"SYNC CODEBASE: {codebase}")
            
            try:
                # Phase 1: Load HCOs
                hco_result = self._sync_hcos(
                    codebase, include_address, max_records, dry_run
                )
                results["accounts"]["processed"] += hco_result["processed"]
                results["accounts"]["created"] += hco_result["created"]
                results["accounts"]["updated"] += hco_result["updated"]
                results["accounts"]["failed"] += hco_result["failed"]
                
                # Phase 2: Load HCPs
                hcp_result = self._sync_hcps(
                    codebase, include_address, max_records, dry_run
                )
                results["contacts"]["processed"] += hcp_result["processed"]
                results["contacts"]["created"] += hcp_result["created"]
                results["contacts"]["updated"] += hcp_result["updated"]
                results["contacts"]["failed"] += hcp_result["failed"]
                
            except Exception as e:
                logger.exception("Error syncing codebase %s", codebase)
                results["errors"].append(f"{codebase}: {e}")
                results["success"] = False
        
        self.sync_logger.end_sync(success=results["success"])
        return results
    
    def sync_delta(
        self,
        codebases: list[str],
        date_from: str,
        date_to: str,
        include_address: bool = False,
        dry_run: bool = False,
    ) -> dict:
        """Run delta/CDC synchronization for changed records.
        
        Args:
            codebases: List of OneKey codebases
            date_from: ISO date string for start of change window
            date_to: ISO date string for end of change window
            include_address: Whether to sync address fields
            dry_run: If True, validate and log but don't write
            
        Returns:
            Summary dict with counts and any errors
        """
        self.sync_logger.start_sync("delta", codebases, date_from, date_to)
        
        # Pre-sync validation
        validation = self.validate_before_sync()
        if not validation.is_valid:
            logger.error("Pre-sync validation failed. Aborting sync.")
            self.sync_logger.end_sync(success=False)
            return {"success": False, "validation": validation.summary()}
        
        results = {
            "success": True,
            "codebases": codebases,
            "date_from": date_from,
            "date_to": date_to,
            "accounts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
            "contacts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
            "by_process_code": {"I": 0, "U": 0, "R": 0},
            "errors": [],
        }
        
        for codebase in codebases:
            self.sync_logger.log_phase(f"DELTA SYNC: {codebase} ({date_from} to {date_to})")
            
            try:
                delta_result = self._sync_delta_codebase(
                    codebase, date_from, date_to, include_address, dry_run
                )
                
                # Merge results
                for key in ["processed", "created", "updated", "failed"]:
                    results["accounts"][key] += delta_result["accounts"][key]
                    results["contacts"][key] += delta_result["contacts"][key]
                
                for code in ["I", "U", "R"]:
                    results["by_process_code"][code] += delta_result.get("by_process_code", {}).get(code, 0)
                    
            except Exception as e:
                logger.exception("Error in delta sync for %s", codebase)
                results["errors"].append(f"{codebase}: {e}")
                results["success"] = False
        
        self.sync_logger.end_sync(success=results["success"])
        return results
    
    def _sync_hcos(
        self,
        codebase: str,
        include_address: bool,
        max_records: int,
        dry_run: bool,
    ) -> dict:
        """Sync HCOs for a single codebase."""
        self.sync_logger.log_phase(f"HCO SYNC: {codebase}")
        
        result = {"processed": 0, "created": 0, "updated": 0, "failed": 0}
        
        # Collect all HCOs
        hcos = []
        logger.info("Extracting HCOs from OneKey %s...", codebase)
        
        for record in self.onekey.iter_population(cod_base=codebase, max_records=max_records):
            if (record.get("entityType") or "").upper() == "WORKPLACE":
                hcos.append(record)
        
        logger.info("Extracted %d HCOs from %s", len(hcos), codebase)
        
        if not hcos:
            return result
        
        # Flatten and compute hierarchy
        flattened = [flatten_hco(r) for r in hcos]
        depths = self._compute_hierarchy_depths(flattened)
        
        # Sort by depth (parents first)
        sorted_hcos = sorted(flattened, key=lambda h: depths.get(h.get("workplaceEid"), 0))
        
        logger.info("HCO hierarchy: max depth %d", max(depths.values()) if depths else 0)
        
        # Process each HCO
        total = len(sorted_hcos)
        for i, hco in enumerate(sorted_hcos, 1):
            workplace_eid = hco.get("workplaceEid")
            
            if i % 50 == 0 or i == total:
                self.sync_logger.log_progress("accounts", i, total)
            
            # Validate record
            record_validation = self.mapping_validator.validate_source_record(hco, "account")
            if not record_validation.is_valid:
                self.sync_logger.log_validation_result(record_validation, f"HCO {workplace_eid}")
                result["failed"] += 1
                continue
            
            # Resolve parent account
            parent_account_id = None
            parent_eid = hco.get("parentWorkplaceEid")
            if parent_eid:
                parent_account_id = self._account_cache.get(parent_eid)
                if not parent_account_id and not dry_run:
                    # Try to find in Dataverse
                    parent_account_id = self._lookup_account(parent_eid)
                
                if parent_account_id:
                    self.sync_logger.log_relationship_resolved(
                        "account", workplace_eid, "account", parent_account_id
                    )
                else:
                    self.sync_logger.log_relationship_failed(
                        "account", workplace_eid, "account", parent_eid, "Parent not found"
                    )
            
            # Transform to Dataverse payload
            payload = self.transformer.transform_hco(
                hco, include_address, parent_account_id
            )
            
            if dry_run:
                logger.debug("DRY RUN: Would upsert account %s: %s", workplace_eid, payload)
                result["processed"] += 1
                continue
            
            # Upsert to Dataverse
            with self.sync_logger.log_operation("upsert", "account", workplace_eid) as log:
                try:
                    key_field = self.config.get_key_field("account")
                    key_value = payload.get(key_field.target_field if key_field else "accountnumber")
                    
                    account_id, created = self.dataverse.upsert_by_field(
                        "accounts",
                        key_field.target_field if key_field else "accountnumber",
                        key_value,
                        payload,
                    )
                    
                    log.target_id = account_id
                    log.operation = "create" if created else "update"
                    
                    self._account_cache[workplace_eid] = account_id
                    
                    if created:
                        result["created"] += 1
                    else:
                        result["updated"] += 1
                    result["processed"] += 1
                    
                    self.sync_logger.log_record_success(
                        "account", workplace_eid, account_id,
                        "create" if created else "update",
                        hco.get("officialName")
                    )
                    
                except Exception as e:
                    result["failed"] += 1
                    self.sync_logger.log_record_error(
                        "account", workplace_eid, "upsert", str(e)
                    )
                    raise
        
        return result
    
    def _sync_hcps(
        self,
        codebase: str,
        include_address: bool,
        max_records: int,
        dry_run: bool,
    ) -> dict:
        """Sync HCPs for a single codebase."""
        self.sync_logger.log_phase(f"HCP SYNC: {codebase}")
        
        result = {"processed": 0, "created": 0, "updated": 0, "failed": 0}
        
        # Collect HCPs (Activities)
        hcps = []
        logger.info("Extracting HCPs from OneKey %s...", codebase)
        
        for record in self.onekey.iter_population(cod_base=codebase, max_records=max_records):
            if (record.get("entityType") or "").upper() == "ACTIVITY":
                hcps.append(record)
        
        logger.info("Extracted %d HCPs from %s", len(hcps), codebase)
        
        if not hcps:
            return result
        
        # Flatten records
        flattened = [flatten_hcp(r) for r in hcps]
        
        # Process each HCP
        total = len(flattened)
        for i, hcp in enumerate(flattened, 1):
            individual_eid = hcp.get("individualEid")
            workplace_eid = hcp.get("workplaceEid")
            
            if i % 50 == 0 or i == total:
                self.sync_logger.log_progress("contacts", i, total)
            
            # Validate record
            record_validation = self.mapping_validator.validate_source_record(hcp, "contact")
            if not record_validation.is_valid:
                self.sync_logger.log_validation_result(record_validation, f"HCP {individual_eid}")
                result["failed"] += 1
                continue
            
            # Resolve account relationship
            account_id = None
            if workplace_eid:
                account_id = self._account_cache.get(workplace_eid)
                if not account_id and not dry_run:
                    account_id = self._lookup_account(workplace_eid)
                
                if account_id:
                    self.sync_logger.log_relationship_resolved(
                        "contact", individual_eid, "account", account_id
                    )
                else:
                    self.sync_logger.log_orphan_contact(individual_eid, workplace_eid)
            
            # Transform to Dataverse payload
            payload = self.transformer.transform_hcp(hcp, include_address, account_id)
            
            if dry_run:
                logger.debug("DRY RUN: Would upsert contact %s: %s", individual_eid, payload)
                result["processed"] += 1
                continue
            
            # Upsert to Dataverse
            with self.sync_logger.log_operation("upsert", "contact", individual_eid) as log:
                try:
                    key_field = self.config.get_key_field("contact")
                    key_value = payload.get(key_field.target_field if key_field else "externaluseridentifier")
                    
                    contact_id, created = self.dataverse.upsert_by_field(
                        "contacts",
                        key_field.target_field if key_field else "externaluseridentifier",
                        key_value,
                        payload,
                    )
                    
                    log.target_id = contact_id
                    log.operation = "create" if created else "update"
                    
                    if created:
                        result["created"] += 1
                    else:
                        result["updated"] += 1
                    result["processed"] += 1
                    
                    name = f"{hcp.get('firstName') or ''} {hcp.get('lastName') or ''}".strip()
                    self.sync_logger.log_record_success(
                        "contact", individual_eid, contact_id,
                        "create" if created else "update",
                        name
                    )
                    
                except Exception as e:
                    result["failed"] += 1
                    self.sync_logger.log_record_error(
                        "contact", individual_eid, "upsert", str(e)
                    )
                    raise
        
        return result
    
    def _sync_delta_codebase(
        self,
        codebase: str,
        date_from: str,
        date_to: str,
        include_address: bool,
        dry_run: bool,
    ) -> dict:
        """Sync delta changes for a single codebase."""
        result = {
            "accounts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
            "contacts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
            "by_process_code": {"I": 0, "U": 0, "R": 0},
        }
        
        # Collect changes
        workplaces = []
        activities = []
        
        for record in self.onekey.iter_population(
            cod_base=codebase,
            date_from=date_from,
            date_to=date_to,
        ):
            process_code = record.get("processCode", "U")
            result["by_process_code"][process_code] = result["by_process_code"].get(process_code, 0) + 1
            
            entity_type = (record.get("entityType") or "").upper()
            if entity_type == "WORKPLACE":
                workplaces.append(record)
            elif entity_type == "ACTIVITY":
                activities.append(record)
        
        logger.info(
            "Delta for %s: %d workplaces, %d activities (I=%d, U=%d, R=%d)",
            codebase, len(workplaces), len(activities),
            result["by_process_code"]["I"],
            result["by_process_code"]["U"],
            result["by_process_code"]["R"],
        )
        
        # Process workplaces first (for hierarchy)
        for record in workplaces:
            hco = flatten_hco(record)
            workplace_eid = hco.get("workplaceEid")
            
            # Resolve parent
            parent_account_id = None
            parent_eid = hco.get("parentWorkplaceEid")
            if parent_eid:
                parent_account_id = self._account_cache.get(parent_eid) or self._lookup_account(parent_eid)
            
            payload = self.transformer.transform_hco(hco, include_address, parent_account_id)
            
            if not dry_run:
                try:
                    key_field = self.config.get_key_field("account")
                    key_value = payload.get(key_field.target_field if key_field else "accountnumber")
                    
                    account_id, created = self.dataverse.upsert_by_field(
                        "accounts", key_field.target_field if key_field else "accountnumber",
                        key_value, payload
                    )
                    self._account_cache[workplace_eid] = account_id
                    
                    if created:
                        result["accounts"]["created"] += 1
                    else:
                        result["accounts"]["updated"] += 1
                except Exception as e:
                    logger.error("Delta sync failed for HCO %s: %s", workplace_eid, e)
                    result["accounts"]["failed"] += 1
            
            result["accounts"]["processed"] += 1
        
        # Process activities
        for record in activities:
            hcp = flatten_hcp(record)
            individual_eid = hcp.get("individualEid")
            workplace_eid = hcp.get("workplaceEid")
            
            account_id = self._account_cache.get(workplace_eid) or self._lookup_account(workplace_eid)
            payload = self.transformer.transform_hcp(hcp, include_address, account_id)
            
            if not dry_run:
                try:
                    key_field = self.config.get_key_field("contact")
                    key_value = payload.get(key_field.target_field if key_field else "externaluseridentifier")
                    
                    contact_id, created = self.dataverse.upsert_by_field(
                        "contacts", key_field.target_field if key_field else "externaluseridentifier",
                        key_value, payload
                    )
                    
                    if created:
                        result["contacts"]["created"] += 1
                    else:
                        result["contacts"]["updated"] += 1
                except Exception as e:
                    logger.error("Delta sync failed for HCP %s: %s", individual_eid, e)
                    result["contacts"]["failed"] += 1
            
            result["contacts"]["processed"] += 1
        
        return result
    
    def _compute_hierarchy_depths(self, hcos: list[dict]) -> dict[str, int]:
        """Compute depth of each HCO in the hierarchy.
        
        Root HCOs (no parent) have depth 0.
        Children have depth = parent_depth + 1.
        """
        parents = {}
        for hco in hcos:
            eid = hco.get("workplaceEid")
            parent_eid = hco.get("parentWorkplaceEid")
            parents[eid] = parent_eid
        
        eids = set(parents.keys())
        depths = {}
        
        def get_depth(eid: str, visited: set) -> int:
            if eid in depths:
                return depths[eid]
            if eid in visited:
                return 0  # Cycle detected
            
            parent = parents.get(eid)
            if not parent or parent not in eids:
                depths[eid] = 0
                return 0
            
            depths[eid] = 1 + get_depth(parent, visited | {eid})
            return depths[eid]
        
        for eid in eids:
            get_depth(eid, set())
        
        return depths
    
    def _lookup_account(self, workplace_eid: str) -> str | None:
        """Lookup account ID from Dataverse by workplace EID."""
        key_field = self.config.get_key_field("account")
        field_name = key_field.target_field if key_field else "accountnumber"
        
        try:
            record = self.dataverse.find_one(
                "accounts",
                filter=f"{field_name} eq '{workplace_eid}'",
                select=[field_name, "accountid"],
            )
            if record:
                account_id = record.get("accountid")
                self._account_cache[workplace_eid] = account_id
                return account_id
        except Exception as e:
            logger.warning("Failed to lookup account %s: %s", workplace_eid, e)
        
        return None


def create_sync_engine(
    onekey_client: "OneKeyClient",
    dataverse_client: "DataverseClient",
    config_dir: str = None,
) -> DynamicSyncEngine:
    """Create a sync engine with configuration loaded from CSV files.
    
    Args:
        onekey_client: Connected OneKey client
        dataverse_client: Connected Dataverse client
        config_dir: Optional path to config directory
        
    Returns:
        Configured DynamicSyncEngine
    """
    from pathlib import Path
    
    config = load_mapping_config(Path(config_dir) if config_dir else None)
    return DynamicSyncEngine(onekey_client, dataverse_client, config)
