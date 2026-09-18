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
from .hierarchy import apply_updates, build_updates, edges_from_hcos, resolve_accounts
from .picklist_sync import sync_picklists_from_config
from onekey.parse import compose_hcp_result, flatten_hcp, flatten_hco
from dataverse.client import chunked

if TYPE_CHECKING:
    from onekey import OneKeyClient
    from dataverse import DataverseClient

logger = logging.getLogger(__name__)

_EMPTY_ENTITY = {"processed": 0, "created": 0, "updated": 0, "failed": 0}
_EMPTY_HIERARCHY = {
    "edges": 0,
    "linked": 0,
    "failed": 0,
    "already_linked": 0,
    "missing_child": 0,
    "missing_parent": 0,
}


def _empty_entity_stats() -> dict:
    return dict(_EMPTY_ENTITY)


def _empty_hierarchy_stats() -> dict:
    return dict(_EMPTY_HIERARCHY)


class DynamicSyncEngine:
    """Orchestrates OneKey to Dataverse synchronization.

    Phase order (full and delta):
      0 picklists → 1 HCO accounts → 2 customeraddresses →
      3 parentaccountid hierarchy → 4 HCP contacts
    """

    def __init__(
        self,
        onekey_client: "OneKeyClient",
        dataverse_client: "DataverseClient",
        config: MappingConfig = None,
        sync_logger: SyncLogger = None,
        bulk_batch_size: int = None,
    ):
        self.onekey = onekey_client
        self.dataverse = dataverse_client
        self.config = config or load_mapping_config()
        self.transformer = TransformEngine(self.config)
        self.mapping_validator = MappingValidator(self.config)
        self.dataverse_validator = DataverseValidator(dataverse_client)
        self.sync_logger = sync_logger or SyncLogger()
        self.bulk_batch_size = bulk_batch_size or getattr(
            getattr(dataverse_client, "config", None), "bulk_batch_size", None
        ) or 100

        # Cache for account lookups (workplaceEid -> accountid)
        self._account_cache: dict[str, str] = {}

    def validate_before_sync(self) -> ValidationResult:
        """Run all validation checks before starting sync."""
        self.sync_logger.log_phase("PRE-SYNC VALIDATION")

        result = ValidationResult()

        mapping_result = self.mapping_validator.validate_mapping_config()
        result.merge(mapping_result)
        self.sync_logger.log_validation_result(mapping_result, "Mapping Config")

        schema_result = self.dataverse_validator.validate_schema(self.config)
        result.merge(schema_result)
        self.sync_logger.log_validation_result(schema_result, "Dataverse Schema")

        logger.info("Pre-sync validation: %s", result.summary())
        return result

    def sync_picklists(self, dry_run: bool = False) -> list[dict]:
        """Phase 0: upsert choice options from value_mapping.csv into Dataverse."""
        self.sync_logger.log_phase("PICKLIST SYNC")
        if not hasattr(self.dataverse, "request"):
            logger.info(
                "Skipping picklist sync (Dataverse client has no metadata API)"
            )
            return []
        return sync_picklists_from_config(
            self.dataverse, self.config, dry_run=dry_run
        )

    def sync_full(
        self,
        codebases: list[str],
        max_records: int = None,
        dry_run: bool = False,
        hco_only: bool = False,
        hcp_only: bool = False,
        sync_addresses: bool = True,
        sync_hierarchy: bool = True,
        sync_picklists: bool = True,
    ) -> dict:
        """Run full migration: picklists → HCO → address → hierarchy → HCP.

        Accounts are upserted without parent binds; hierarchy is Phase 3.
        Addresses use ``customeraddress`` only (never account/contact address1_*).
        """
        if hco_only and hcp_only:
            raise ValueError("hco_only and hcp_only cannot both be True")

        self.transformer.picklist_gaps.clear()
        self.sync_logger.start_sync("full", codebases)

        validation = self.validate_before_sync()
        if not validation.is_valid:
            logger.error("Pre-sync validation failed. Aborting sync.")
            self.sync_logger.end_sync(success=False)
            return {"success": False, "validation": validation.summary()}

        results = {
            "success": True,
            "codebases": codebases,
            "picklists": [],
            "accounts": _empty_entity_stats(),
            "addresses": _empty_entity_stats(),
            "hierarchy": _empty_hierarchy_stats(),
            "contacts": _empty_entity_stats(),
            "picklist_gaps": 0,
            "picklist_gap_report": None,
            "errors": [],
        }

        if sync_picklists and not hcp_only:
            try:
                results["picklists"] = self.sync_picklists(dry_run=dry_run)
            except Exception as e:
                # Missing option sets on a fresh env must not block HCO/HCP load.
                logger.exception("Picklist sync failed — continuing without option upsert")
                results["errors"].append(f"picklists: {e}")
                results["picklists"] = []

        for codebase in codebases:
            self.sync_logger.log_phase(f"SYNC CODEBASE: {codebase}")
            try:
                flattened_hcos: list[dict] = []

                if not hcp_only:
                    hco_result, flattened_hcos = self._sync_hcos(
                        codebase,
                        max_records=max_records,
                        dry_run=dry_run,
                    )
                    self._merge_entity(results["accounts"], hco_result)

                    if sync_addresses:
                        addr_result = self._sync_addresses(
                            flattened_hcos, dry_run=dry_run
                        )
                        self._merge_entity(results["addresses"], addr_result)

                    if sync_hierarchy:
                        hier_result = self._sync_hierarchy(
                            flattened_hcos, dry_run=dry_run
                        )
                        self._merge_hierarchy(results["hierarchy"], hier_result)

                if not hco_only:
                    hcp_result = self._sync_hcps(
                        codebase, max_records=max_records, dry_run=dry_run
                    )
                    self._merge_entity(results["contacts"], hcp_result)

            except Exception as e:
                logger.exception("Error syncing codebase %s", codebase)
                results["errors"].append(f"{codebase}: {e}")
                results["success"] = False

        gap_path = self.transformer.picklist_gaps.write_report()
        results["picklist_gaps"] = len(self.transformer.picklist_gaps.gaps)
        results["picklist_gap_report"] = str(gap_path) if gap_path else None

        self.sync_logger.end_sync(success=results["success"])
        return results

    def sync_delta(
        self,
        codebases: list[str],
        date_from: str,
        date_to: str,
        dry_run: bool = False,
        sync_addresses: bool = True,
        sync_hierarchy: bool = True,
        sync_picklists: bool = True,
    ) -> dict:
        """Run CDC delta with the same phase order as full sync."""
        self.transformer.picklist_gaps.clear()
        self.sync_logger.start_sync("delta", codebases, date_from, date_to)

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
            "picklists": [],
            "accounts": _empty_entity_stats(),
            "addresses": _empty_entity_stats(),
            "hierarchy": _empty_hierarchy_stats(),
            "contacts": _empty_entity_stats(),
            "by_process_code": {"I": 0, "U": 0, "R": 0},
            "picklist_gaps": 0,
            "picklist_gap_report": None,
            "errors": [],
        }

        if sync_picklists:
            try:
                results["picklists"] = self.sync_picklists(dry_run=dry_run)
            except Exception as e:
                logger.exception("Picklist sync failed — continuing without option upsert")
                results["errors"].append(f"picklists: {e}")
                results["picklists"] = []

        for codebase in codebases:
            self.sync_logger.log_phase(
                f"DELTA SYNC: {codebase} ({date_from} to {date_to})"
            )
            try:
                delta_result = self._sync_delta_codebase(
                    codebase,
                    date_from,
                    date_to,
                    dry_run=dry_run,
                    sync_addresses=sync_addresses,
                    sync_hierarchy=sync_hierarchy,
                )
                self._merge_entity(results["accounts"], delta_result["accounts"])
                self._merge_entity(results["addresses"], delta_result["addresses"])
                self._merge_hierarchy(results["hierarchy"], delta_result["hierarchy"])
                self._merge_entity(results["contacts"], delta_result["contacts"])
                for code in ["I", "U", "R"]:
                    results["by_process_code"][code] += delta_result.get(
                        "by_process_code", {}
                    ).get(code, 0)
            except Exception as e:
                logger.exception("Error in delta sync for %s", codebase)
                results["errors"].append(f"{codebase}: {e}")
                results["success"] = False

        gap_path = self.transformer.picklist_gaps.write_report()
        results["picklist_gaps"] = len(self.transformer.picklist_gaps.gaps)
        results["picklist_gap_report"] = str(gap_path) if gap_path else None

        self.sync_logger.end_sync(success=results["success"])
        return results

    @staticmethod
    def _merge_entity(target: dict, source: dict) -> None:
        for key in ("processed", "created", "updated", "failed"):
            target[key] += source.get(key, 0)

    @staticmethod
    def _merge_hierarchy(target: dict, source: dict) -> None:
        for key in _EMPTY_HIERARCHY:
            target[key] += source.get(key, 0)
    
    def _sync_hcos(
        self,
        codebase: str,
        max_records: int = None,
        dry_run: bool = False,
        flattened: list[dict] | None = None,
    ) -> tuple[dict, list[dict]]:
        """Sync HCOs for a single codebase using bulk upserts.

        Parent ``parentaccountid`` is always deferred to ``_sync_hierarchy``.
        """
        self.sync_logger.log_phase(f"HCO SYNC: {codebase}")

        result = _empty_entity_stats()

        if flattened is None:
            hcos = []
            logger.info("Extracting HCOs from OneKey %s...", codebase)
            for record in self.onekey.iter_population(
                cod_base=codebase, max_records=max_records
            ):
                if (record.get("entityType") or "").upper() == "WORKPLACE":
                    hcos.append(record)
            logger.info("Extracted %d HCOs from %s", len(hcos), codebase)
            flattened = [flatten_hco(r) for r in hcos]

        if not flattened:
            return result, flattened

        logger.info(
            "Upserting %d accounts (batch size %d); parent links deferred",
            len(flattened),
            self.bulk_batch_size,
        )

        total = len(flattened)
        prepared = []
        done = 0

        for hco in flattened:
            workplace_eid = hco.get("workplaceEid")
            record_validation = self.mapping_validator.validate_source_record(
                hco, "account"
            )
            if not record_validation.is_valid:
                self.sync_logger.log_validation_result(
                    record_validation, f"HCO {workplace_eid}"
                )
                result["failed"] += 1
                done += 1
                continue

            payload = self.transformer.transform_hco(hco)
            key_field = self.config.get_key_field("account")
            key_name = key_field.target_field if key_field else "cr4ec_externalid"
            key_value = payload.get(key_name)
            if not key_value:
                result["failed"] += 1
                done += 1
                self.sync_logger.log_record_error(
                    "account",
                    workplace_eid,
                    "upsert",
                    f"Missing key field {key_name}",
                )
                continue

            prepared.append((workplace_eid, key_name, key_value, payload, hco))

        if dry_run:
            for workplace_eid, _, _, payload, _ in prepared:
                logger.debug(
                    "DRY RUN: Would upsert account %s: %s", workplace_eid, payload
                )
                result["processed"] += 1
            self.sync_logger.log_progress("accounts", len(prepared), total)
            return result, flattened

        if not prepared:
            return result, flattened

        key_name = prepared[0][1]
        payloads = [item[3] for item in prepared]

        try:
            bulk_results = self.dataverse.bulk_upsert_by_field(
                "accounts",
                key_name,
                payloads,
                batch_size=self.bulk_batch_size,
            )
        except Exception as e:
            result["failed"] += len(prepared)
            for workplace_eid, *_ in prepared:
                self.sync_logger.log_record_error(
                    "account", workplace_eid, "bulk_upsert", str(e)
                )
            raise

        for (workplace_eid, _, _, _, hco), (_, account_id, created) in zip(
            prepared, bulk_results
        ):
            self._account_cache[workplace_eid] = account_id
            if created:
                result["created"] += 1
            else:
                result["updated"] += 1
            result["processed"] += 1
            done += 1
            self.sync_logger.log_record_success(
                "account",
                workplace_eid,
                account_id,
                "create" if created else "update",
                hco.get("officialName"),
            )
            if done % 100 == 0 or done == len(prepared):
                self.sync_logger.log_progress("accounts", done, len(prepared))

        return result, flattened

    def _sync_addresses(
        self,
        hcos: list[dict],
        dry_run: bool = False,
    ) -> dict:
        """Phase 2: upsert customeraddress rows for flattened HCOs."""
        self.sync_logger.log_phase("ADDRESS SYNC (customeraddress)")
        result = _empty_entity_stats()
        if not hcos:
            return result

        # Ensure account GUIDs are cached for parentid binds.
        missing = {
            h.get("workplaceEid")
            for h in hcos
            if h.get("workplaceEid") and h.get("workplaceEid") not in self._account_cache
        }
        if missing and not dry_run:
            self._lookup_accounts_bulk(missing)

        prepared = []
        for hco in hcos:
            workplace_eid = hco.get("workplaceEid")
            address_eid = hco.get("address_addressEid") or hco.get("addressEid")
            if not workplace_eid or not address_eid:
                continue
            account_id = self._account_cache.get(workplace_eid)
            if not account_id and not dry_run:
                result["failed"] += 1
                self.sync_logger.log_record_error(
                    "customeraddress",
                    workplace_eid,
                    "upsert",
                    "Account not found for address parentid",
                )
                continue
            payload = self.transformer.transform_address(
                hco, parent_account_id=account_id
            )
            if not payload:
                continue
            key = payload.get("cr4ec_externalid")
            if not key:
                result["failed"] += 1
                continue
            prepared.append((key, payload, workplace_eid))

        logger.info("Prepared %d customeraddress payloads", len(prepared))
        if dry_run:
            result["processed"] = len(prepared)
            return result
        if not prepared:
            return result

        try:
            bulk_results = self.dataverse.bulk_upsert_by_field(
                "customeraddresses",
                "cr4ec_externalid",
                [p for _, p, _ in prepared],
                batch_size=self.bulk_batch_size,
            )
        except Exception as e:
            result["failed"] += len(prepared)
            for key, _, workplace_eid in prepared:
                self.sync_logger.log_record_error(
                    "customeraddress", workplace_eid, "bulk_upsert", str(e)
                )
            raise

        for (key, _, workplace_eid), (_, record_id, created) in zip(
            prepared, bulk_results
        ):
            if created:
                result["created"] += 1
            else:
                result["updated"] += 1
            result["processed"] += 1
            self.sync_logger.log_record_success(
                "customeraddress",
                key,
                record_id,
                "create" if created else "update",
                workplace_eid,
            )

        return result

    def _sync_hierarchy(
        self,
        hcos: list[dict],
        dry_run: bool = False,
        skip_unchanged: bool = True,
    ) -> dict:
        """Phase 3: set parentaccountid from managerWorkplaceEid edges."""
        self.sync_logger.log_phase("HCO HIERARCHY LINK")
        result = _empty_hierarchy_stats()
        edges = edges_from_hcos(hcos)
        result["edges"] = len(edges)
        if not edges:
            logger.info("No parent edges to link")
            return result

        eids = sorted({eid for pair in edges for eid in pair})
        if dry_run:
            # Prefer cache; count how many edges look resolvable.
            ready = 0
            for child, parent in edges:
                if child in self._account_cache and parent in self._account_cache:
                    ready += 1
            result["linked"] = ready
            logger.info(
                "DRY RUN: would link up to %d/%d hierarchy edges", ready, len(edges)
            )
            return result

        accounts = resolve_accounts(self.dataverse, eids)
        for eid, row in accounts.items():
            if row.get("accountid"):
                self._account_cache[eid] = row["accountid"]

        updates, stats = build_updates(
            edges, accounts, skip_unchanged=skip_unchanged
        )
        result["already_linked"] = stats.get("already_linked", 0)
        result["missing_child"] = stats.get("missing_child", 0)
        result["missing_parent"] = stats.get("missing_parent", 0)

        logger.info(
            "Hierarchy plan: ready=%d already_linked=%d missing_child=%d "
            "missing_parent=%d",
            stats["ready"],
            stats["already_linked"],
            stats["missing_child"],
            stats["missing_parent"],
        )

        if not updates:
            return result

        linked, failed = apply_updates(
            self.dataverse, updates, self.bulk_batch_size
        )
        result["linked"] = linked
        result["failed"] = failed
        return result

    def _collect_population_hcps(
        self,
        codebase: str,
        max_records: int = None,
        date_from: str = None,
        date_to: str = None,
    ) -> list[dict]:
        """Collect HCP rows from population by joining INDIVIDUAL + ACTIVITY.

        Population returns entity types as separate packets (WORKPLACE,
        INDIVIDUAL, ACTIVITY). ACTIVITY rows only carry link EIDs; person and
        workplace detail must be joined from the other entity types.

        One contact per individual: when multiple activities exist, prefer
        ``isMainActivity``.
        """
        individuals: dict[str, dict] = {}
        workplaces: dict[str, dict] = {}
        activities_by_individual: dict[str, dict] = {}

        logger.info(
            "Extracting HCPs from OneKey %s (joining INDIVIDUAL + ACTIVITY)...",
            codebase,
        )

        for record in self.onekey.iter_population(
            cod_base=codebase,
            max_records=max_records,
            date_from=date_from,
            date_to=date_to,
        ):
            entity_type = (record.get("entityType") or "").upper()
            if entity_type == "WORKPLACE":
                workplace = record.get("workplace") or {}
                workplace_eid = workplace.get("workplaceEid")
                if workplace_eid:
                    workplaces[workplace_eid] = record
            elif entity_type == "INDIVIDUAL":
                individual = record.get("individual") or {}
                individual_eid = individual.get("individualEid")
                if individual_eid:
                    individuals[individual_eid] = record
            elif entity_type == "ACTIVITY":
                activity = record.get("activity") or {}
                individual_eid = activity.get("individualEid")
                if not individual_eid:
                    continue
                existing = activities_by_individual.get(individual_eid)
                if existing is None:
                    activities_by_individual[individual_eid] = record
                else:
                    existing_activity = existing.get("activity") or {}
                    if activity.get("isMainActivity") and not existing_activity.get(
                        "isMainActivity"
                    ):
                        activities_by_individual[individual_eid] = record

        composed = []
        for individual_eid, activity_record in activities_by_individual.items():
            activity = activity_record.get("activity") or {}
            composed.append(
                compose_hcp_result(
                    individuals.get(individual_eid),
                    activity_record,
                    workplaces.get(activity.get("workplaceEid")),
                )
            )

        logger.info(
            "Joined %d HCPs from %s (%d individuals, %d activities, %d workplaces indexed)",
            len(composed),
            codebase,
            len(individuals),
            len(activities_by_individual),
            len(workplaces),
        )
        return composed

    def _sync_hcps(
        self,
        codebase: str,
        max_records: int = None,
        dry_run: bool = False,
        date_from: str = None,
        date_to: str = None,
        composed: list[dict] | None = None,
    ) -> dict:
        """Sync HCPs for a single codebase using bulk upserts."""
        self.sync_logger.log_phase(f"HCP SYNC: {codebase}")

        result = _empty_entity_stats()

        if composed is None:
            composed = self._collect_population_hcps(
                codebase,
                max_records=max_records,
                date_from=date_from,
                date_to=date_to,
            )

        if not composed:
            return result

        flattened = [flatten_hcp(r) for r in composed]

        workplace_eids = {
            hcp.get("workplaceEid")
            for hcp in flattened
            if hcp.get("workplaceEid") and hcp.get("workplaceEid") not in self._account_cache
        }
        if workplace_eids and not dry_run:
            self._lookup_accounts_bulk(workplace_eids)

        total = len(flattened)
        prepared = []

        for i, hcp in enumerate(flattened, 1):
            individual_eid = hcp.get("individualEid")
            workplace_eid = hcp.get("workplaceEid")

            record_validation = self.mapping_validator.validate_source_record(
                hcp, "contact"
            )
            if not record_validation.is_valid:
                self.sync_logger.log_validation_result(
                    record_validation, f"HCP {individual_eid}"
                )
                result["failed"] += 1
                continue

            account_id = None
            if workplace_eid:
                account_id = self._account_cache.get(workplace_eid)
                if account_id:
                    self.sync_logger.log_relationship_resolved(
                        "contact", individual_eid, "account", account_id
                    )
                else:
                    self.sync_logger.log_orphan_contact(individual_eid, workplace_eid)

            payload = self.transformer.transform_hcp(hcp, account_id=account_id)
            key_field = self.config.get_key_field("contact")
            key_name = key_field.target_field if key_field else "externaluseridentifier"
            key_value = payload.get(key_name)
            if not key_value:
                result["failed"] += 1
                self.sync_logger.log_record_error(
                    "contact",
                    individual_eid,
                    "upsert",
                    f"Missing key field {key_name}",
                )
                continue

            if dry_run:
                logger.debug(
                    "DRY RUN: Would upsert contact %s: %s", individual_eid, payload
                )
                result["processed"] += 1
                if i % 50 == 0 or i == total:
                    self.sync_logger.log_progress("contacts", i, total)
                continue

            prepared.append((individual_eid, key_name, payload, hcp))

        if dry_run or not prepared:
            return result

        key_name = prepared[0][1]
        done = 0
        for batch in chunked(prepared, self.bulk_batch_size):
            payloads = [item[2] for item in batch]
            try:
                bulk_results = self.dataverse.bulk_upsert_by_field(
                    "contacts",
                    key_name,
                    payloads,
                    batch_size=self.bulk_batch_size,
                )
            except Exception as e:
                result["failed"] += len(batch)
                for individual_eid, *_ in batch:
                    self.sync_logger.log_record_error(
                        "contact", individual_eid, "bulk_upsert", str(e)
                    )
                raise

            for (individual_eid, _, _, hcp), (_, contact_id, created) in zip(
                batch, bulk_results
            ):
                if created:
                    result["created"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                done += 1
                name = f"{hcp.get('firstName') or ''} {hcp.get('lastName') or ''}".strip()
                self.sync_logger.log_record_success(
                    "contact",
                    individual_eid,
                    contact_id,
                    "create" if created else "update",
                    name,
                )

            self.sync_logger.log_progress("contacts", done, len(prepared))

        return result

    def _sync_delta_codebase(
        self,
        codebase: str,
        date_from: str,
        date_to: str,
        dry_run: bool = False,
        sync_addresses: bool = True,
        sync_hierarchy: bool = True,
    ) -> dict:
        """Sync delta changes for a single codebase using bulk upserts."""
        result = {
            "accounts": _empty_entity_stats(),
            "addresses": _empty_entity_stats(),
            "hierarchy": _empty_hierarchy_stats(),
            "contacts": _empty_entity_stats(),
            "by_process_code": {"I": 0, "U": 0, "R": 0},
        }

        workplaces = []
        individuals: dict[str, dict] = {}
        workplace_index: dict[str, dict] = {}
        activities_by_individual: dict[str, dict] = {}

        for record in self.onekey.iter_population(
            cod_base=codebase,
            date_from=date_from,
            date_to=date_to,
        ):
            process_code = record.get("processCode", "U")
            result["by_process_code"][process_code] = (
                result["by_process_code"].get(process_code, 0) + 1
            )

            entity_type = (record.get("entityType") or "").upper()
            if entity_type == "WORKPLACE":
                workplaces.append(record)
                workplace = record.get("workplace") or {}
                workplace_eid = workplace.get("workplaceEid")
                if workplace_eid:
                    workplace_index[workplace_eid] = record
            elif entity_type == "INDIVIDUAL":
                individual = record.get("individual") or {}
                individual_eid = individual.get("individualEid")
                if individual_eid:
                    individuals[individual_eid] = record
            elif entity_type == "ACTIVITY":
                activity = record.get("activity") or {}
                individual_eid = activity.get("individualEid")
                if not individual_eid:
                    continue
                existing = activities_by_individual.get(individual_eid)
                if existing is None:
                    activities_by_individual[individual_eid] = record
                else:
                    existing_activity = existing.get("activity") or {}
                    if activity.get("isMainActivity") and not existing_activity.get(
                        "isMainActivity"
                    ):
                        activities_by_individual[individual_eid] = record

        logger.info(
            "Delta for %s: %d workplaces, %d individuals, %d activities "
            "(I=%d, U=%d, R=%d)",
            codebase,
            len(workplaces),
            len(individuals),
            len(activities_by_individual),
            result["by_process_code"].get("I", 0),
            result["by_process_code"].get("U", 0),
            result["by_process_code"].get("R", 0),
        )

        flattened_hcos: list[dict] = []
        if workplaces:
            flattened_hcos = [flatten_hco(r) for r in workplaces]
            hco_result, flattened_hcos = self._sync_hcos(
                codebase,
                dry_run=dry_run,
                flattened=flattened_hcos,
            )
            result["accounts"] = hco_result

            if sync_addresses:
                result["addresses"] = self._sync_addresses(
                    flattened_hcos, dry_run=dry_run
                )
            if sync_hierarchy:
                result["hierarchy"] = self._sync_hierarchy(
                    flattened_hcos, dry_run=dry_run
                )

        if activities_by_individual:
            composed = [
                compose_hcp_result(
                    individuals.get(individual_eid),
                    activity_record,
                    workplace_index.get(
                        (activity_record.get("activity") or {}).get("workplaceEid")
                    ),
                )
                for individual_eid, activity_record in activities_by_individual.items()
            ]
            result["contacts"] = self._sync_hcps(
                codebase,
                dry_run=dry_run,
                composed=composed,
            )

        return result

    def _lookup_account(self, workplace_eid: str) -> str | None:
        """Lookup account ID from Dataverse by workplace EID."""
        found = self._lookup_accounts_bulk([workplace_eid])
        return found.get(workplace_eid)
    
    def _lookup_accounts_bulk(self, workplace_eids) -> dict[str, str]:
        """Resolve many workplace EIDs to account GUIDs; update the cache."""
        key_field = self.config.get_key_field("account")
        field_name = key_field.target_field if key_field else "cr4ec_externalid"
        
        needed = [
            str(eid) for eid in workplace_eids
            if eid and str(eid) not in self._account_cache
        ]
        if not needed:
            return {
                str(eid): self._account_cache[str(eid)]
                for eid in workplace_eids
                if eid and str(eid) in self._account_cache
            }
        
        find_many = getattr(self.dataverse, "find_many_by_field", None)
        try:
            if find_many:
                rows = find_many(
                    "accounts",
                    field_name,
                    needed,
                    select=[field_name, "accountid"],
                )
                for match_value, row in rows.items():
                    account_id = row.get("accountid")
                    if account_id:
                        self._account_cache[str(match_value)] = account_id
            else:
                for eid in needed:
                    record = self.dataverse.find_one(
                        "accounts",
                        filter=f"{field_name} eq '{eid}'",
                        select=[field_name, "accountid"],
                    )
                    if record and record.get("accountid"):
                        self._account_cache[eid] = record["accountid"]
        except Exception as e:
            logger.warning("Failed bulk account lookup: %s", e)
        
        return {
            str(eid): self._account_cache[str(eid)]
            for eid in workplace_eids
            if eid and str(eid) in self._account_cache
        }


def create_sync_engine(
    onekey_client: "OneKeyClient",
    dataverse_client: "DataverseClient",
    config_dir: str = None,
    bulk_batch_size: int = None,
) -> DynamicSyncEngine:
    """Create a sync engine with configuration loaded from CSV files.
    
    Args:
        onekey_client: Connected OneKey client
        dataverse_client: Connected Dataverse client
        config_dir: Optional path to config directory
        bulk_batch_size: Optional CreateMultiple/UpdateMultiple batch size
        
    Returns:
        Configured DynamicSyncEngine
    """
    from pathlib import Path
    
    config = load_mapping_config(Path(config_dir) if config_dir else None)
    return DynamicSyncEngine(
        onekey_client,
        dataverse_client,
        config,
        bulk_batch_size=bulk_batch_size,
    )
