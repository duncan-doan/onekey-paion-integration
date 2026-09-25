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
from .contracted_ids import workplace_eid_from_record, individual_eid_from_record
from .hierarchy import apply_updates, build_updates, edges_from_hcos, resolve_accounts
from .picklist_sync import sync_picklists_from_config
from .w2w_relations import collect_w2w_relations
from .activity_affiliation import flatten_activity_for_affiliation
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

    Canonical phase order (full and delta) — contracted ``refreshMyOnekey``
    IDs only (no search enrichment):

      0. Picklists (value_mapping → Dataverse choices)
      1. HCO accounts (WORKPLACE → account)
      2. customeraddress (workplaceAddresses → parentid=account GUID)
      3. HCO hierarchy (managerWorkplaceEid → parentaccountid)
      4. HCO–HCO affiliations (workplace.relations[] → ts_affiliation)
      5. HCP contacts (INDIVIDUAL+main ACTIVITY → contact + parentcustomerid)
      6. HCP–HCO affiliations (all ACTIVITY → ts_affiliation)

    Bulk write notes: ``customeraddress`` / ``ts_affiliation`` use per-row
    create/update (no CreateMultiple/UpdateMultiple) in this org.
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
        # Contracted workplace EIDs from the latest refreshMyOnekey extract
        self._contracted_workplace_eids: set[str] = set()
        self._contracted_individual_eids: set[str] = set()
        # Cache for contact lookups (individualEid -> contactid)
        self._contact_cache: dict[str, str] = {}

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
        sync_hco_relations: bool = True,
        sync_activity_affiliations: bool = True,
        sync_picklists: bool = True,
    ) -> dict:
        """Run full migration in canonical phase order (see class docstring).

        Accounts are upserted without parent binds; hierarchy is Phase 3.
        Addresses use ``customeraddress`` only (never account/contact address1_*).
        Both HCO–HCO and HCP–HCO land in ``ts_affiliation``.
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
            "mode": "full",
            "picklists": [],
            "accounts": _empty_entity_stats(),
            "addresses": _empty_entity_stats(),
            "hierarchy": _empty_hierarchy_stats(),
            "affiliations": _empty_entity_stats(),
            "activity_affiliations": _empty_entity_stats(),
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
                    hco_result, flattened_hcos, raw_workplaces = self._sync_hcos(
                        codebase,
                        max_records=max_records,
                        dry_run=dry_run,
                    )
                    self._merge_entity(results["accounts"], hco_result)

                    contracted = self._contracted_workplace_eids

                    if sync_addresses:
                        addr_result = self._sync_addresses(
                            flattened_hcos, dry_run=dry_run
                        )
                        self._merge_entity(results["addresses"], addr_result)

                    if sync_hierarchy:
                        hier_result = self._sync_hierarchy(
                            flattened_hcos,
                            dry_run=dry_run,
                            allowed_workplace_eids=contracted,
                        )
                        self._merge_hierarchy(results["hierarchy"], hier_result)

                    if sync_hco_relations:
                        aff_result = self._sync_hco_relations(
                            raw_workplaces,
                            dry_run=dry_run,
                            allowed_workplace_eids=contracted,
                        )
                        self._merge_entity(results["affiliations"], aff_result)

                if not hco_only:
                    composed, activity_packets = self._collect_population_hcps(
                        codebase, max_records=max_records
                    )
                    hcp_result = self._sync_hcps(
                        codebase,
                        max_records=max_records,
                        dry_run=dry_run,
                        composed=composed,
                    )
                    self._merge_entity(results["contacts"], hcp_result)

                    if sync_activity_affiliations:
                        act_result = self._sync_activity_affiliations(
                            activity_packets, dry_run=dry_run
                        )
                        self._merge_entity(
                            results["activity_affiliations"], act_result
                        )

            except Exception as e:
                logger.exception("Error syncing codebase %s", codebase)
                results["errors"].append(f"{codebase}: {e}")
                results["success"] = False

        gap_path = self.transformer.picklist_gaps.write_report()
        results["picklist_gaps"] = len(self.transformer.picklist_gaps.gaps)
        results["picklist_gap_report"] = str(gap_path) if gap_path else None

        self.sync_logger.end_sync(success=results["success"])
        return results

    def sync_affiliations_only(
        self,
        codebases: list[str],
        max_records: int = None,
        dry_run: bool = False,
    ) -> dict:
        """Upsert only ``ts_affiliation`` rows from contracted population IDs.

        Walks ``refreshMyOnekey`` once per codebase, builds workplace/individual
        allowlists, looks up existing Dataverse accounts/contacts (no account or
        contact upsert), then writes:

        - HCO–HCO from ``workplace.relations[]`` (both ends contracted)
        - HCP–HCO from ACTIVITY packets (individual + workplace contracted)
        """
        self.transformer.picklist_gaps.clear()
        self.sync_logger.start_sync("affiliations_only", codebases)

        validation = self.validate_before_sync()
        if not validation.is_valid:
            logger.error("Pre-sync validation failed. Aborting sync.")
            self.sync_logger.end_sync(success=False)
            return {"success": False, "validation": validation.summary()}

        results = {
            "success": True,
            "codebases": codebases,
            "mode": "affiliations_only",
            "affiliations": _empty_entity_stats(),
            "activity_affiliations": _empty_entity_stats(),
            "picklist_gaps": 0,
            "picklist_gap_report": None,
            "errors": [],
        }

        for codebase in codebases:
            self.sync_logger.log_phase(f"AFFILIATIONS ONLY: {codebase}")
            try:
                workplaces, individuals, activities = self._collect_population_packets(
                    codebase, max_records=max_records
                )
                contracted_wp = {
                    eid
                    for eid in (workplace_eid_from_record(r) for r in workplaces)
                    if eid
                }
                contracted_ind = {
                    eid
                    for eid in (individual_eid_from_record(r) for r in individuals)
                    if eid
                }
                self._contracted_workplace_eids |= contracted_wp
                self._contracted_individual_eids |= contracted_ind

                logger.info(
                    "Contracted allowlists for %s: %d workplaces, %d individuals, "
                    "%d activity packets",
                    codebase,
                    len(contracted_wp),
                    len(contracted_ind),
                    len(activities),
                )

                if not dry_run and contracted_wp:
                    self._lookup_accounts_bulk(sorted(contracted_wp))
                if not dry_run and contracted_ind:
                    self._lookup_contacts_bulk(sorted(contracted_ind))

                hco_aff = self._sync_hco_relations(
                    workplaces,
                    dry_run=dry_run,
                    allowed_workplace_eids=contracted_wp,
                )
                self._merge_entity(results["affiliations"], hco_aff)

                act_aff = self._sync_activity_affiliations(
                    activities, dry_run=dry_run
                )
                self._merge_entity(results["activity_affiliations"], act_aff)
            except Exception as e:
                logger.exception("Error syncing affiliations for %s", codebase)
                results["errors"].append(f"{codebase}: {e}")
                results["success"] = False

        gap_path = self.transformer.picklist_gaps.write_report()
        results["picklist_gaps"] = len(self.transformer.picklist_gaps.gaps)
        results["picklist_gap_report"] = str(gap_path) if gap_path else None

        self.sync_logger.end_sync(success=results["success"])
        return results

    def _collect_population_packets(
        self,
        codebase: str,
        max_records: int = None,
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Return (workplaces, individuals, activities) from refreshMyOnekey."""
        workplaces: list[dict] = []
        individuals: list[dict] = []
        activities: list[dict] = []

        logger.info(
            "Walking refreshMyOnekey population for %s (affiliations allowlist)...",
            codebase,
        )
        for record in self.onekey.iter_population(
            cod_base=codebase, max_records=max_records
        ):
            entity_type = (record.get("entityType") or "").upper()
            if entity_type == "WORKPLACE":
                workplaces.append(record)
            elif entity_type == "INDIVIDUAL":
                individuals.append(record)
            elif entity_type == "ACTIVITY":
                activities.append(record)

        logger.info(
            "Population %s: %d workplaces, %d individuals, %d activities",
            codebase,
            len(workplaces),
            len(individuals),
            len(activities),
        )
        return workplaces, individuals, activities

    def sync_delta(
        self,
        codebases: list[str],
        date_from: str,
        date_to: str,
        dry_run: bool = False,
        sync_addresses: bool = True,
        sync_hierarchy: bool = True,
        sync_hco_relations: bool = True,
        sync_activity_affiliations: bool = True,
        sync_picklists: bool = True,
    ) -> dict:
        """Run CDC delta with the same canonical phase order as full sync."""
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
            "mode": "delta",
            "date_from": date_from,
            "date_to": date_to,
            "picklists": [],
            "accounts": _empty_entity_stats(),
            "addresses": _empty_entity_stats(),
            "hierarchy": _empty_hierarchy_stats(),
            "affiliations": _empty_entity_stats(),
            "activity_affiliations": _empty_entity_stats(),
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
                    sync_hco_relations=sync_hco_relations,
                    sync_activity_affiliations=sync_activity_affiliations,
                )
                self._merge_entity(results["accounts"], delta_result["accounts"])
                self._merge_entity(results["addresses"], delta_result["addresses"])
                self._merge_hierarchy(results["hierarchy"], delta_result["hierarchy"])
                self._merge_entity(
                    results["affiliations"], delta_result["affiliations"]
                )
                self._merge_entity(
                    results["activity_affiliations"],
                    delta_result.get("activity_affiliations") or _empty_entity_stats(),
                )
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
    ) -> tuple[dict, list[dict], list[dict]]:
        """Sync HCOs for a single codebase using bulk upserts.

        Parent ``parentaccountid`` is always deferred to ``_sync_hierarchy``.

        Returns:
            stats, flattened HCO rows, raw OneKey WORKPLACE records (for W2W).
        """
        self.sync_logger.log_phase(f"HCO SYNC: {codebase}")

        result = _empty_entity_stats()
        raw_workplaces: list[dict] = []

        if flattened is None:
            hcos = []
            logger.info(
                "Extracting HCOs from OneKey %s (refreshMyOnekey population)...",
                codebase,
            )
            for record in self.onekey.iter_population(
                cod_base=codebase, max_records=max_records
            ):
                if (record.get("entityType") or "").upper() == "WORKPLACE":
                    hcos.append(record)
            raw_workplaces = hcos
            logger.info("Extracted %d contracted HCOs from %s", len(hcos), codebase)
            flattened = [flatten_hco(r) for r in hcos]

        contracted = {
            eid
            for eid in (
                workplace_eid_from_record(r) for r in (raw_workplaces or [])
            )
            if eid
        }
        if not contracted and flattened:
            contracted = {
                str(h.get("workplaceEid"))
                for h in flattened
                if h.get("workplaceEid")
            }
        self._contracted_workplace_eids |= contracted
        if contracted:
            logger.info(
                "Contracted workplace allowlist for %s: %d EIDs "
                "(relations/hierarchy limited to this set)",
                codebase,
                len(contracted),
            )

        if not flattened:
            return result, flattened, raw_workplaces

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
            key_name = self.config.require_key_field("account")
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
            return result, flattened, raw_workplaces

        if not prepared:
            return result, flattened, raw_workplaces

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

        return result, flattened, raw_workplaces

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
            key = payload.get(self.config.require_key_field("customeraddress"))
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

        address_key = self.config.require_key_field("customeraddress")
        try:
            bulk_results = self.dataverse.bulk_upsert_by_field(
                "customeraddresses",
                address_key,
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
        allowed_workplace_eids: set[str] | None = None,
    ) -> dict:
        """Phase 3: set parentaccountid from managerWorkplaceEid edges."""
        self.sync_logger.log_phase("HCO HIERARCHY LINK")
        result = _empty_hierarchy_stats()
        allow = (
            allowed_workplace_eids
            if allowed_workplace_eids is not None
            else self._contracted_workplace_eids or None
        )
        edges = edges_from_hcos(hcos, allowed_workplace_eids=allow)
        result["edges"] = len(edges)
        if not edges:
            logger.info(
                "No parent edges to link"
                + (
                    " (after contracted-ID filter)"
                    if allow is not None
                    else ""
                )
            )
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

        account_key = self.config.require_key_field("account")
        accounts = resolve_accounts(self.dataverse, eids, account_key)
        for eid, row in accounts.items():
            if row.get("accountid"):
                self._account_cache[eid] = row["accountid"]

        updates, stats = build_updates(
            edges, accounts, account_key, skip_unchanged=skip_unchanged
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
            self.dataverse, updates, self.bulk_batch_size, match_field=account_key
        )
        result["linked"] = linked
        result["failed"] = failed
        return result

    def _affiliation_entity_set(self) -> str:
        ed = self.config.get_entity("Dataverse", "ts_affiliation")
        return (ed.entity_set if ed and ed.entity_set else "ts_affiliations")

    def _sync_hco_relations(
        self,
        raw_workplaces: list[dict],
        dry_run: bool = False,
        allowed_workplace_eids: set[str] | None = None,
    ) -> dict:
        """Phase 3b: upsert deduped HCO–HCO links into ``ts_affiliation``."""
        self.sync_logger.log_phase("HCO-HCO RELATION SYNC (ts_affiliation)")
        result = _empty_entity_stats()

        allow = (
            allowed_workplace_eids
            if allowed_workplace_eids is not None
            else self._contracted_workplace_eids or None
        )
        edges = collect_w2w_relations(
            raw_workplaces, allowed_workplace_eids=allow
        )
        if not edges:
            logger.info(
                "No HCO-HCO relations to sync"
                + (
                    " (after contracted-ID filter)"
                    if allow is not None
                    else ""
                )
            )
            return result

        logger.info(
            "Upserting %d HCO-HCO affiliation(s) from %d workplace(s) "
            "(contracted allowlist=%s)",
            len(edges),
            len(raw_workplaces),
            len(allow) if allow is not None else "off",
        )

        affiliation_key = self.config.require_key_field("ts_affiliation")
        entity_set = self._affiliation_entity_set()

        eids = sorted(
            {
                eid
                for edge in edges
                for eid in (
                    edge.get("childWorkplaceEid"),
                    edge.get("parentWorkplaceEid"),
                )
                if eid
            }
        )
        if not dry_run and eids:
            self._lookup_accounts_bulk(eids)

        prepared: list[tuple[str, str, dict, dict]] = []

        for edge in edges:
            external_id = edge.get("ts_externalid")
            child_eid = edge.get("childWorkplaceEid")
            parent_eid = edge.get("parentWorkplaceEid")

            child_id = self._account_cache.get(child_eid)
            parent_id = self._account_cache.get(parent_eid)

            if not child_id:
                result["failed"] += 1
                self.sync_logger.log_relationship_failed(
                    "ts_affiliation",
                    external_id,
                    "account",
                    child_eid,
                    f"Child account not found for {child_eid}",
                )
                continue
            if not parent_id:
                result["failed"] += 1
                self.sync_logger.log_relationship_failed(
                    "ts_affiliation",
                    external_id,
                    "account",
                    parent_eid,
                    f"Parent account not found for {parent_eid}",
                )
                continue

            payload = self.transformer.transform_w2w_affiliation(
                edge,
                child_account_id=child_id,
                parent_account_id=parent_id,
            )
            if not payload.get(affiliation_key):
                result["failed"] += 1
                self.sync_logger.log_record_error(
                    "ts_affiliation",
                    external_id,
                    "transform",
                    f"Missing key field {affiliation_key}",
                )
                continue

            prepared.append((external_id, affiliation_key, payload, edge))

        if dry_run:
            result["processed"] = len(prepared)
            logger.info("DRY RUN: would upsert %d HCO-HCO affiliations", len(prepared))
            return result

        if not prepared:
            return result

        done = 0
        for batch in chunked(prepared, self.bulk_batch_size):
            payloads = [item[2] for item in batch]
            try:
                bulk_results = self.dataverse.bulk_upsert_by_field(
                    entity_set,
                    affiliation_key,
                    payloads,
                    batch_size=self.bulk_batch_size,
                )
            except Exception as e:
                result["failed"] += len(batch)
                for external_id, *_ in batch:
                    self.sync_logger.log_record_error(
                        "ts_affiliation", external_id, "bulk_upsert", str(e)
                    )
                raise

            for (external_id, _, _payload, edge), (_, record_id, created) in zip(
                batch, bulk_results
            ):
                if created:
                    result["created"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                done += 1
                self.sync_logger.log_record_success(
                    "ts_affiliation",
                    external_id,
                    record_id,
                    "create" if created else "update",
                    f"{edge.get('childWorkplaceEid')} → {edge.get('parentWorkplaceEid')}",
                )

            self.sync_logger.log_progress("affiliations", done, len(prepared))

        return result

    def _sync_activity_affiliations(
        self,
        activity_records: list[dict],
        dry_run: bool = False,
    ) -> dict:
        """Upsert HCP–HCO Activity rows into ``ts_affiliation`` (AFF-*)."""
        self.sync_logger.log_phase("HCP-HCO ACTIVITY AFFILIATION SYNC (ts_affiliation)")
        result = _empty_entity_stats()

        if not activity_records:
            logger.info("No HCP-HCO activities to sync")
            return result

        logger.info(
            "Upserting %d HCP-HCO activity affiliation(s)",
            len(activity_records),
        )

        affiliation_key = self.config.require_key_field("ts_affiliation")
        entity_set = self._affiliation_entity_set()

        flat_rows: list[dict] = []
        allow_wp = self._contracted_workplace_eids or None
        allow_ind = self._contracted_individual_eids or None
        skipped_out_of_contract = 0
        for record in activity_records:
            flat = flatten_activity_for_affiliation(record)
            if not (
                flat.get("activityEid")
                and flat.get("individualEid")
                and flat.get("workplaceEid")
            ):
                continue
            if allow_wp is not None and flat["workplaceEid"] not in allow_wp:
                skipped_out_of_contract += 1
                continue
            if allow_ind is not None and flat["individualEid"] not in allow_ind:
                skipped_out_of_contract += 1
                continue
            flat_rows.append(flat)

        if skipped_out_of_contract:
            logger.info(
                "Skipped %d HCP-HCO affiliations outside contracted population "
                "(workplace and/or individual)",
                skipped_out_of_contract,
            )

        individual_eids = sorted(
            {row["individualEid"] for row in flat_rows if row.get("individualEid")}
        )
        workplace_eids = sorted(
            {row["workplaceEid"] for row in flat_rows if row.get("workplaceEid")}
        )
        if not dry_run:
            if individual_eids:
                self._lookup_contacts_bulk(individual_eids)
            if workplace_eids:
                self._lookup_accounts_bulk(workplace_eids)

        prepared: list[tuple[str, str, dict, dict]] = []

        for flat in flat_rows:
            external_id = flat["activityEid"]
            contact_id = self._contact_cache.get(flat["individualEid"])
            account_id = self._account_cache.get(flat["workplaceEid"])

            if not contact_id:
                result["failed"] += 1
                self.sync_logger.log_relationship_failed(
                    "ts_affiliation",
                    external_id,
                    "contact",
                    flat["individualEid"],
                    f"Contact not found for {flat['individualEid']}",
                )
                continue
            if not account_id:
                result["failed"] += 1
                self.sync_logger.log_relationship_failed(
                    "ts_affiliation",
                    external_id,
                    "account",
                    flat["workplaceEid"],
                    f"Account not found for {flat['workplaceEid']}",
                )
                continue

            payload = self.transformer.transform_activity_affiliation(
                flat,
                contact_id=contact_id,
                account_id=account_id,
            )
            if not payload.get(affiliation_key):
                result["failed"] += 1
                self.sync_logger.log_record_error(
                    "ts_affiliation",
                    external_id,
                    "transform",
                    f"Missing key field {affiliation_key}",
                )
                continue

            prepared.append((external_id, affiliation_key, payload, flat))

        if dry_run:
            result["processed"] = len(prepared)
            logger.info(
                "DRY RUN: would upsert %d HCP-HCO activity affiliations",
                len(prepared),
            )
            return result

        if not prepared:
            return result

        done = 0
        for batch in chunked(prepared, self.bulk_batch_size):
            payloads = [item[2] for item in batch]
            try:
                bulk_results = self.dataverse.bulk_upsert_by_field(
                    entity_set,
                    affiliation_key,
                    payloads,
                    batch_size=self.bulk_batch_size,
                )
            except Exception as e:
                result["failed"] += len(batch)
                for external_id, *_ in batch:
                    self.sync_logger.log_record_error(
                        "ts_affiliation", external_id, "bulk_upsert", str(e)
                    )
                raise

            for (external_id, _, _payload, flat), (_, record_id, created) in zip(
                batch, bulk_results
            ):
                if created:
                    result["created"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                done += 1
                self.sync_logger.log_record_success(
                    "ts_affiliation",
                    external_id,
                    record_id,
                    "create" if created else "update",
                    f"{flat.get('individualEid')} → {flat.get('workplaceEid')}",
                )

            self.sync_logger.log_progress("activity_affiliations", done, len(prepared))

        return result

    def _collect_population_hcps(
        self,
        codebase: str,
        max_records: int = None,
        date_from: str = None,
        date_to: str = None,
    ) -> tuple[list[dict], list[dict]]:
        """Collect HCP contact rows + all ACTIVITY packets from population.

        Returns:
            ``(composed_hcp_rows, all_activity_packets)``

        Contact composition keeps one ACTIVITY per individual (prefer
        ``isMainActivity``). Affiliation sync uses ``all_activity_packets``.
        """
        individuals: dict[str, dict] = {}
        workplaces: dict[str, dict] = {}
        activities_by_individual: dict[str, dict] = {}
        all_activities: list[dict] = []

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
                all_activities.append(record)
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
        skipped_unjoined = 0
        for individual_eid, activity_record in activities_by_individual.items():
            activity = activity_record.get("activity") or {}
            workplace_eid = activity.get("workplaceEid")
            # Only process IDs present in refreshMyOnekey packets — do not
            # fall back to search/get_by_id for missing workplace/individual.
            if individual_eid not in individuals:
                skipped_unjoined += 1
                continue
            if workplace_eid and workplace_eid not in workplaces:
                skipped_unjoined += 1
                logger.debug(
                    "Skipping activity for %s: workplace %s not in contracted "
                    "population",
                    individual_eid,
                    workplace_eid,
                )
                continue
            if not workplace_eid:
                skipped_unjoined += 1
                continue
            composed.append(
                compose_hcp_result(
                    individuals.get(individual_eid),
                    activity_record,
                    workplaces.get(workplace_eid),
                )
            )

        contracted_wp = set(workplaces)
        self._contracted_workplace_eids |= contracted_wp
        self._contracted_individual_eids |= set(individuals)

        logger.info(
            "Joined %d HCPs from %s (%d individuals, %d main activities, "
            "%d activity packets, %d workplaces indexed; skipped %d outside "
            "contracted population)",
            len(composed),
            codebase,
            len(individuals),
            len(activities_by_individual),
            len(all_activities),
            len(workplaces),
            skipped_unjoined,
        )
        return composed, all_activities

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
            composed, _activity_packets = self._collect_population_hcps(
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
            key_name = self.config.require_key_field("contact")
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
                if contact_id:
                    self._contact_cache[str(individual_eid)] = contact_id
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
        sync_hco_relations: bool = True,
        sync_activity_affiliations: bool = True,
    ) -> dict:
        """Sync delta changes for a single codebase using bulk upserts."""
        result = {
            "accounts": _empty_entity_stats(),
            "addresses": _empty_entity_stats(),
            "hierarchy": _empty_hierarchy_stats(),
            "affiliations": _empty_entity_stats(),
            "activity_affiliations": _empty_entity_stats(),
            "contacts": _empty_entity_stats(),
            "by_process_code": {"I": 0, "U": 0, "R": 0},
        }

        workplaces = []
        individuals: dict[str, dict] = {}
        workplace_index: dict[str, dict] = {}
        activities_by_individual: dict[str, dict] = {}
        all_activities: list[dict] = []

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
                    self._contracted_workplace_eids.add(str(workplace_eid))
            elif entity_type == "INDIVIDUAL":
                individual = record.get("individual") or {}
                individual_eid = individual.get("individualEid")
                if individual_eid:
                    individuals[individual_eid] = record
                    self._contracted_individual_eids.add(str(individual_eid))
            elif entity_type == "ACTIVITY":
                all_activities.append(record)
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
            "Delta for %s: %d workplaces, %d individuals, %d main activities, "
            "%d activity packets (I=%d, U=%d, R=%d)",
            codebase,
            len(workplaces),
            len(individuals),
            len(activities_by_individual),
            len(all_activities),
            result["by_process_code"].get("I", 0),
            result["by_process_code"].get("U", 0),
            result["by_process_code"].get("R", 0),
        )

        flattened_hcos: list[dict] = []
        if workplaces:
            flattened_hcos = [flatten_hco(r) for r in workplaces]
            hco_result, flattened_hcos, _ = self._sync_hcos(
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
                    flattened_hcos,
                    dry_run=dry_run,
                    allowed_workplace_eids=self._contracted_workplace_eids or None,
                )
            if sync_hco_relations:
                result["affiliations"] = self._sync_hco_relations(
                    workplaces,
                    dry_run=dry_run,
                    allowed_workplace_eids=self._contracted_workplace_eids or None,
                )

        if activities_by_individual:
            composed = []
            skipped = 0
            for individual_eid, activity_record in activities_by_individual.items():
                activity = activity_record.get("activity") or {}
                workplace_eid = activity.get("workplaceEid")
                if individual_eid not in individuals:
                    skipped += 1
                    continue
                if not workplace_eid or workplace_eid not in workplace_index:
                    skipped += 1
                    continue
                composed.append(
                    compose_hcp_result(
                        individuals.get(individual_eid),
                        activity_record,
                        workplace_index.get(workplace_eid),
                    )
                )
            if skipped:
                logger.info(
                    "Delta HCP: skipped %d activities outside contracted "
                    "population join",
                    skipped,
                )
            result["contacts"] = self._sync_hcps(
                codebase,
                dry_run=dry_run,
                composed=composed,
            )

        if sync_activity_affiliations and all_activities:
            result["activity_affiliations"] = self._sync_activity_affiliations(
                all_activities, dry_run=dry_run
            )

        return result

    def _lookup_contacts_bulk(self, individual_eids) -> dict[str, str]:
        """Resolve many individual EIDs to contact GUIDs; update the cache."""
        field_name = self.config.require_key_field("contact")

        needed = [
            str(eid)
            for eid in individual_eids
            if eid and str(eid) not in self._contact_cache
        ]
        if not needed:
            return {
                str(eid): self._contact_cache[str(eid)]
                for eid in individual_eids
                if eid and str(eid) in self._contact_cache
            }

        find_many = getattr(self.dataverse, "find_many_by_field", None)
        try:
            if find_many:
                rows = find_many(
                    "contacts",
                    field_name,
                    needed,
                    select=[field_name, "contactid"],
                )
                for match_value, row in rows.items():
                    contact_id = row.get("contactid")
                    if contact_id:
                        self._contact_cache[str(match_value)] = contact_id
            else:
                for eid in needed:
                    record = self.dataverse.find_one(
                        "contacts",
                        filter=f"{field_name} eq '{eid}'",
                        select=[field_name, "contactid"],
                    )
                    if record and record.get("contactid"):
                        self._contact_cache[eid] = record["contactid"]
        except Exception as e:
            logger.warning("Failed bulk contact lookup: %s", e)

        return {
            str(eid): self._contact_cache[str(eid)]
            for eid in individual_eids
            if eid and str(eid) in self._contact_cache
        }

    def _lookup_account(self, workplace_eid: str) -> str | None:
        """Lookup account ID from Dataverse by workplace EID."""
        found = self._lookup_accounts_bulk([workplace_eid])
        return found.get(workplace_eid)
    
    def _lookup_accounts_bulk(self, workplace_eids) -> dict[str, str]:
        """Resolve many workplace EIDs to account GUIDs; update the cache."""
        field_name = self.config.require_key_field("account")

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
