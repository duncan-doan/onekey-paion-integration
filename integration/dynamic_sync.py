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
import time
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import TYPE_CHECKING

from .dynamic_mapping import load_mapping_config, MappingConfig
from .hco_name import hcp_name_by_workplace
from .env import skip_unchanged
from .transform import TransformEngine
from .validation import MappingValidator, DataverseValidator, ValidationResult
from .sync_logger import SyncLogger, msg, setup_logging, short_job
from .contracted_ids import workplace_eid_from_record, individual_eid_from_record
from .hierarchy import apply_updates, build_updates, edges_from_hcos, resolve_accounts
from .lookup_sync import CatalogResolver, CountryResolver, sync_lookup_tables
from .address_rows import address_id, address_sources, plan_address_rows, workplace_batches
from .picklist_sync import sync_picklists_from_config
from .w2w_relations import (
    WORKPLACE_TO_WORKPLACE_LINK,
    collect_w2w_relations,
    explode_w2w_relations,
)
from .activity_affiliation import flatten_activity_for_affiliation
from .job_trace import (
    ENTITY_BY_NAME,
    JOB_AFFILIATIONS,
    JOB_DELTA,
    JOB_FULL,
    JOB_HCO,
    JOB_HCP,
    PHASE_ADDRESSES,
    PHASE_HCO,
    PHASE_HCO_RELATIONS,
    PHASE_HCP,
    PHASE_HCP_ACTIVITY,
    PHASE_HIERARCHY,
    PHASE_PICKLISTS,
    JobTracer,
)
from onekey.parse import compose_hcp_result, flatten_hcp, flatten_hco
from dataverse.client import chunked, escape_odata

if TYPE_CHECKING:
    from onekey import OneKeyClient
    from dataverse import DataverseClient

logger = logging.getLogger(__name__)

_EMPTY_ENTITY = {"processed": 0, "created": 0, "updated": 0, "skipped": 0, "failed": 0}
_EMPTY_HIERARCHY = {
    "edges": 0,
    "linked": 0,
    "failed": 0,
    "already_linked": 0,
    "missing_child": 0,
    "missing_parent": 0,
    # Parents cleared: OneKey no longer sends a managerWorkplaceEid.
    "unlinked": 0,
}


class SyncPaused(RuntimeError):
    """The run spent its time budget; it stopped between two batches.

    Raised by DynamicSyncEngine._check_deadline. The sync methods catch it,
    record the job as not covering its window, and return with ``paused``
    set, so the queue worker can queue a continuation (integration.queue_sync).
    ``phase`` and ``stats`` carry the counts of the phase it stopped in, which
    would otherwise be lost with the phase's return value.
    """

    def __init__(self, message: str, phase: str | None = None, stats: dict | None = None):
        super().__init__(message)
        self.phase = phase
        self.stats = stats
        # Set by _sync_delta_codebase: the counts of the phases it finished.
        self.delta_result: dict | None = None


def _ctx(engine, **context) -> dict:
    """Context for an engine log line: the given fields, then the short job ID."""
    return {**context, "job": short_job(engine.tracer.job_id)}


def _empty_entity_stats() -> dict:
    return dict(_EMPTY_ENTITY)


def _codebase_outcome(failed: bool, results: dict) -> str:
    """The codebase phase's logged outcome: error, paused or ok."""
    if failed:
        return "error"
    return "paused" if results.get("paused") else "ok"


def _operation(created: bool, unchanged: bool) -> str:
    """Record log operation: create, skip (already up to date) or update."""
    if created:
        return "create"
    return "skip" if unchanged else "update"


def main_activity_by_individual(activity_records) -> dict[str, dict]:
    """One ACTIVITY per individual: the first marked ``isMainActivity``, else the first.

    The full sync and the delta both pick a contact's activity with this, so
    they agree on which workplace is its parent.
    """
    picked: dict[str, dict] = {}
    for record in activity_records:
        activity = record.get("activity") or {}
        individual_eid = activity.get("individualEid")
        if not individual_eid:
            continue
        existing = picked.get(individual_eid)
        if existing is None or (
            activity.get("isMainActivity")
            and not (existing.get("activity") or {}).get("isMainActivity")
        ):
            picked[individual_eid] = record
    return picked


def _empty_hierarchy_stats() -> dict:
    return dict(_EMPTY_HIERARCHY)


class DynamicSyncEngine:
    """Orchestrates OneKey to Dataverse synchronization.

    Canonical phase order (full and delta) — contracted ``refreshMyOnekey``
    IDs only (no search enrichment):

      0. Picklists (value_mapping → Dataverse choices)
      1. HCO accounts (WORKPLACE → account), including ts_hierarchylevel
         derived from the complete population's Contact placement chain
      2. customeraddress (workplaceAddresses → parentid=account GUID)
      3. HCO hierarchy (managerWorkplaceEid → parentaccountid)
      4. HCO–HCO affiliations (workplace.relations[] → ts_affiliation)
      5. HCP contacts (INDIVIDUAL+main ACTIVITY → contact + ts_department / ts_hospital / ts_hospitalgroup)
      6. HCP–HCO affiliations (all ACTIVITY → ts_affiliation)

    Bulk write notes: ``customeraddress`` has no CreateMultiple or
    UpdateMultiple, so its creates and updates are per-row POSTs and
    PATCHes in one $batch. ``ts_affiliation`` uses CreateMultiple, which
    keeps an inactive record's statecode, and UpdateMultiple (see
    ``dataverse.client._NO_*_MULTIPLE``).
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
        self.catalogs = CatalogResolver(dataverse_client)
        self.transformer.catalog_resolver = self.catalogs.ensure
        self.countries = CountryResolver(dataverse_client)
        self.transformer.country_resolver = self.countries.bind
        self.mapping_validator = MappingValidator(self.config)
        self.dataverse_validator = DataverseValidator(dataverse_client)
        self.sync_logger = sync_logger or SyncLogger()
        self.bulk_batch_size = bulk_batch_size or getattr(
            getattr(dataverse_client, "config", None), "bulk_batch_size", None
        ) or 100
        # Do not update a record that already holds the values to write.
        self.skip_unchanged = skip_unchanged()
        self.tracer = JobTracer(
            dataverse_client, batch_size=self.bulk_batch_size
        )
        self._trace_phase = PHASE_HCO
        self.succeeded_ids: set[str] = set()
        self.succeeded_records: set[tuple[int, int, str, str]] = set()

        # Cache for account lookups (workplaceEid -> accountid)
        self._account_cache: dict[str, str] = {}
        # Account GUID -> current ts_hierarchylevel. This is intentionally
        # per-job: delta fallbacks can reuse Account rows already read in the
        # same run without trusting values cached before the job started.
        self._account_hierarchy_level_cache: dict[str, int | None] = {}
        # Contracted workplace EIDs from the latest refreshMyOnekey extract
        self._contracted_workplace_eids: set[str] = set()
        self._contracted_individual_eids: set[str] = set()
        # Cache for contact lookups (individualEid -> contactid)
        self._contact_cache: dict[str, str] = {}
        # One HCP name per nameless workplace, from the same population extract.
        self._hcp_name_by_workplace: dict[str, str] = {}
        # False only while sync_full holds a whole population walk. A delta
        # window, a retry or a sample sees part of it at most, so existing
        # accounts keep their name (_keep_existing_nameless_names), and a link
        # to a record this run did not read counts as contracted when the
        # record is already in Dataverse (_admit_known).
        self._partial_population = True
        # time.monotonic() after which a run stops between batches, or None.
        # Queue workers set it: Flex Consumption stops an execution 60
        # minutes into a scale-in, so a run ends cleanly before that and a
        # continuation message picks up the rest.
        self.deadline: float | None = None
        # Records created or updated by the current job (not skipped or
        # rejected): a paused run that wrote none made no progress.
        self.records_written = 0

    def close(self) -> None:
        """Release what the engine holds open: its sync log file."""
        self.sync_logger.close()

    def _check_deadline(
        self, before: str, phase: str | None = None, stats: dict | None = None
    ) -> None:
        """Raise SyncPaused when the time budget is spent; call between batches.

        ``phase`` is the results key of the running phase and ``stats`` its
        counts so far, so a pause does not lose them.
        """
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise SyncPaused(f"time budget spent before a {before} batch", phase, stats)

    def validate_before_sync(self) -> ValidationResult:
        """Run all validation checks before starting sync."""
        started = self.sync_logger.phase_started("validation")

        result = ValidationResult()

        mapping_result = self.mapping_validator.validate_mapping_config()
        result.merge(mapping_result)
        self.sync_logger.log_validation_result(mapping_result, "Mapping Config")

        schema_result = self.dataverse_validator.validate_schema(self.config)
        result.merge(schema_result)
        self.sync_logger.log_validation_result(schema_result, "Dataverse Schema")

        self.sync_logger.phase_finished(
            "validation",
            started,
            {"errors": len(result.errors), "warnings": len(result.warnings)},
            outcome="ok" if result.is_valid else "invalid",
        )
        return result

    def _begin_job(
        self,
        job_type: int,
        codebases: list[str],
        dry_run: bool,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> None:
        self.succeeded_ids.clear()
        self.succeeded_records.clear()
        self._account_hierarchy_level_cache = {}
        self.records_written = 0
        self.tracer.begin(
            job_type=job_type,
            codebases=codebases,
            dry_run=dry_run,
            date_from=date_from,
            date_to=date_to,
        )
        self.sync_logger.job_id = self.tracer.job_id
        self._http_baseline = self._http_snapshot()
        self._set_request_hook(self.tracer.beat)

    def _checkpoint(self, results: dict | None) -> None:
        self.tracer.checkpoint(results)

    def _finish_job(self, results: dict | None, aborted: bool = False) -> None:
        self._set_request_hook(None)
        if isinstance(results, dict):
            results["records_written"] = self.records_written
        self.tracer.finish(results, aborted=aborted)
        self._log_http_summary()

    def _set_request_hook(self, hook) -> None:
        """Point both clients' after_request at ``hook`` (None clears it).

        While a job runs, each OneKey or Dataverse request gives the tracer a
        chance to refresh the heartbeat, so long phases still look alive to
        the queue worker's busy check.
        """
        clients = (getattr(self.onekey, "session", None), self.dataverse)
        for client in clients:
            if client is not None and hasattr(client, "after_request"):
                client.after_request = hook

    def _http_snapshot(self) -> dict[str, dict]:
        """Request totals of the OneKey and Dataverse clients, by service."""
        clients = {
            "onekey": getattr(getattr(self.onekey, "session", None), "stats", None),
            "dataverse": getattr(self.dataverse, "stats", None),
        }
        return {
            service: stats.as_dict()
            for service, stats in clients.items()
            if hasattr(stats, "as_dict")
        }

    def _log_http_summary(self) -> None:
        """Log one http_summary line per service for the job that just finished."""
        baseline = getattr(self, "_http_baseline", {}) or {}
        for service, now in self._http_snapshot().items():
            before = baseline.get(service, {})
            delta = {key: now[key] - before.get(key, 0) for key in now}
            calls = delta["calls"]
            if not calls:
                continue
            logger.info(
                msg(
                    f"{'OneKey' if service == 'onekey' else 'Dataverse'} calls",
                    _ctx(self),
                    calls=calls,
                    retries=delta["attempts"] - calls,
                    throttled=delta["throttled"],
                    failed=delta["failed"],
                    seconds=delta["seconds"],
                    avg_ms=round(delta["seconds"] * 1000 / calls),
                )
            )

    @contextmanager
    def _phase(self, phase: int, name: str, **fields):
        """Set the trace phase and log phase_start / phase_complete with timing.

        The body stores its counts in the yielded dict under ``"stats"``.
        """
        self._trace_phase = phase
        outcome: dict = {}
        started = self.sync_logger.phase_started(name, **fields)
        try:
            yield outcome
        except SyncPaused:
            self.sync_logger.phase_finished(
                name, started, outcome.get("stats"), outcome="paused", **fields
            )
            raise
        except Exception as exc:
            self.sync_logger.phase_finished(
                name, started, outcome.get("stats"), outcome="error",
                error=type(exc).__name__, **fields,
            )
            raise
        else:
            self.sync_logger.phase_finished(name, started, outcome.get("stats"), **fields)
        finally:
            self.tracer.flush(heartbeat=True)

    def _fail(
        self,
        entity: str,
        source_id: str | None,
        operation: str,
        message: str,
        *,
        external_id: str | None = None,
        category: str | None = None,
        retryable: bool | None = None,
        exc: BaseException | None = None,
        log: bool = True,
    ) -> None:
        if log:
            self.sync_logger.log_record_error(
                entity, source_id or "", operation, message
            )
        self.tracer.record_failure(
            phase=self._trace_phase,
            entity=entity,
            source_id=source_id,
            operation=operation,
            message=message,
            external_id=external_id,
            category=category,
            retryable=retryable,
            exc=exc,
        )

    def _note_success(
        self,
        *source_ids: str | None,
        entity: str | int | None = None,
        external_id: str | None = None,
    ) -> None:
        """Remember successful writes with enough context to resolve failures."""
        entity_value = ENTITY_BY_NAME.get(entity) if isinstance(entity, str) else entity
        for source_id in source_ids:
            if source_id:
                source = str(source_id)
                self.succeeded_ids.add(source)
                if entity_value is not None:
                    self.succeeded_records.add(
                        (self._trace_phase, entity_value, source, str(external_id or ""))
                    )

    def sync_picklists(
        self,
        dry_run: bool = False,
        codebases: list[str] | None = None,
        choices: bool = True,
    ) -> list[dict]:
        """Phase 0: choice options, then code-list lookup tables.

        Choice options still come from ``value_mapping.csv``. Lists that live
        in lookup tables (specialty, HCO type, title, …) are loaded from
        OneKey ``/ok/searchCodeList``. Their ``ts_externalid`` is the template in
        ``lookup_table_mapping.csv`` (``WUK.TET.HOS``).

        ``choices=False`` skips the choice options: they do not depend on the
        codebase, so the scheduled timer syncs them once for every country.
        """
        with self._phase(PHASE_PICKLISTS, "picklists") as phase:
            picklists: list[dict] = []
            if not choices:
                logger.info(msg("Choice picklists skipped, already synced for this run", _ctx(self)))
            elif hasattr(self.dataverse, "request"):
                picklists.extend(
                    sync_picklists_from_config(
                        self.dataverse, self.config, dry_run=dry_run
                    )
                )
            else:
                logger.info(
                    msg("Choice picklists skipped, Dataverse client has no metadata API", _ctx(self))
                )
            if codebases:
                picklists.extend(
                    sync_lookup_tables(
                        self.dataverse,
                        self.onekey,
                        self.config,
                        codebases,
                        dry_run=dry_run,
                    )
                )
            self.catalogs.dry_run = dry_run
            if not dry_run:
                self.catalogs.preload(self.config.lookup_tables)
            phase["stats"] = {"types": len(picklists)}
            return picklists

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
        sync_choices: bool = True,
    ) -> dict:
        """Run full migration in canonical phase order (see class docstring).

        Accounts are upserted without parent binds; hierarchy is Phase 3.
        Addresses use ``customeraddress`` only (never account/contact address1_*).
        Both HCO–HCO and HCP–HCO land in ``ts_affiliation``.
        """
        if hco_only and hcp_only:
            raise ValueError("hco_only and hcp_only cannot both be True")

        if hco_only:
            job_type = JOB_HCO
            sync_label = "hco_only"
        elif hcp_only:
            job_type = JOB_HCP
            sync_label = "hcp_only"
        else:
            job_type = JOB_FULL
            sync_label = "full"

        results = {
            "success": False,
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
        aborted = False
        self._begin_job(job_type, codebases, dry_run)
        try:
            self.transformer.picklist_gaps.clear()
            self.sync_logger.start_sync(sync_label, codebases)

            validation = self.validate_before_sync()
            if not validation.is_valid:
                logger.error(msg("Sync aborted, pre-sync validation failed", _ctx(self)))
                results["validation"] = validation.summary()
                results["errors"].append(validation.summary())
                self.sync_logger.end_sync(success=False, results=results)
                return results

            results["success"] = True

            if sync_picklists:
                self._trace_phase = PHASE_PICKLISTS
                try:
                    results["picklists"] = self.sync_picklists(
                        dry_run=dry_run, codebases=codebases, choices=sync_choices
                    )
                except Exception as e:
                    # Missing option sets on a fresh env must not block HCO/HCP load.
                    logger.exception(
                        msg("Picklist sync failed, continuing without option upsert", _ctx(self))
                    )
                    results["errors"].append(f"picklists: {e}")
                    results["picklists"] = []
                self._checkpoint(results)
            else:
                self.catalogs.dry_run = dry_run
                if not dry_run:
                    self.catalogs.preload(self.config.lookup_tables)

            for codebase in codebases:
                codebase_started = self.sync_logger.phase_started("codebase", codbase=codebase)
                codebase_failed = False
                try:
                    flattened_hcos: list[dict] = []
                    # One population walk feeds both halves: a walk is hundreds
                    # of paced packets, and two would read two snapshots.
                    packets = None
                    self._hcp_name_by_workplace = {}
                    # max_records stops the walk early, so it sees some names only.
                    self._partial_population = bool(max_records)
                    if not hco_only and not hcp_only:
                        packets = self._collect_population_packets(
                            codebase, max_records=max_records
                        )
                        self._hcp_name_by_workplace = hcp_name_by_workplace(
                            packets[1], packets[2]
                        )

                    # A complete full population already contains every
                    # input needed to derive the Account level. Put it in the
                    # normal Account upsert instead of reading and updating
                    # Accounts again after the Contact phase. A sampled run
                    # is incomplete, so it keeps the safe promotion fallback.
                    source_hierarchy_levels = (
                        self._population_hierarchy_levels(*packets)
                        if packets is not None and not max_records
                        else None
                    )

                    if not hcp_only:
                        hco_result, flattened_hcos, raw_workplaces = self._sync_hcos(
                            codebase,
                            max_records=max_records,
                            dry_run=dry_run,
                            raw_workplaces=packets[0] if packets is not None else None,
                            hierarchy_levels=source_hierarchy_levels,
                        )
                        self._merge_entity(results["accounts"], hco_result)
                        self._checkpoint(results)

                        contracted = self._contracted_workplace_eids

                        if sync_addresses:
                            addr_result = self._sync_addresses(
                                flattened_hcos, dry_run=dry_run
                            )
                            self._merge_entity(results["addresses"], addr_result)
                            self._checkpoint(results)

                        if sync_hierarchy:
                            hier_result = self._sync_hierarchy(
                                flattened_hcos,
                                dry_run=dry_run,
                                allowed_workplace_eids=contracted,
                            )
                            self._merge_hierarchy(results["hierarchy"], hier_result)
                            self._checkpoint(results)

                        if sync_hco_relations:
                            aff_result = self._sync_hco_relations(
                                raw_workplaces,
                                dry_run=dry_run,
                                allowed_workplace_eids=contracted,
                            )
                            self._merge_entity(results["affiliations"], aff_result)
                            self._checkpoint(results)

                    if not hco_only:
                        if packets is not None:
                            composed, stats = self._compose_population_hcps(*packets)
                            activity_packets = packets[2]
                            logger.info(
                                msg("HCP rows composed", _ctx(self, codbase=codebase), **stats)
                            )
                        else:
                            composed, activity_packets = self._collect_population_hcps(
                                codebase, max_records=max_records
                            )
                        hcp_result = self._sync_hcps(
                            codebase,
                            max_records=max_records,
                            dry_run=dry_run,
                            composed=composed,
                            update_hierarchy_levels=source_hierarchy_levels is None,
                        )
                        self._merge_entity(results["contacts"], hcp_result)
                        self._checkpoint(results)

                        if sync_activity_affiliations:
                            act_result = self._sync_activity_affiliations(
                                activity_packets, dry_run=dry_run
                            )
                            self._merge_entity(
                                results["activity_affiliations"], act_result
                            )
                            self._checkpoint(results)

                except SyncPaused as exc:
                    self._record_pause(results, codebase, exc)
                except Exception as e:
                    logger.exception(msg(f"Codebase {codebase} failed", _ctx(self)))
                    results["errors"].append(f"{codebase}: {e}")
                    results["success"] = False
                    codebase_failed = True
                    self._checkpoint(results)
                self.sync_logger.phase_finished(
                    "codebase",
                    codebase_started,
                    outcome=_codebase_outcome(codebase_failed, results),
                    codbase=codebase,
                )
                if results.get("paused"):
                    break  # the deadline holds for every codebase

            gap_path = self.transformer.picklist_gaps.write_report()
            results["picklist_gaps"] = len(self.transformer.picklist_gaps.gaps)
            results["picklist_gap_report"] = str(gap_path) if gap_path else None

            self.sync_logger.end_sync(success=results["success"], results=results)
            return results
        except Exception as exc:
            aborted = True
            message = str(exc)
            if message and message not in results["errors"]:
                results["errors"].append(message)
            raise
        finally:
            self._finish_job(results, aborted=aborted)

    def _record_pause(self, results: dict, codebase: str, exc: SyncPaused) -> None:
        """Keep a paused phase's counts and mark the job as not covering its window.

        The error entry is what keeps the job from covering its window
        (scheduled_sync.covered_window ignores only picklist errors), so the
        next delta, or the continuation, reads it again.
        """
        if exc.phase in results and exc.stats:
            self._merge_entity(results[exc.phase], exc.stats)
        results["paused"] = True
        results["errors"].append(f"{codebase}: paused, {exc}")
        logger.warning(
            msg(
                f"Codebase {codebase} paused; a continuation finishes it",
                _ctx(self),
                reason=str(exc),
                written=self.records_written,
            )
        )
        self._checkpoint(results)

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
        results = {
            "success": False,
            "codebases": codebases,
            "mode": "affiliations_only",
            "affiliations": _empty_entity_stats(),
            "activity_affiliations": _empty_entity_stats(),
            "picklist_gaps": 0,
            "picklist_gap_report": None,
            "errors": [],
        }
        aborted = False
        self._begin_job(JOB_AFFILIATIONS, codebases, dry_run)
        try:
            self.transformer.picklist_gaps.clear()
            self.sync_logger.start_sync("affiliations_only", codebases)

            validation = self.validate_before_sync()
            if not validation.is_valid:
                logger.error(msg("Sync aborted, pre-sync validation failed", _ctx(self)))
                results["validation"] = validation.summary()
                results["errors"].append(validation.summary())
                self.sync_logger.end_sync(success=False, results=results)
                return results

            results["success"] = True

            for codebase in codebases:
                codebase_started = self.sync_logger.phase_started("codebase", codbase=codebase)
                codebase_failed = False
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
                        msg(
                            "Contracted allowlist",
                            _ctx(self, codbase=codebase),
                            workplaces=len(contracted_wp),
                            individuals=len(contracted_ind),
                            activity_packets=len(activities),
                        )
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
                    self._checkpoint(results)

                    act_aff = self._sync_activity_affiliations(
                        activities, dry_run=dry_run
                    )
                    self._merge_entity(results["activity_affiliations"], act_aff)
                    self._checkpoint(results)
                except Exception as e:
                    logger.exception(msg(f"Codebase {codebase} failed", _ctx(self)))
                    results["errors"].append(f"{codebase}: {e}")
                    results["success"] = False
                    codebase_failed = True
                    self._checkpoint(results)
                self.sync_logger.phase_finished(
                    "codebase",
                    codebase_started,
                    outcome="error" if codebase_failed else "ok",
                    codbase=codebase,
                )

            gap_path = self.transformer.picklist_gaps.write_report()
            results["picklist_gaps"] = len(self.transformer.picklist_gaps.gaps)
            results["picklist_gap_report"] = str(gap_path) if gap_path else None

            self.sync_logger.end_sync(success=results["success"], results=results)
            return results
        except Exception as exc:
            aborted = True
            message = str(exc)
            if message and message not in results["errors"]:
                results["errors"].append(message)
            raise
        finally:
            self._finish_job(results, aborted=aborted)

    @staticmethod
    def _split_population(records) -> tuple[list[dict], list[dict], list[dict]]:
        """Sort population records into (workplaces, individuals, activities)."""
        workplaces: list[dict] = []
        individuals: list[dict] = []
        activities: list[dict] = []
        for record in records:
            entity_type = (record.get("entityType") or "").upper()
            if entity_type == "WORKPLACE":
                workplaces.append(record)
            elif entity_type == "INDIVIDUAL":
                individuals.append(record)
            elif entity_type == "ACTIVITY":
                activities.append(record)
        return workplaces, individuals, activities

    def _collect_population_packets(
        self,
        codebase: str,
        max_records: int = None,
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Return (workplaces, individuals, activities) from refreshMyOnekey."""
        started = self.sync_logger.phase_started("extract", codbase=codebase)
        workplaces, individuals, activities = self._split_population(
            self.onekey.iter_population(cod_base=codebase, max_records=max_records)
        )

        self.sync_logger.phase_finished(
            "extract",
            started,
            {
                "workplaces": len(workplaces),
                "individuals": len(individuals),
                "activities": len(activities),
            },
            codbase=codebase,
        )
        return workplaces, individuals, activities

    def _population_hierarchy_levels(
        self,
        workplace_records: list[dict],
        individual_records: list[dict],
        activity_records: list[dict],
    ) -> dict[str, int]:
        """Derive ``{workplaceEid: hierarchy level}`` from a full population.

        This follows the same chain used by Contact placement: the main
        activity's workplace is the department; its manager (or active HIE
        group parent) is the hospital; and that hospital's equivalent parent
        is the group. When a workplace has multiple roles, the role closest
        to an HCP wins (3 > 2 > 1).
        """
        workplaces = {
            str(eid): record.get("workplace") or record
            for record in workplace_records
            if (eid := workplace_eid_from_record(record))
        }
        individuals = {
            str(eid)
            for record in individual_records
            if (eid := individual_eid_from_record(record))
        }
        if not workplaces or not individuals:
            return {}

        allowed = set(workplaces)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        latest_hie: dict[str, tuple[str, str]] = {}
        for edge in collect_w2w_relations(workplace_records, allowed):
            if edge.get("linkCode") != "HIE":
                continue
            end = str(edge.get("endValidDate") or "")
            if end and end[:10] < today:
                continue
            child = str(edge.get("childWorkplaceEid") or "")
            parent = str(edge.get("parentWorkplaceEid") or "")
            started = str(edge.get("beginValidDate") or "")
            if child and parent and (
                child not in latest_hie or started > latest_hie[child][1]
            ):
                latest_hie[child] = (parent, started)

        def parent_of(workplace_eid: str) -> str | None:
            manager = (workplaces.get(workplace_eid) or {}).get(
                "managerWorkplaceEid"
            )
            if manager and str(manager) in workplaces:
                return str(manager)
            hie = latest_hie.get(workplace_eid)
            if not hie:
                return None
            parent = hie[0]
            structure = str(
                (workplaces.get(parent) or {}).get("structureTypeCode") or ""
            ).upper()
            return parent if structure in {"GOR", "POR"} else None

        levels: dict[str, int] = {}

        def promote(workplace_eid: str | None, level: int) -> None:
            if workplace_eid:
                levels[workplace_eid] = max(level, levels.get(workplace_eid, 0))

        for individual_eid, record in main_activity_by_individual(
            activity_records
        ).items():
            activity = record.get("activity") or {}
            department = activity.get("workplaceEid")
            if individual_eid not in individuals or department not in workplaces:
                continue
            department = str(department)
            hospital = parent_of(department)
            group = parent_of(hospital) if hospital else None
            promote(group, 1)
            promote(hospital, 2)
            promote(department, 3)
        return levels

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
        sync_choices: bool = True,
    ) -> dict:
        """Run CDC delta with the same canonical phase order as full sync."""
        results = {
            "success": False,
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
            # Contact changes the window could not apply; see _sync_delta_codebase.
            "unjoined": {"activities": 0, "individuals": 0},
            "picklist_gaps": 0,
            "picklist_gap_report": None,
            "errors": [],
        }
        aborted = False
        self._begin_job(
            JOB_DELTA,
            codebases,
            dry_run,
            date_from=date_from,
            date_to=date_to,
        )
        try:
            self.transformer.picklist_gaps.clear()
            self.sync_logger.start_sync("delta", codebases, date_from, date_to)

            validation = self.validate_before_sync()
            if not validation.is_valid:
                logger.error(msg("Sync aborted, pre-sync validation failed", _ctx(self)))
                results["validation"] = validation.summary()
                results["errors"].append(validation.summary())
                self.sync_logger.end_sync(success=False, results=results)
                return results

            results["success"] = True

            if sync_picklists:
                self._trace_phase = PHASE_PICKLISTS
                try:
                    results["picklists"] = self.sync_picklists(
                        dry_run=dry_run, codebases=codebases, choices=sync_choices
                    )
                except Exception as e:
                    logger.exception(
                        msg("Picklist sync failed, continuing without option upsert", _ctx(self))
                    )
                    results["errors"].append(f"picklists: {e}")
                    results["picklists"] = []
                self._checkpoint(results)
            else:
                self.catalogs.dry_run = dry_run
                if not dry_run:
                    self.catalogs.preload(self.config.lookup_tables)

            for codebase in codebases:
                codebase_started = self.sync_logger.phase_started(
                    "codebase", codbase=codebase, date_from=date_from, date_to=date_to
                )
                codebase_failed = False
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
                    self._merge_delta_result(results, delta_result)
                    self._checkpoint(results)
                except SyncPaused as exc:
                    # The phases that finished before the pause still count.
                    if exc.delta_result is not None:
                        self._merge_delta_result(results, exc.delta_result)
                    self._record_pause(results, codebase, exc)
                except Exception as e:
                    logger.exception(msg(f"Codebase {codebase} failed", _ctx(self)))
                    results["errors"].append(f"{codebase}: {e}")
                    results["success"] = False
                    codebase_failed = True
                    self._checkpoint(results)
                self.sync_logger.phase_finished(
                    "codebase",
                    codebase_started,
                    outcome=_codebase_outcome(codebase_failed, results),
                    codbase=codebase,
                )
                if results.get("paused"):
                    break  # the deadline holds for every codebase

            gap_path = self.transformer.picklist_gaps.write_report()
            results["picklist_gaps"] = len(self.transformer.picklist_gaps.gaps)
            results["picklist_gap_report"] = str(gap_path) if gap_path else None

            self.sync_logger.end_sync(success=results["success"], results=results)
            return results
        except Exception as exc:
            aborted = True
            message = str(exc)
            if message and message not in results["errors"]:
                results["errors"].append(message)
            raise
        finally:
            self._finish_job(results, aborted=aborted)

    @staticmethod
    def _merge_entity(target: dict, source: dict) -> None:
        for key in _EMPTY_ENTITY:
            target[key] += source.get(key, 0)

    @staticmethod
    def _merge_hierarchy(target: dict, source: dict) -> None:
        for key in _EMPTY_HIERARCHY:
            target[key] += source.get(key, 0)

    def _merge_delta_result(self, results: dict, delta_result: dict) -> None:
        """Add one codebase's _sync_delta_codebase counts to the delta's results."""
        for key in ("accounts", "addresses", "affiliations", "activity_affiliations", "contacts"):
            self._merge_entity(results[key], delta_result.get(key) or _empty_entity_stats())
        self._merge_hierarchy(results["hierarchy"], delta_result.get("hierarchy") or {})
        for code in ["I", "U", "R"]:
            results["by_process_code"][code] += delta_result.get("by_process_code", {}).get(code, 0)
        for kind, count in (delta_result.get("unjoined") or {}).items():
            results["unjoined"][kind] = results["unjoined"].get(kind, 0) + count

    def _bulk_upsert(
        self, entity_set: str, key_name: str, payloads: list[dict], update_only: bool = False
    ) -> tuple[list[tuple], dict, set]:
        """bulk_upsert_by_field, keeping records Dataverse rejects on their own.

        Returns the results, ``{match_value: error}`` for the rejected
        records, whose result has no record id, and the match values left
        alone because Dataverse already held their values (skip_unchanged).
        One bad record becomes one failure row instead of stopping the
        codebase, so the job still covers its window. A fault shared by every
        record still raises.
        """
        rejected: dict = {}
        unchanged: set = set()
        results = self.dataverse.bulk_upsert_by_field(
            entity_set,
            key_name,
            payloads,
            batch_size=self.bulk_batch_size,
            record_errors=rejected,
            skip_unchanged=self.skip_unchanged,
            skipped=unchanged,
            **({"update_only": True} if update_only else {}),
        )
        self.records_written += sum(
            1 for match, record_id, _ in results if record_id and match not in unchanged
        )
        return results, rejected, unchanged

    def _stamp_nameless_hcp_names(self, flattened: list[dict]) -> None:
        """Attach one HCP name to workplaces that have no name of their own."""
        names = self._hcp_name_by_workplace
        if not names:
            return
        for row in flattened:
            if str(row.get("officialName") or "").strip() or str(row.get("usualName") or "").strip():
                continue
            picked = names.get(str(row.get("workplaceEid") or ""))
            if picked:
                row["hcpName"] = picked

    def _sync_hcos(
        self,
        codebase: str,
        max_records: int = None,
        dry_run: bool = False,
        flattened: list[dict] | None = None,
        raw_workplaces: list[dict] | None = None,
        hierarchy_levels: dict[str, int] | None = None,
    ) -> tuple[dict, list[dict], list[dict]]:
        with self._phase(PHASE_HCO, "accounts", codbase=codebase) as phase:
            result = self._sync_hcos_impl(
                codebase,
                max_records=max_records,
                dry_run=dry_run,
                flattened=flattened,
                raw_workplaces=raw_workplaces,
                hierarchy_levels=hierarchy_levels,
            )
            phase["stats"] = result[0]
            return result

    def _sync_hcos_impl(
        self,
        codebase: str,
        max_records: int = None,
        dry_run: bool = False,
        flattened: list[dict] | None = None,
        raw_workplaces: list[dict] | None = None,
        hierarchy_levels: dict[str, int] | None = None,
    ) -> tuple[dict, list[dict], list[dict]]:
        """Sync HCOs for a single codebase using bulk upserts.

        Parent ``parentaccountid`` is always deferred to ``_sync_hierarchy``.
        The workplaces are ``flattened`` rows, else ``raw_workplaces`` already
        read from the population, else read here from refreshMyOnekey.

        Returns:
            stats, flattened HCO rows, raw OneKey WORKPLACE records (for W2W;
            empty when ``flattened`` was given).
        """

        result = _empty_entity_stats()

        if flattened is not None:
            raw_workplaces = []
        else:
            if raw_workplaces is None:
                raw_workplaces = []
                individuals = []
                activities = []
                started = self.sync_logger.phase_started("extract", codbase=codebase)
                for record in self.onekey.iter_population(
                    cod_base=codebase, max_records=max_records
                ):
                    entity_type = (record.get("entityType") or "").upper()
                    if entity_type == "WORKPLACE":
                        raw_workplaces.append(record)
                    elif entity_type == "INDIVIDUAL":
                        individuals.append(record)
                    elif entity_type == "ACTIVITY":
                        activities.append(record)
                if not self._hcp_name_by_workplace:
                    self._hcp_name_by_workplace = hcp_name_by_workplace(
                        individuals, activities
                    )
                self.sync_logger.phase_finished(
                    "extract", started, {"workplaces": len(raw_workplaces)}, codbase=codebase
                )
            flattened = [flatten_hco(r) for r in raw_workplaces]

        self._stamp_nameless_hcp_names(flattened)

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
                msg(
                    "Contracted allowlist",
                    _ctx(self, codbase=codebase),
                    workplaces=len(contracted),
                )
            )

        if not flattened:
            return result, flattened, raw_workplaces

        logger.info(
            msg(
                "Upserting accounts",
                _ctx(self),
                records=len(flattened),
                batch_size=self.bulk_batch_size,
            )
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
                self._fail(
                    "account",
                    workplace_eid,
                    "validate",
                    "; ".join(str(err) for err in record_validation.errors)
                    or "HCO validation failed",
                    category="validation",
                    retryable=False,
                    log=False,
                )
                continue

            payload = self.transformer.transform_hco(hco)
            if workplace_eid in (hierarchy_levels or {}):
                payload["ts_hierarchylevel"] = hierarchy_levels[workplace_eid]
            key_name = self.config.require_key_field("account")
            key_value = payload.get(key_name)
            if not key_value:
                result["failed"] += 1
                done += 1
                self._fail(
                    "account",
                    workplace_eid,
                    "upsert",
                    f"Missing key field {key_name}",
                    category="mapping",
                    retryable=False,
                )
                continue

            prepared.append((workplace_eid, key_name, key_value, payload, hco))

        if dry_run:
            for workplace_eid, _, _, payload, _ in prepared:
                logger.debug(
                    "Dry run account %s: fields=%s | job=%s",
                    workplace_eid, ",".join(sorted(payload)), short_job(self.tracer.job_id),
                )
                result["processed"] += 1
            self.sync_logger.log_progress("accounts", len(prepared), total)
            return result, flattened, raw_workplaces

        if not prepared:
            return result, flattened, raw_workplaces

        if self._partial_population:
            self._keep_existing_nameless_names(prepared)

        key_name = prepared[0][1]

        # One bulk call per batch, so a run can stop between batches when
        # its time budget is spent (see _check_deadline).
        for batch in chunked(prepared, self.bulk_batch_size):
            self._check_deadline("accounts", "accounts", result)
            payloads = [item[3] for item in batch]
            try:
                bulk_results, rejected, unchanged = self._bulk_upsert(
                    "accounts", key_name, payloads
                )
            except Exception as e:
                result["failed"] += len(batch)
                for workplace_eid, _, key_value, _, _ in batch:
                    self._fail(
                        "account",
                        workplace_eid,
                        "upsert",
                        str(e),
                        external_id=key_value,
                        exc=e,
                    )
                raise

            for (workplace_eid, _, key_value, payload, hco), (match, account_id, created) in zip(
                batch, bulk_results
            ):
                if not account_id:
                    error = rejected.get(match)
                    result["failed"] += 1
                    done += 1
                    self._fail(
                        "account",
                        workplace_eid,
                        "upsert",
                        str(error or "Dataverse rejected the record"),
                        external_id=key_value,
                        exc=error,
                    )
                    continue
                self._account_cache[workplace_eid] = account_id
                if "ts_hierarchylevel" in payload:
                    self._account_hierarchy_level_cache[
                        str(account_id).lower()
                    ] = payload["ts_hierarchylevel"]
                if created:
                    result["created"] += 1
                elif match in unchanged:
                    result["skipped"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                done += 1
                self._note_success(workplace_eid, entity="account", external_id=key_value)
                self.sync_logger.log_record_success(
                    "account",
                    workplace_eid,
                    account_id,
                    _operation(created, match in unchanged),
                )
                if done % 100 == 0 or done == len(prepared):
                    self.sync_logger.log_progress("accounts", done, len(prepared))

        return result, flattened, raw_workplaces

    def _sync_addresses(
        self,
        hcos: list[dict],
        dry_run: bool = False,
    ) -> dict:
        with self._phase(PHASE_ADDRESSES, "addresses") as phase:
            phase["stats"] = self._sync_addresses_impl(hcos, dry_run=dry_run)
            return phase["stats"]

    def _sync_addresses_impl(
        self,
        hcos: list[dict],
        dry_run: bool = False,
    ) -> dict:
        """Phase 2: upsert customeraddress rows for flattened HCOs."""
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

        address_key = self.config.require_key_field("customeraddress")
        # (key, payload, workplace_eid); each workplace's in address-number order.
        prepared = []
        for hco in hcos:
            workplace_eid = hco.get("workplaceEid")
            sources = [s for s in address_sources(hco) if address_id(s)]
            if not workplace_eid or not sources:
                continue
            account_id = self._account_cache.get(workplace_eid)
            if not account_id and not dry_run:
                result["failed"] += 1
                self._fail(
                    "customeraddress",
                    workplace_eid,
                    "relate",
                    "Account not found for address parentid",
                    category="lookup",
                    retryable=False,
                )
                continue
            keys: set[str] = set()
            for source in sources:
                payload = self.transformer.transform_address(
                    source, parent_account_id=account_id
                )
                if not payload:
                    continue
                key = payload.get(address_key)
                if not key:
                    result["failed"] += 1
                    self._fail(
                        "customeraddress",
                        workplace_eid,
                        "upsert",
                        "Missing customeraddress key",
                        external_id=address_id(source),
                        category="mapping",
                        retryable=False,
                    )
                    continue
                if key in keys:
                    continue
                keys.add(key)
                prepared.append((key, payload, workplace_eid))

        logger.info(msg("Upserting addresses", _ctx(self), records=len(prepared)))
        if dry_run:
            result["processed"] = len(prepared)
            return result
        if not prepared:
            return result

        for batch in workplace_batches(prepared, self.bulk_batch_size):
            self._check_deadline("addresses", "addresses", result)
            self._place_addresses(batch, address_key)
            try:
                bulk_results, rejected, unchanged = self._bulk_upsert(
                    "customeraddresses", address_key, [p for _, p, _ in batch]
                )
            except Exception as e:
                result["failed"] += len(batch)
                for key, _, workplace_eid in batch:
                    self._fail(
                        "customeraddress",
                        workplace_eid,
                        "upsert",
                        str(e),
                        external_id=key,
                        exc=e,
                    )
                raise

            for (key, _, workplace_eid), (match, record_id, created) in zip(
                batch, bulk_results
            ):
                if not record_id:
                    error = rejected.get(match)
                    result["failed"] += 1
                    self._fail(
                        "customeraddress",
                        workplace_eid,
                        "upsert",
                        str(error or "Dataverse rejected the record"),
                        external_id=key,
                        exc=error,
                    )
                    continue
                if created:
                    result["created"] += 1
                elif match in unchanged:
                    result["skipped"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                self._note_success(workplace_eid, key, entity="customeraddress", external_id=key)
                self.sync_logger.log_record_success(
                    "customeraddress",
                    key,
                    record_id,
                    _operation(created, match in unchanged),
                )

        return result

    def _place_addresses(self, batch: list[tuple], key_field: str) -> None:
        """Number each account's OneKey addresses 1, 2, 3, ... in OneKey order.

        ``batch`` holds ``(key, payload, workplace_eid)``, each workplace's
        addresses together and in order: main, mailing, then secondary
        (onekey.parse.workplace_addresses). For the accounts it covers, this
        reads their address rows, plans which row each address goes to
        (integration.address_rows.plan_address_rows), runs the plan's deletes,
        key clears and key claims through $batch, and sets ``addressnumber``
        on the payloads, in place, for the upsert that follows.

        A step that fails leaves the account as it is and its addresses are
        written as before, without addressnumber: an environment may refuse
        to delete address rows, for one. The run goes on either way.
        """
        dv = self.dataverse
        groups: dict[str, list[tuple[str, dict]]] = {}
        for key, payload, eid in batch:
            payload.pop("addressnumber", None)
            groups.setdefault(eid, []).append((key, payload))
        if not hasattr(dv, "retrieve_multiple"):
            return
        accounts = {eid: str(self._account_cache.get(eid) or "").lower() for eid in groups}
        rows_by_account: dict[str, list[dict]] = {}
        try:
            # 100 parent filters stay under the client's 6,000-character filter cap.
            for chunk in chunked(sorted(set(accounts.values()) - {""}), 100):
                rows = dv.retrieve_multiple(
                    "customeraddresses",
                    select=["customeraddressid", "_parentid_value", "addressnumber", key_field],
                    filter=" or ".join(f"_parentid_value eq {account}" for account in chunk),
                )
                for row in rows or []:
                    parent = str(row.get("_parentid_value") or "").lower()
                    rows_by_account.setdefault(parent, []).append(
                        {
                            "id": row["customeraddressid"],
                            "number": row.get("addressnumber"),
                            "key": row.get(key_field),
                        }
                    )
        except Exception as exc:  # noqa: BLE001 — the addresses are still written
            logger.warning(
                msg(
                    "Could not read address rows; address numbers left as they are",
                    _ctx(self),
                    error=str(exc)[:300],
                )
            )
            return

        plans = {
            eid: plan_address_rows(
                rows_by_account.get(accounts[eid], []),
                [key for key, _ in items],
                f"{eid} - ",
            )
            for eid, items in groups.items()
        }
        if any(plan.writes for plan in plans.values()):
            if hasattr(dv, "batch_write"):
                self._run_address_plans(plans, key_field)
            else:
                plans = {eid: plan for eid, plan in plans.items() if not plan.writes}
        for eid, items in groups.items():
            plan = plans.get(eid)
            if plan is None:
                continue
            for key, payload in items:
                if key in plan.numbers:
                    payload["addressnumber"] = plan.numbers[key]

    def _run_address_plans(self, plans: dict, key_field: str) -> None:
        """Run the deletes and blanks, clears and claims of the address plans, in that order.

        A blank empties the key and every mapped address column, as Dataverse
        leaves rows 1 and 2 of a new account. A row Dataverse will not delete
        (an environment with "Enable deletion of address records" off) is
        blanked instead, so the plan goes on and the row stays, empty. A plan
        whose step still fails is dropped from ``plans``, so its account is
        written as before, except when the row's address left OneKey: that
        row keeps it and the rest of the plan goes on. A failed claim drops
        only that address's number.
        """
        dv = self.dataverse
        problems: list[str] = []
        blank = {
            fm.target_field: None
            for fm in self.config.get_field_mappings("customeraddress")
            if fm.target_field and not fm.is_lookup
        }
        blank[key_field] = None

        def run(steps, method, body, report=True):
            """Send ``(eid, row_id)`` steps through $batch; return the failed ones."""
            outcomes = []
            for chunk in chunked(steps, 500):
                outcomes.extend(
                    dv.batch_write(
                        [(method, f"customeraddresses({row_id})", body(eid, row_id)) for eid, row_id in chunk],
                        idempotent=True,
                    )
                )
            failed = []
            for (eid, row_id), (status, _, error) in zip(steps, outcomes):
                # 404 on a delete: a stopped run deleted it already.
                if status is not None and (status < 300 or (method == "DELETE" and status == 404)):
                    continue
                failed.append((eid, row_id))
                if report:
                    problems.append(f"{method} {plans[eid].holding.get(row_id) or row_id}: {error or status}")
            return failed

        deletes = [(eid, row_id) for eid, plan in plans.items() for row_id in plan.deletes]
        blanks = [(eid, row_id) for eid, plan in plans.items() for row_id in plan.blanks]
        undeleted = run(deletes, "DELETE", lambda eid, row_id: None, report=False) if deletes else []
        if undeleted:
            logger.info(
                msg(
                    "Address rows Dataverse would not delete are emptied instead",
                    _ctx(self),
                    count=len(undeleted),
                )
            )
        refused = run(undeleted + blanks, "PATCH", lambda eid, row_id: blank) if undeleted or blanks else []
        for eid, row_id in refused:
            plan = plans.get(eid)
            if plan is not None and plan.holding.get(row_id) in plan.numbers:
                plans.pop(eid)

        clears = [(eid, row_id) for eid, plan in plans.items() for row_id in plan.clears]
        for eid, _ in run(clears, "PATCH", lambda eid, row_id: {key_field: None}) if clears else []:
            plans.pop(eid, None)

        claims = [(eid, row_id) for eid, plan in plans.items() for row_id in plan.claims]
        for eid, row_id in run(
            claims, "PATCH", lambda eid, row_id: {key_field: plans[eid].claims[row_id]}
        ) if claims else []:
            plans[eid].numbers.pop(plans[eid].claims[row_id], None)

        if problems:
            logger.warning(
                msg(
                    "Address rows not renumbered, written as before",
                    _ctx(self),
                    count=len(problems),
                    first=problems[0][:300],
                )
            )

    def _sync_hierarchy(
        self,
        hcos: list[dict],
        dry_run: bool = False,
        skip_unchanged: bool = True,
        allowed_workplace_eids: set[str] | None = None,
    ) -> dict:
        with self._phase(PHASE_HIERARCHY, "hierarchy") as phase:
            phase["stats"] = self._sync_hierarchy_impl(
                hcos,
                dry_run=dry_run,
                skip_unchanged=skip_unchanged,
                allowed_workplace_eids=allowed_workplace_eids,
            )
            return phase["stats"]

    def _sync_hierarchy_impl(
        self,
        hcos: list[dict],
        dry_run: bool = False,
        skip_unchanged: bool = True,
        allowed_workplace_eids: set[str] | None = None,
    ) -> dict:
        """Phase 3: set parentaccountid from managerWorkplaceEid edges."""
        result = _empty_hierarchy_stats()
        allow = (
            allowed_workplace_eids
            if allowed_workplace_eids is not None
            else self._contracted_workplace_eids or None
        )
        if allow is not None and self._partial_population and not dry_run:
            # A parent that did not change in this window is still contracted.
            allow = set(allow) | self._admit_known(
                workplace_eids={hco.get("parentWorkplaceEid") for hco in hcos}
            )
        edges = edges_from_hcos(hcos, allowed_workplace_eids=allow)
        result["edges"] = len(edges)
        if not dry_run:
            result["unlinked"], unlink_failed = self._clear_removed_parents(hcos, allow)
            result["failed"] += unlink_failed
        if not edges:
            logger.info(
                msg("No parent edges to link", _ctx(self), contracted_filter=allow is not None)
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
                msg("Dry run, would link parents", _ctx(self), ready=ready, edges=len(edges))
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
        for child_eid in stats.get("missing_child_eids") or []:
            self._fail(
                "account",
                child_eid,
                "relate",
                "Child account not found for hierarchy link",
                category="lookup",
                retryable=False,
            )
        for child_eid in stats.get("missing_parent_eids") or []:
            self._fail(
                "account",
                child_eid,
                "relate",
                "Parent account not found for hierarchy link",
                category="lookup",
                retryable=False,
            )

        logger.info(
            msg(
                "Hierarchy plan",
                _ctx(self),
                ready=stats["ready"],
                already_linked=stats["already_linked"],
                missing_child=stats["missing_child"],
                missing_parent=stats["missing_parent"],
            )
        )

        for child_eid in stats.get("already_linked_eids") or []:
            self._note_success(child_eid, entity="account")

        if not updates:
            return result

        link_failures: list[tuple[str, BaseException]] = []
        counted = {"linked": 0}

        def before_batch(linked_so_far: int) -> None:
            self.records_written += linked_so_far - counted["linked"]
            counted["linked"] = linked_so_far
            self._check_deadline("hierarchy")

        linked, failed = apply_updates(
            self.dataverse,
            updates,
            self.bulk_batch_size,
            match_field=account_key,
            failures=link_failures,
            before_batch=before_batch,
        )
        self.records_written += linked - counted["linked"]
        for child_eid, exc in link_failures:
            self._fail(
                "account",
                child_eid,
                "update",
                str(exc),
                exc=exc,
            )
        failed_eids = {child_eid for child_eid, _ in link_failures}
        for row in updates:
            child_eid = str(row[account_key])
            if child_eid not in failed_eids:
                self._note_success(child_eid, entity="account")
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
        with self._phase(PHASE_HCO_RELATIONS, "hco_relations") as phase:
            phase["stats"] = self._sync_hco_relations_impl(
                raw_workplaces,
                dry_run=dry_run,
                allowed_workplace_eids=allowed_workplace_eids,
            )
            return phase["stats"]

    def _sync_hco_relations_impl(
        self,
        raw_workplaces: list[dict],
        dry_run: bool = False,
        allowed_workplace_eids: set[str] | None = None,
    ) -> dict:
        """Phase 3b: upsert deduped HCO–HCO links into ``ts_affiliation``."""
        result = _empty_entity_stats()

        allow = (
            allowed_workplace_eids
            if allowed_workplace_eids is not None
            else self._contracted_workplace_eids or None
        )
        if allow is not None and self._partial_population and not dry_run:
            # The related workplace need not have changed in this window.
            related = explode_w2w_relations(raw_workplaces)
            allow = set(allow) | self._admit_known(
                workplace_eids={
                    eid
                    for edge in related
                    for eid in (edge["childWorkplaceEid"], edge["parentWorkplaceEid"])
                }
            )
        edges = collect_w2w_relations(
            raw_workplaces, allowed_workplace_eids=allow
        )
        if not dry_run:
            result["updated"] += self._deactivate_removed_relations(raw_workplaces)
        if not edges:
            logger.info(
                msg("No HCO relations to sync", _ctx(self), contracted_filter=allow is not None)
            )
            return result

        logger.info(
            msg(
                "Upserting HCO relations",
                _ctx(self),
                records=len(edges),
                workplaces=len(raw_workplaces),
                allowlist=len(allow) if allow is not None else "off",
            )
        )

        affiliation_key = self.config.require_key_field("ts_affiliation")
        entity_set = self._affiliation_entity_set()

        eids = sorted(
            {
                eid
                for edge in edges
                for eid in (
                    edge.get("fromWorkplaceEid"),
                    edge.get("toWorkplaceEid"),
                )
                if eid
            }
        )
        if not dry_run and eids:
            self._lookup_accounts_bulk(eids)

        prepared: list[tuple[str, str, dict, dict]] = []

        for edge in edges:
            external_id = edge.get("ts_externalid")
            from_eid = edge.get("fromWorkplaceEid")
            to_eid = edge.get("toWorkplaceEid")

            from_id = self._account_cache.get(from_eid)
            to_id = self._account_cache.get(to_eid)

            if not from_id and not dry_run:
                result["failed"] += 1
                self.sync_logger.log_relationship_failed(
                    "ts_affiliation",
                    external_id,
                    "account",
                    from_eid,
                    f"From account not found for {from_eid}",
                )
                self._fail(
                    "ts_affiliation",
                    from_eid,
                    "relate",
                    f"From account not found for {from_eid}",
                    external_id=external_id,
                    category="lookup",
                    retryable=False,
                    log=False,
                )
                continue
            if not to_id and not dry_run:
                result["failed"] += 1
                self.sync_logger.log_relationship_failed(
                    "ts_affiliation",
                    external_id,
                    "account",
                    to_eid,
                    f"To account not found for {to_eid}",
                )
                self._fail(
                    "ts_affiliation",
                    to_eid,
                    "relate",
                    f"To account not found for {to_eid}",
                    external_id=external_id,
                    category="lookup",
                    retryable=False,
                    log=False,
                )
                continue

            payload = self.transformer.transform_w2w_affiliation(
                edge,
                from_account_id=from_id,
                to_account_id=to_id,
            )
            if not payload.get(affiliation_key):
                result["failed"] += 1
                self._fail(
                    "ts_affiliation",
                    from_eid,
                    "validate",
                    f"Missing key field {affiliation_key}",
                    external_id=external_id,
                    category="mapping",
                    retryable=False,
                )
                continue

            prepared.append((external_id, affiliation_key, payload, edge))

        if dry_run:
            result["processed"] = len(prepared)
            logger.info(
                msg("Dry run, would upsert HCO relations", _ctx(self), records=len(prepared))
            )
            return result

        if not prepared:
            return result

        done = 0
        for batch in chunked(prepared, self.bulk_batch_size):
            self._check_deadline("HCO relations", "affiliations", result)
            payloads = [item[2] for item in batch]
            try:
                bulk_results, rejected, unchanged = self._bulk_upsert(
                    entity_set, affiliation_key, payloads
                )
            except Exception as e:
                result["failed"] += len(batch)
                for external_id, _, _, edge in batch:
                    self._fail(
                        "ts_affiliation",
                        edge.get("fromWorkplaceEid") or external_id,
                        "upsert",
                        str(e),
                        external_id=external_id,
                        exc=e,
                    )
                raise

            for (external_id, _, _payload, edge), (match, record_id, created) in zip(
                batch, bulk_results
            ):
                if not record_id:
                    error = rejected.get(match)
                    result["failed"] += 1
                    self._fail(
                        "ts_affiliation",
                        edge.get("fromWorkplaceEid") or external_id,
                        "upsert",
                        str(error or "Dataverse rejected the record"),
                        external_id=external_id,
                        exc=error,
                    )
                    continue
                if created:
                    result["created"] += 1
                elif match in unchanged:
                    result["skipped"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                done += 1
                self._note_success(
                    external_id,
                    edge.get("fromWorkplaceEid"),
                    edge.get("toWorkplaceEid"),
                    entity="ts_affiliation",
                    external_id=external_id,
                )
                self.sync_logger.log_record_success(
                    "ts_affiliation",
                    external_id,
                    record_id,
                    _operation(created, match in unchanged),
                )

            self.sync_logger.log_progress("affiliations", done, len(prepared))

        return result

    def _sync_activity_affiliations(
        self,
        activity_records: list[dict],
        dry_run: bool = False,
    ) -> dict:
        with self._phase(PHASE_HCP_ACTIVITY, "activity_affiliations") as phase:
            phase["stats"] = self._sync_activity_affiliations_impl(
                activity_records, dry_run=dry_run
            )
            return phase["stats"]

    def _sync_activity_affiliations_impl(
        self,
        activity_records: list[dict],
        dry_run: bool = False,
    ) -> dict:
        """Upsert HCP–HCO Activity rows into ``ts_affiliation`` (AFF-*)."""
        result = _empty_entity_stats()

        if not activity_records:
            logger.info(msg("No activity affiliations to upsert", _ctx(self)))
            return result

        logger.info(
            msg("Upserting activity affiliations", _ctx(self), records=len(activity_records))
        )

        affiliation_key = self.config.require_key_field("ts_affiliation")
        entity_set = self._affiliation_entity_set()

        flat_rows: list[dict] = []
        flats = [flatten_activity_for_affiliation(record) for record in activity_records]
        if self._partial_population and not dry_run:
            # The activity's workplace or individual need not have changed in
            # this window to be contracted. The check applies even when the
            # window holds no workplace: an empty set is not "no filter" here.
            self._admit_known(
                workplace_eids={flat.get("workplaceEid") for flat in flats},
                individual_eids={flat.get("individualEid") for flat in flats},
            )
            allow_wp = self._contracted_workplace_eids
            allow_ind = self._contracted_individual_eids
        else:
            allow_wp = self._contracted_workplace_eids or None
            allow_ind = self._contracted_individual_eids or None
        skipped_out_of_contract = 0
        for flat in flats:
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
                msg(
                    "Activity affiliations skipped outside contracted population",
                    _ctx(self),
                    records=skipped_out_of_contract,
                )
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

            if not contact_id and not dry_run:
                result["failed"] += 1
                self.sync_logger.log_relationship_failed(
                    "ts_affiliation",
                    external_id,
                    "contact",
                    flat["individualEid"],
                    f"Contact not found for {flat['individualEid']}",
                )
                self._fail(
                    "ts_affiliation",
                    external_id,
                    "relate",
                    f"Contact not found for {flat['individualEid']}",
                    external_id=external_id,
                    category="lookup",
                    retryable=False,
                    log=False,
                )
                continue
            if not account_id and not dry_run:
                result["failed"] += 1
                self.sync_logger.log_relationship_failed(
                    "ts_affiliation",
                    external_id,
                    "account",
                    flat["workplaceEid"],
                    f"Account not found for {flat['workplaceEid']}",
                )
                self._fail(
                    "ts_affiliation",
                    external_id,
                    "relate",
                    f"Account not found for {flat['workplaceEid']}",
                    external_id=external_id,
                    category="lookup",
                    retryable=False,
                    log=False,
                )
                continue

            payload = self.transformer.transform_activity_affiliation(
                flat,
                contact_id=contact_id,
                account_id=account_id,
            )
            if not payload.get(affiliation_key):
                result["failed"] += 1
                self._fail(
                    "ts_affiliation",
                    external_id,
                    "validate",
                    f"Missing key field {affiliation_key}",
                    category="mapping",
                    retryable=False,
                )
                continue

            prepared.append((external_id, affiliation_key, payload, flat))

        if dry_run:
            result["processed"] = len(prepared)
            role_count = sum(
                len(self.transformer.affiliation_role_rows(flat, "dry-run", entity_set))
                for _, _, _, flat in prepared
            )
            logger.info(
                msg(
                    "Dry run, would upsert activity affiliations",
                    _ctx(self),
                    records=len(prepared),
                    roles=role_count,
                )
            )
            return result

        if not prepared:
            return result

        role_rows: list[dict] = []
        done = 0
        for batch in chunked(prepared, self.bulk_batch_size):
            self._check_deadline("activity affiliations", "activity_affiliations", result)
            payloads = [item[2] for item in batch]
            try:
                bulk_results, rejected, unchanged = self._bulk_upsert(
                    entity_set, affiliation_key, payloads
                )
            except Exception as e:
                result["failed"] += len(batch)
                for external_id, *_ in batch:
                    self._fail(
                        "ts_affiliation",
                        external_id,
                        "upsert",
                        str(e),
                        external_id=external_id,
                        exc=e,
                    )
                raise

            for (external_id, _, _payload, flat), (match, record_id, created) in zip(
                batch, bulk_results
            ):
                if not record_id:
                    error = rejected.get(match)
                    result["failed"] += 1
                    self._fail(
                        "ts_affiliation",
                        external_id,
                        "upsert",
                        str(error or "Dataverse rejected the record"),
                        external_id=external_id,
                        exc=error,
                    )
                    continue
                if created:
                    result["created"] += 1
                elif match in unchanged:
                    result["skipped"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                done += 1
                # Only the activity: its failure rows use the activity EID. The
                # individual and workplace EIDs would resolve the contact and
                # account rows, which this write did not touch.
                self._note_success(external_id, entity="ts_affiliation", external_id=external_id)
                self.sync_logger.log_record_success(
                    "ts_affiliation",
                    external_id,
                    record_id,
                    _operation(created, match in unchanged),
                )
                role_rows.extend(
                    self.transformer.affiliation_role_rows(flat, record_id, entity_set)
                )

            self.sync_logger.log_progress("activity_affiliations", done, len(prepared))

        roles = self._upsert_affiliation_roles(role_rows, result)
        logger.info(
            msg(
                "Affiliation roles written",
                _ctx(self),
                processed=roles["processed"],
                created=roles["created"],
                updated=roles["updated"],
                failed=roles["failed"],
            )
        )
        return result

    def _upsert_affiliation_roles(
        self, rows: list[dict], affiliations: dict | None = None
    ) -> dict:
        """Upsert TIH role rows onto the affiliations just written.

        The child table has no alternate key. ``ts_name``
        (``{activityEid} - {role code}``) is the match column. The time
        budget is checked before each batch, as in the affiliation loop;
        ``affiliations`` is that phase's counts, which a pause keeps. A
        continuation finds the affiliations unchanged and writes the roles
        not written yet.
        """
        result = _empty_entity_stats()
        if not rows:
            return result
        entity_set = self._role_entity_set()
        done = 0
        for batch in chunked(rows, self.bulk_batch_size):
            self._check_deadline("affiliation roles", "activity_affiliations", affiliations)
            try:
                bulk_results, rejected, unchanged = self._bulk_upsert(
                    entity_set, "ts_name", batch
                )
            except Exception as exc:
                result["failed"] += len(batch)
                self._fail(
                    "ts_affiliationrole",
                    "",
                    "upsert",
                    str(exc),
                    exc=exc,
                )
                raise
            for match, record_id, created in bulk_results:
                if not record_id:
                    result["failed"] += 1
                    self._fail(
                        "ts_affiliationrole",
                        match,
                        "upsert",
                        str(rejected.get(match) or "Dataverse rejected the role row"),
                        external_id=match,
                        exc=rejected.get(match),
                    )
                    continue
                if created:
                    result["created"] += 1
                elif match in unchanged:
                    result["skipped"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                done += 1
                self.sync_logger.log_record_success(
                    "ts_affiliationrole",
                    match,
                    record_id,
                    _operation(created, match in unchanged),
                )
            self.sync_logger.log_progress("affiliation_roles", done, len(rows))
        return result

    def _role_entity_set(self) -> str:
        ed = self.config.get_entity("Dataverse", "ts_affiliationrole")
        return ed.entity_set if ed and ed.entity_set else "ts_affiliationroles"

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
        started = self.sync_logger.phase_started("extract", codbase=codebase, entity="hcp")
        workplaces, individuals, activities = self._split_population(
            self.onekey.iter_population(
                cod_base=codebase,
                max_records=max_records,
                date_from=date_from,
                date_to=date_to,
            )
        )
        composed, stats = self._compose_population_hcps(workplaces, individuals, activities)
        self.sync_logger.phase_finished(
            "extract", started, stats, codbase=codebase, entity="hcp"
        )
        return composed, activities

    def _compose_population_hcps(
        self,
        workplace_records: list[dict],
        individual_records: list[dict],
        activity_records: list[dict],
    ) -> tuple[list[dict], dict]:
        """Join population packets into HCP contact rows; return them and the counts."""
        workplaces: dict[str, dict] = {}
        for record in workplace_records:
            workplace_eid = (record.get("workplace") or {}).get("workplaceEid")
            if workplace_eid:
                workplaces[workplace_eid] = record
        individuals: dict[str, dict] = {}
        for record in individual_records:
            individual_eid = (record.get("individual") or {}).get("individualEid")
            if individual_eid:
                individuals[individual_eid] = record
        activities_by_individual = main_activity_by_individual(activity_records)

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
                    "Activity skipped, workplace outside contracted population: "
                    "individual=%s workplace=%s | job=%s",
                    individual_eid, workplace_eid, short_job(self.tracer.job_id),
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

        self._contracted_workplace_eids |= set(workplaces)
        self._contracted_individual_eids |= set(individuals)

        stats = {
            "hcps": len(composed),
            "individuals": len(individuals),
            "main_activities": len(activities_by_individual),
            "activity_packets": len(activity_records),
            "workplaces": len(workplaces),
            "skipped_out_of_contract": skipped_unjoined,
        }
        return composed, stats

    def _compose_delta_hcps(
        self,
        individuals: dict[str, dict],
        activities_by_individual: dict[str, dict],
        workplace_index: dict[str, dict],
        *,
        dry_run: bool = False,
    ) -> tuple[list[dict], int, set[str]]:
        """Join one delta's packets into contact rows for every changed individual.

        A delta returns only what changed: an individual whose name changed
        comes without its activity's workplace, often without the activity
        too. Each one is still written with what arrived:

        - Its parent comes from its main activity, when that activity is in
          the window and its workplace is in the window or an account;
          otherwise the parent stays as it is. Another activity may be the
          main one, so a secondary activity never moves the parent.
        - The transform leaves out fields without a value, so a workplace
          outside the window leaves the department as it is.
        - A new individual is created only as the full sync would: through an
          activity at a workplace in the window or already an account.

        Returns the rows, the number of new individuals left out, and the
        individuals written without their parent or department, whose
        pending failures stay open (_sync_hcps). A dry run reads nothing from
        Dataverse, so it treats every individual as an existing contact.
        """
        if not dry_run:
            self._lookup_contacts_bulk(list(individuals))
            # Only a main activity links a parent, and a new individual's
            # activity also decides whether it is in the contract.
            wanted = set()
            for individual_eid in individuals:
                activity_record = activities_by_individual.get(individual_eid) or {}
                activity = activity_record.get("activity") or {}
                workplace_eid = activity.get("workplaceEid")
                if (
                    workplace_eid
                    and workplace_eid not in workplace_index
                    and (activity.get("isMainActivity") or individual_eid not in self._contact_cache)
                ):
                    wanted.add(workplace_eid)
            self._lookup_accounts_bulk(wanted)

        composed = []
        left_out = 0
        partial: set[str] = set()
        for individual_eid, individual_record in individuals.items():
            activity_record = activities_by_individual.get(individual_eid)
            activity = (activity_record or {}).get("activity") or {}
            workplace_eid = activity.get("workplaceEid")
            known_workplace = bool(workplace_eid) and (
                workplace_eid in workplace_index or workplace_eid in self._account_cache
            )
            if not (dry_run or individual_eid in self._contact_cache or known_workplace):
                left_out += 1
                continue
            if not (known_workplace and activity.get("isMainActivity")):
                activity_record = None
            workplace_record = workplace_index.get(workplace_eid) if activity_record else None
            if workplace_record is None:
                partial.add(individual_eid)
            composed.append(
                compose_hcp_result(individual_record, activity_record, workplace_record)
            )
        return composed, left_out, partial

    def _sync_hcps(
        self,
        codebase: str,
        max_records: int = None,
        dry_run: bool = False,
        date_from: str = None,
        date_to: str = None,
        composed: list[dict] | None = None,
        partial: set[str] | None = None,
        update_hierarchy_levels: bool = True,
    ) -> dict:
        with self._phase(PHASE_HCP, "contacts", codbase=codebase) as phase:
            phase["stats"] = self._sync_hcps_impl(
                codebase,
                max_records=max_records,
                dry_run=dry_run,
                date_from=date_from,
                date_to=date_to,
                composed=composed,
                partial=partial,
                update_hierarchy_levels=update_hierarchy_levels,
            )
            return phase["stats"]

    def _sync_hcps_impl(
        self,
        codebase: str,
        max_records: int = None,
        dry_run: bool = False,
        date_from: str = None,
        date_to: str = None,
        composed: list[dict] | None = None,
        partial: set[str] | None = None,
        update_hierarchy_levels: bool = True,
    ) -> dict:
        """Sync HCPs for a single codebase using bulk upserts.

        ``partial`` holds individuals whose row leaves out the parent or the
        department (a delta, see _compose_delta_hcps). Writing them does not
        resolve their pending failures: what failed may be the part left out.
        """

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

        # A contact's department is its workplace; its hospital is that
        # workplace's parent.
        workplace_eids = {
            eid
            for hcp in flattened
            for eid in (hcp.get("workplaceEid"), hcp.get("hospitalWorkplaceEid"))
            if eid and eid not in self._account_cache
        }
        if workplace_eids and not dry_run:
            self._lookup_accounts_bulk(workplace_eids)
        # A delta row without its workplace (the workplace did not change)
        # takes the hospital from that workplace's parent account, which the
        # account and hierarchy phases have already written.
        unloaded = {
            hcp.get("workplaceEid")
            for hcp in flattened
            if not hcp.get("workplaceLoaded") and hcp.get("workplaceEid") in self._account_cache
        }
        parent_accounts = self._account_parents(unloaded) if unloaded and not dry_run else {}
        hospitals = {
            hcp.get("individualEid"): self._hospital_of(hcp, parent_accounts) for hcp in flattened
        }
        mapped = {fm.target_field for fm in self.config.get_field_mappings("contact")}
        if not dry_run and "ts_hospital" in mapped:
            # A workplace without a parent account (managerWorkplaceEid) takes
            # its HIE parent as the hospital, when that parent is a group.
            orphans = {
                hcp.get("individualEid"): str(self._account_cache.get(hcp.get("workplaceEid") or "") or "").lower()
                for hcp in flattened
                if hospitals.get(hcp.get("individualEid")) == (True, None)
            }
            hie_groups = self._hie_group_parents(set(orphans.values()))
            for individual_eid, workplace_account in orphans.items():
                if workplace_account in hie_groups:
                    hospitals[individual_eid] = (True, hie_groups[workplace_account])
        groups = {}
        if not dry_run and "ts_hospitalgroup" in mapped:
            groups = self._parent_or_group({h for _, h in hospitals.values() if h})

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
                self._fail(
                    "contact",
                    individual_eid,
                    "validate",
                    "; ".join(str(err) for err in record_validation.errors)
                    or "HCP validation failed",
                    category="validation",
                    retryable=False,
                    log=False,
                )
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

            hospital_known, hospital_id = hospitals[individual_eid]
            group_id = groups.get((hospital_id or "").lower())
            payload = self.transformer.transform_hcp(
                hcp,
                account_id=account_id,
                hospital_id=hospital_id,
                hospital_known=hospital_known,
                group_id=group_id,
            )
            key_name = self.config.require_key_field("contact")
            key_value = payload.get(key_name)
            if not key_value:
                result["failed"] += 1
                self._fail(
                    "contact",
                    individual_eid,
                    "upsert",
                    f"Missing key field {key_name}",
                    category="mapping",
                    retryable=False,
                )
                continue

            if dry_run:
                logger.debug(
                    "Dry run contact %s: fields=%s | job=%s",
                    individual_eid, ",".join(sorted(payload)), short_job(self.tracer.job_id),
                )
                result["processed"] += 1
                if i % 50 == 0 or i == total:
                    self.sync_logger.log_progress("contacts", i, total)
                continue

            prepared.append(
                (
                    individual_eid,
                    key_name,
                    payload,
                    account_id,
                    hospital_id,
                    group_id,
                )
            )

        if dry_run or not prepared:
            return result

        key_name = prepared[0][1]
        done = 0
        departments: set[str] = set()
        hospital_accounts: set[str] = set()
        group_accounts: set[str] = set()
        for batch in chunked(prepared, self.bulk_batch_size):
            self._check_deadline("contacts", "contacts", result)
            payloads = [item[2] for item in batch]
            try:
                bulk_results, rejected, unchanged = self._bulk_upsert(
                    "contacts", key_name, payloads
                )
            except Exception as e:
                result["failed"] += len(batch)
                for individual_eid, _, payload, *_ in batch:
                    self._fail(
                        "contact",
                        individual_eid,
                        "upsert",
                        str(e),
                        external_id=payload.get(key_name),
                        exc=e,
                    )
                raise

            for (
                individual_eid,
                _,
                payload,
                department_id,
                hospital_id,
                group_id,
            ), (match, contact_id, created) in zip(batch, bulk_results):
                if not contact_id:
                    error = rejected.get(match)
                    result["failed"] += 1
                    self._fail(
                        "contact",
                        individual_eid,
                        "upsert",
                        str(error or "Dataverse rejected the record"),
                        external_id=payload.get(key_name),
                        exc=error,
                    )
                    continue
                self._contact_cache[str(individual_eid)] = contact_id
                if department_id:
                    departments.add(str(department_id))
                if hospital_id:
                    hospital_accounts.add(str(hospital_id))
                if group_id:
                    group_accounts.add(str(group_id))
                if created:
                    result["created"] += 1
                elif match in unchanged:
                    result["skipped"] += 1
                else:
                    result["updated"] += 1
                result["processed"] += 1
                done += 1
                if individual_eid not in (partial or ()):
                    self._note_success(
                        individual_eid, entity="contact", external_id=payload.get(key_name)
                    )
                self.sync_logger.log_record_success(
                    "contact",
                    individual_eid,
                    contact_id,
                    _operation(created, match in unchanged),
                )

            self.sync_logger.log_progress("contacts", done, len(prepared))

        if update_hierarchy_levels:
            self._set_account_hierarchy_levels(
                departments, hospital_accounts, group_accounts
            )
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
            "unjoined": {"activities": 0, "individuals": 0},
        }

        workplaces = []
        individuals: dict[str, dict] = {}
        workplace_index: dict[str, dict] = {}
        all_activities: list[dict] = []

        started = self.sync_logger.phase_started(
            "extract", codbase=codebase, date_from=date_from, date_to=date_to
        )
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

        activities_by_individual = main_activity_by_individual(all_activities)
        self._hcp_name_by_workplace = hcp_name_by_workplace(
            individuals.values(), all_activities
        )
        self._partial_population = True
        self.sync_logger.phase_finished(
            "extract",
            started,
            {
                "workplaces": len(workplaces),
                "individuals": len(individuals),
                "main_activities": len(activities_by_individual),
                "activity_packets": len(all_activities),
                "inserted": result["by_process_code"].get("I", 0),
                "updated": result["by_process_code"].get("U", 0),
                # R is REFRESH, "updated or inserted" (Developer Guide
                # 6.13.5), not a removal: it is upserted like the others.
                "refreshed": result["by_process_code"].get("R", 0),
            },
            codbase=codebase,
        )

        try:
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

            # OneKey sends INDIVIDUAL, ACTIVITY and WORKPLACE as separate
            # results, the ACTIVITY's own individual and workplace empty
            # (checked on UAT), and a delta only those that changed. Every
            # changed individual is written (_compose_delta_hcps). A main
            # activity whose individual did not change, such as a move to
            # another workplace, moves the contact's department, hospital and
            # group (_refresh_contact_placement), as does a change to a
            # department's or hospital's parent. Other activities without
            # their individual are counted on the job, as are new individuals
            # outside the contracted workplaces.
            if individuals:
                composed, result["unjoined"]["individuals"], partial = self._compose_delta_hcps(
                    individuals,
                    activities_by_individual,
                    workplace_index,
                    dry_run=dry_run,
                )
                result["contacts"] = self._sync_hcps(
                    codebase,
                    dry_run=dry_run,
                    composed=composed,
                    partial=partial,
                )
            applied: set[str] = set()
            if not dry_run:
                moves = {}
                for individual_eid, record in activities_by_individual.items():
                    activity = record.get("activity") or {}
                    if individual_eid not in individuals and activity.get("isMainActivity"):
                        moves[individual_eid] = activity.get("workplaceEid")
                placement, applied = self._refresh_contact_placement(
                    moves, set(workplace_index), skip=set(individuals)
                )
                self._merge_entity(result["contacts"], placement)
            result["unjoined"]["activities"] = sum(
                1
                for eid in activities_by_individual
                if eid not in individuals and eid not in applied
            )
            if any(result["unjoined"].values()):
                logger.warning(
                    msg(
                        "Delta changes not applied to contacts",
                        _ctx(self, codbase=codebase),
                        activities_without_individual=result["unjoined"]["activities"],
                        new_individuals_without_workplace=result["unjoined"]["individuals"],
                    )
                )

            if sync_activity_affiliations and all_activities:
                result["activity_affiliations"] = self._sync_activity_affiliations(
                    all_activities, dry_run=dry_run
                )

        except SyncPaused as exc:
            exc.delta_result = result
            raise
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
            # Raise rather than carry on: every contact left out of the cache
            # would fail as "not found", a non-retryable lookup failure, and
            # the job would still cover its window.
            logger.warning(msg("Contact lookup failed", _ctx(self), error=str(e)))
            raise

        return {
            str(eid): self._contact_cache[str(eid)]
            for eid in individual_eids
            if eid and str(eid) in self._contact_cache
        }

    def _lookup_account(self, workplace_eid: str) -> str | None:
        """Lookup account ID from Dataverse by workplace EID."""
        found = self._lookup_accounts_bulk([workplace_eid])
        return found.get(workplace_eid)
    
    def _admit_known(self, workplace_eids=(), individual_eids=()) -> set[str]:
        """Count workplaces and individuals already in Dataverse as contracted.

        A run that reads only part of the population (_partial_population)
        knows only the records it read, so a link from a record that changed
        to one that did not, such as a new activity at an existing workplace,
        would be dropped as outside the contract. Accounts and contacts are
        loaded only from the contracted population, so one that exists is in
        it. Returns the workplace EIDs it added.
        """
        wanted = {
            str(eid) for eid in workplace_eids
            if eid and str(eid) not in self._contracted_workplace_eids
        }
        admitted = set(self._lookup_accounts_bulk(wanted)) if wanted else set()
        self._contracted_workplace_eids |= admitted
        wanted = {
            str(eid) for eid in individual_eids
            if eid and str(eid) not in self._contracted_individual_eids
        }
        if wanted:
            self._contracted_individual_eids |= set(self._lookup_contacts_bulk(wanted))
        return admitted

    def _keep_existing_nameless_names(self, prepared: list[tuple]) -> None:
        """Leave out the built name of nameless workplaces that already exist.

        A delta sees only the activities changed in its window, so the HCP
        part of a built name (integration.hco_name) may be missing, or be a
        different HCP than the full sync chose; writing it would rename the
        account back and forth. Existing accounts keep their name until the
        next full sync, and a new one gets the name built from the window.
        """
        nameless = [
            workplace_eid
            for workplace_eid, _, _, _, hco in prepared
            if workplace_eid
            and not str(hco.get("officialName") or "").strip()
            and not str(hco.get("usualName") or "").strip()
        ]
        if not nameless:
            return
        existing = self._lookup_accounts_bulk(nameless)
        for workplace_eid, _, _, payload, _ in prepared:
            if workplace_eid in existing:
                payload.pop("name", None)

    def _hospital_of(self, hcp: dict, parent_accounts: dict) -> tuple[bool, str | None]:
        """Return ``(known, hospital account id)`` for one contact row.

        The hospital is the parent of the HCP's workplace: from OneKey when the
        row carries its workplace, else from the workplace's account. A
        workplace without a parent account gives ``(True, None)``, which
        clears it; an unknown one ``(False, None)``, which leaves it as it is.
        """
        if hcp.get("workplaceLoaded"):
            return True, self._account_cache.get(hcp.get("hospitalWorkplaceEid") or "")
        workplace_eid = hcp.get("workplaceEid")
        return workplace_eid in parent_accounts, parent_accounts.get(workplace_eid)

    def _parent_or_group(self, account_ids) -> dict[str, str | None]:
        """Return ``{account id: its parent account id or None}``.

        The parent is the account's own parent (managerWorkplaceEid) and,
        without one, its HIE parent when that is a group of organisations.
        This is a department's hospital and a hospital's group. OneKey links
        a hospital to its group only through HIE: in prod (2026-10-23) no
        hospital had a manager.
        """
        ids = sorted({str(i).lower() for i in account_ids if i})
        rows = self._account_rows(ids)
        managers = {a: (rows.get(a) or {}).get("_parentaccountid_value") for a in ids}
        hie_groups = self._hie_group_parents(a for a in ids if not managers[a])
        return {a: (str(managers[a]).lower() if managers[a] else hie_groups.get(a)) for a in ids}

    def _hie_group_parents(self, account_ids) -> dict[str, str]:
        """Return ``{account id: HIE parent id}`` where that parent is a group.

        The HIE parent is the account's direct parent in OneKey's hierarchy
        (an HIE "Is managed by" relation, written by the HCO-HCO phase before
        contacts). A group of organisations has structure type GOR or POR. An
        ended relation is ignored; of several, the one that started last wins.
        """
        ids = sorted({str(i).lower() for i in account_ids if i})
        hie = self.config.get_value("linkcode", "HIE")
        group_structure = self.config.get_value("structuretype", "GOR")
        if not ids or not hie or not group_structure:
            return {}
        today = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
        latest: dict[str, tuple[str, str]] = {}
        for chunk in chunked(ids, 50):
            rows = self.dataverse.retrieve_multiple(
                "ts_affiliations",
                select=["_ts_fromid_value", "_ts_toid_value", "ts_startdate"],
                filter=(
                    f"ts_linktype eq {hie} and ts_isparent eq true and statecode eq 0"
                    f" and (ts_enddate eq null or ts_enddate ge {today}) and ("
                    + " or ".join(f"_ts_fromid_value eq {i}" for i in chunk)
                    + ")"
                ),
            )
            for row in rows:
                child, parent = row.get("_ts_fromid_value"), row.get("_ts_toid_value")
                if not child or not parent:
                    continue
                started = str(row.get("ts_startdate") or "")
                if child.lower() not in latest or started > latest[child.lower()][1]:
                    latest[child.lower()] = (parent.lower(), started)
        parents = {child: parent for child, (parent, _) in latest.items()}
        rows = self._account_rows(parents.values())
        return {
            account: parent
            for account, parent in parents.items()
            if str((rows.get(parent) or {}).get("ts_structuretype")) == str(group_structure)
        }

    def _clear_removed_parents(self, hcos: list[dict], allow) -> tuple[int, int]:
        """Clear the parent of accounts OneKey now sends without a manager.

        The hierarchy edges only link, so a workplace that lost its
        managerWorkplaceEid would keep its old parent account. UpdateMultiple
        cannot clear parentaccountid (HTTP 400, dev 2026-10-23), so the clears
        are PATCHes in a $batch. Returns ``(cleared, failed)``.
        """
        orphans = sorted({
            str(hco["workplaceEid"])
            for hco in hcos
            if hco.get("workplaceEid")
            and not hco.get("parentWorkplaceEid")
            and (allow is None or hco["workplaceEid"] in allow)
        })
        if not orphans:
            return 0, 0
        account_key = self.config.require_key_field("account")
        accounts = resolve_accounts(self.dataverse, orphans, account_key)
        clears = [
            (eid, row["accountid"])
            for eid, row in sorted(accounts.items())
            if row.get("accountid") and row.get("_parentaccountid_value")
        ]
        cleared = failed = 0
        for chunk in chunked(clears, 100):
            outcomes = self.dataverse.batch_write(
                [("PATCH", f"accounts({account_id})", {"parentaccountid": None}) for _, account_id in chunk],
                idempotent=True,
            )
            for (eid, _), (status, _, error) in zip(chunk, outcomes):
                if status and status < 400:
                    cleared += 1
                else:
                    failed += 1
                    logger.warning(msg("Parent account not cleared", _ctx(self), workplace=eid, error=str(error)[:300]))
        if clears:
            logger.info(msg("Parent accounts cleared", _ctx(self), cleared=cleared, failed=failed))
        return cleared, failed

    def _deactivate_removed_relations(self, raw_workplaces: list[dict]) -> int:
        """Deactivate HCO–HCO rows a processed workplace no longer has in OneKey.

        Each row belongs to the workplace that owns the ``relations[]`` entry:
        its key is ``{owner} - {related} - {linkCode}``. A workplace record
        without a ``relations`` list is left alone, as what it lacks is
        unknown. W2W-011 makes a relation that comes back active again.
        Returns the number of rows deactivated.
        """
        current: set[str] = set()
        owners: set[str] = set()
        for record in raw_workplaces:
            workplace = record.get("workplace") or record
            eid = workplace.get("workplaceEid")
            relations = workplace.get("relations")
            if not eid or not isinstance(relations, list):
                continue
            owners.add(str(eid))
            for relation in relations:
                if (
                    isinstance(relation, dict)
                    and relation.get("typeCode") == WORKPLACE_TO_WORKPLACE_LINK
                    and relation.get("onekeyEid")
                    and relation.get("linkCode")
                ):
                    current.add(f"{eid} - {relation['onekeyEid']} - {relation['linkCode']}")
        if not owners:
            return 0
        entity_set = self._affiliation_entity_set()
        key = self.config.require_key_field("ts_affiliation")
        if len(owners) > 200:
            # A full sync: read the codebase's active rows once.
            prefixes = sorted({eid[:3] for eid in owners})
            scopes = ["(" + " or ".join(f"startswith({key},'{escape_odata(p)}')" for p in prefixes) + ")"]
        else:
            scopes = [
                "(" + " or ".join(f"startswith({key},'{escape_odata(o)} - ')" for o in chunk) + ")"
                for chunk in chunked(sorted(owners), 25)
            ]
        stale = []
        for scope in scopes:
            for row in self.dataverse.retrieve_multiple(
                entity_set,
                select=[key, "ts_affiliationid"],
                filter=f"statecode eq 0 and contains({key},' - ') and {scope}",
            ):
                row_key = row.get(key) or ""
                if row_key.split(" - ")[0] in owners and row_key not in current:
                    stale.append(row)
        deactivated = 0
        for chunk in chunked(stale, 100):
            outcomes = self.dataverse.batch_write(
                [("PATCH", f"{entity_set}({row['ts_affiliationid']})", {"statecode": 1}) for row in chunk],
                idempotent=True,
            )
            for row, (status, _, error) in zip(chunk, outcomes):
                if status and status < 400:
                    deactivated += 1
                else:
                    logger.warning(msg("HCO relation not deactivated", _ctx(self), key=row.get(key), error=str(error)[:300]))
        if stale:
            logger.info(msg("HCO relations OneKey no longer sends deactivated", _ctx(self), deactivated=deactivated, found=len(stale)))
        return deactivated

    def _refresh_contact_placement(
        self,
        moves: dict[str, str],
        changed_workplace_eids: set[str],
        skip: set[str],
    ) -> tuple[dict, set[str]]:
        """Recompute department, hospital and group for contacts a delta changed
        without writing them.

        ``moves`` maps an individual whose main activity arrived without the
        individual to that activity's workplace, its new department. A
        contact whose department, hospital or group is a workplace in
        ``changed_workplace_eids`` is recomputed too: that workplace's parent
        or group may have changed. Individuals in ``skip`` were written by the
        contact phase. Each level is the parent account and, without one, the
        HIE group (``_parent_or_group``), read after the account, hierarchy
        and HCO-HCO phases. Only existing contacts are updated.

        Returns the contact counts and the individuals whose move was applied.
        """
        stats = _empty_entity_stats()
        mapped = {fm.target_field for fm in self.config.get_field_mappings("contact")}
        columns = [c for c in ("ts_department", "ts_hospital", "ts_hospitalgroup") if c in mapped]
        if "ts_department" not in columns:
            return stats, set()
        key = self.config.require_key_field("contact")
        departments: dict[str, str] = {}
        moves = {i: w for i, w in moves.items() if i and w and i not in skip}
        if moves:
            self._lookup_contacts_bulk(list(moves))
            self._lookup_accounts_bulk(set(moves.values()))
            for individual, workplace in moves.items():
                if individual in self._contact_cache and workplace in self._account_cache:
                    departments[individual] = str(self._account_cache[workplace]).lower()
        applied = set(departments)
        if changed_workplace_eids:
            self._lookup_accounts_bulk(changed_workplace_eids)
            changed = sorted({
                str(self._account_cache[eid]).lower()
                for eid in changed_workplace_eids
                if eid in self._account_cache
            })
            for chunk in chunked(changed, 25):
                rows = self.dataverse.retrieve_multiple(
                    "contacts",
                    select=[key, "_ts_department_value"],
                    filter=" or ".join(f"_{column}_value eq {i}" for column in columns for i in chunk),
                )
                for row in rows:
                    individual, department = row.get(key), row.get("_ts_department_value")
                    if individual and department and individual not in skip and individual not in departments:
                        departments[individual] = str(department).lower()
        if not departments:
            return stats, applied
        hospitals = self._parent_or_group(set(departments.values()))
        groups = (
            self._parent_or_group({h for h in hospitals.values() if h})
            if "ts_hospitalgroup" in columns
            else {}
        )
        payloads = []
        placements = []
        for individual, department in sorted(departments.items()):
            hospital = hospitals.get(department)
            group = groups.get(hospital) if hospital else None
            placements.append((individual, department, hospital, group))
            payloads.append({
                key: individual,
                **self.transformer.contact_account_binds(
                    department, hospital, True, group
                ),
            })
        results, rejected, unchanged = self._bulk_upsert(
            "contacts", key, payloads, update_only=True
        )
        stats["processed"] = len(payloads)
        hierarchy_departments: set[str] = set()
        hierarchy_hospitals: set[str] = set()
        hierarchy_groups: set[str] = set()
        for (_individual, department, hospital, group), (match, record_id, _) in zip(
            placements, results
        ):
            if not record_id:
                stats["failed"] += 1
                if match in rejected:
                    logger.warning(
                        msg(
                            "Contact placement not updated",
                            _ctx(self),
                            individual=match,
                            error=str(rejected[match])[:300],
                        )
                    )
            elif match in unchanged:
                stats["skipped"] += 1
            else:
                stats["updated"] += 1
            if record_id:
                hierarchy_departments.add(department)
                if hospital:
                    hierarchy_hospitals.add(hospital)
                if group:
                    hierarchy_groups.add(group)
        self._set_account_hierarchy_levels(
            hierarchy_departments,
            hierarchy_hospitals,
            hierarchy_groups,
        )
        logger.info(
            msg(
                "Contact placement refreshed",
                _ctx(self),
                moves=len(applied),
                **{
                    key: stats[key]
                    for key in ("processed", "updated", "skipped", "failed")
                },
            )
        )
        return stats, applied

    def _set_account_hierarchy_levels(
        self,
        departments=(),
        hospitals=(),
        groups=(),
    ) -> int:
        """Set Account hierarchy level from the resolved contact placement chain.

        An Account directly referenced by ``contact.ts_department`` is level 3;
        one referenced by ``contact.ts_hospital`` is level 2; and one referenced
        by ``contact.ts_hospitalgroup`` is level 1. If an Account has more than
        one role, the most direct HCP role wins (3 > 2 > 1). A partial delta
        never lowers an existing level because other unchanged contacts may
        still reference that Account in the more direct role.
        """
        levels: dict[str, int] = {}
        for account_ids, level in ((groups, 1), (hospitals, 2), (departments, 3)):
            for account_id in account_ids:
                if account_id:
                    key = str(account_id).lower()
                    levels[key] = max(level, levels.get(key, 0))
        if not levels:
            return 0

        existing = self._account_hierarchy_level_cache
        missing = sorted(account_id for account_id in levels if account_id not in existing)
        for account_ids in chunked(missing, 50):
            rows = self.dataverse.retrieve_multiple(
                "accounts",
                select=["accountid", "ts_hierarchylevel"],
                filter=" or ".join(
                    f"accountid eq {account_id}" for account_id in account_ids
                ),
            )
            # Remember an empty value too, so another placement update in the
            # same job does not repeat this Account read.
            for account_id in account_ids:
                existing[account_id] = None
            for row in rows:
                account_id = str(row.get("accountid") or "").lower()
                try:
                    current = int(row.get("ts_hierarchylevel"))
                except (TypeError, ValueError):
                    continue
                if account_id:
                    existing[account_id] = current

        updates = [
            {"accountid": account_id, "ts_hierarchylevel": level}
            for account_id, level in sorted(levels.items())
            if (existing.get(account_id) or 0) < level
        ]
        updated = 0
        for batch in chunked(updates, self.bulk_batch_size):
            self._check_deadline("contacts")
            update_multiple = getattr(self.dataverse, "update_multiple", None)
            if update_multiple:
                update_multiple("accounts", batch)
            else:
                for row in batch:
                    self.dataverse.update(
                        "accounts",
                        row["accountid"],
                        {"ts_hierarchylevel": row["ts_hierarchylevel"]},
                    )
            for row in batch:
                existing[row["accountid"]] = row["ts_hierarchylevel"]
            updated += len(batch)
            self.records_written += len(batch)
        if updates:
            logger.info(
                msg(
                    "Account hierarchy levels updated",
                    _ctx(self),
                    updated=updated,
                    departments=sum(level == 3 for level in levels.values()),
                    hospitals=sum(level == 2 for level in levels.values()),
                    groups=sum(level == 1 for level in levels.values()),
                )
            )
        return updated

    def _account_rows(self, account_ids) -> dict[str, dict]:
        """Return ``{account id: row}`` with the structure type and parent account."""
        rows: dict[str, dict] = {}
        for chunk in chunked(sorted({str(i).lower() for i in account_ids if i}), 50):
            for row in self.dataverse.retrieve_multiple(
                "accounts",
                select=[
                    "accountid",
                    "ts_structuretype",
                    "_parentaccountid_value",
                    "ts_hierarchylevel",
                ],
                filter=" or ".join(f"accountid eq {i}" for i in chunk),
            ):
                account_id = str(row.get("accountid")).lower()
                rows[account_id] = row
                if "ts_hierarchylevel" in row:
                    try:
                        level = int(row.get("ts_hierarchylevel"))
                    except (TypeError, ValueError):
                        level = None
                    self._account_hierarchy_level_cache[account_id] = level
        return rows

    def _account_parents(self, workplace_eids) -> dict[str, str | None]:
        """Return ``{workplaceEid: parent account id or None}`` for existing accounts."""
        field_name = self.config.require_key_field("account")
        rows = self.dataverse.find_many_by_field(
            "accounts", field_name, sorted(workplace_eids),
            select=[field_name, "accountid", "_parentaccountid_value"],
        )
        return {
            str(eid): row.get("_parentaccountid_value")
            for eid, row in rows.items()
            if row.get("accountid")
        }

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
                    select=[field_name, "accountid", "ts_hierarchylevel"],
                )
                for match_value, row in rows.items():
                    account_id = row.get("accountid")
                    if account_id:
                        self._account_cache[str(match_value)] = account_id
                        if "ts_hierarchylevel" in row:
                            try:
                                level = int(row.get("ts_hierarchylevel"))
                            except (TypeError, ValueError):
                                level = None
                            self._account_hierarchy_level_cache[
                                str(account_id).lower()
                            ] = level
            else:
                for eid in needed:
                    record = self.dataverse.find_one(
                        "accounts",
                        filter=f"{field_name} eq '{eid}'",
                        select=[field_name, "accountid", "ts_hierarchylevel"],
                    )
                    if record and record.get("accountid"):
                        self._account_cache[eid] = record["accountid"]
                        if "ts_hierarchylevel" in record:
                            try:
                                level = int(record.get("ts_hierarchylevel"))
                            except (TypeError, ValueError):
                                level = None
                            self._account_hierarchy_level_cache[
                                str(record["accountid"]).lower()
                            ] = level
        except Exception as e:
            # Raise for the reason given in _lookup_contacts_bulk.
            logger.warning(msg("Account lookup failed", _ctx(self), error=str(e)))
            raise

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
