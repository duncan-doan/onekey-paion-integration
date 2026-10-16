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
from .picklist_sync import sync_picklists_from_config
from .w2w_relations import collect_w2w_relations
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
from dataverse.client import chunked

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
        # Contracted workplace EIDs from the latest refreshMyOnekey extract
        self._contracted_workplace_eids: set[str] = set()
        self._contracted_individual_eids: set[str] = set()
        # Cache for contact lookups (individualEid -> contactid)
        self._contact_cache: dict[str, str] = {}
        # One HCP name per nameless workplace, from the same population extract.
        self._hcp_name_by_workplace: dict[str, str] = {}
        # False only while sync_full holds the names from a whole population
        # walk. A delta window, a retry or a sample sees some of a workplace's
        # activities at most, so existing accounts keep their name
        # (_keep_existing_nameless_names).
        self._hcp_names_partial = True
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
                    self._hcp_names_partial = bool(max_records)
                    if not hco_only and not hcp_only:
                        packets = self._collect_population_packets(
                            codebase, max_records=max_records
                        )
                        self._hcp_name_by_workplace = hcp_name_by_workplace(
                            packets[1], packets[2]
                        )

                    if not hcp_only:
                        hco_result, flattened_hcos, raw_workplaces = self._sync_hcos(
                            codebase,
                            max_records=max_records,
                            dry_run=dry_run,
                            raw_workplaces=packets[0] if packets is not None else None,
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
        self, entity_set: str, key_name: str, payloads: list[dict]
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
    ) -> tuple[dict, list[dict], list[dict]]:
        with self._phase(PHASE_HCO, "accounts", codbase=codebase) as phase:
            result = self._sync_hcos_impl(
                codebase,
                max_records=max_records,
                dry_run=dry_run,
                flattened=flattened,
                raw_workplaces=raw_workplaces,
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

        if self._hcp_names_partial:
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

            for (workplace_eid, _, key_value, _, hco), (match, account_id, created) in zip(
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

        prepared = []
        for hco in hcos:
            workplace_eid = hco.get("workplaceEid")
            address_eid = hco.get("address_addressEid") or hco.get("addressEid")
            if not workplace_eid or not address_eid:
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
            payload = self.transformer.transform_address(
                hco, parent_account_id=account_id
            )
            if not payload:
                continue
            key = payload.get(self.config.require_key_field("customeraddress"))
            if not key:
                result["failed"] += 1
                self._fail(
                    "customeraddress",
                    workplace_eid,
                    "upsert",
                    "Missing customeraddress key",
                    external_id=address_eid,
                    category="mapping",
                    retryable=False,
                )
                continue
            prepared.append((key, payload, workplace_eid))

        logger.info(msg("Upserting addresses", _ctx(self), records=len(prepared)))
        if dry_run:
            result["processed"] = len(prepared)
            return result
        if not prepared:
            return result

        address_key = self.config.require_key_field("customeraddress")
        for batch in chunked(prepared, self.bulk_batch_size):
            self._check_deadline("addresses", "addresses", result)
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
        edges = edges_from_hcos(hcos, allowed_workplace_eids=allow)
        result["edges"] = len(edges)
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
        edges = collect_w2w_relations(
            raw_workplaces, allowed_workplace_eids=allow
        )
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

        roles = self._upsert_affiliation_roles(role_rows)
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

    def _upsert_affiliation_roles(self, rows: list[dict]) -> dict:
        """Upsert TIH role rows onto the affiliations just written.

        The child table has no alternate key. ``ts_name``
        (``{activityEid} - {role code}``) is the match column.
        """
        result = _empty_entity_stats()
        if not rows:
            return result
        entity_set = self._role_entity_set()
        done = 0
        for batch in chunked(rows, self.bulk_batch_size):
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

            payload = self.transformer.transform_hcp(hcp, account_id=account_id)
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

            prepared.append((individual_eid, key_name, payload, hcp))

        if dry_run or not prepared:
            return result

        key_name = prepared[0][1]
        done = 0
        for batch in chunked(prepared, self.bulk_batch_size):
            self._check_deadline("contacts", "contacts", result)
            payloads = [item[2] for item in batch]
            try:
                bulk_results, rejected, unchanged = self._bulk_upsert("contacts", key_name, payloads)
            except Exception as e:
                result["failed"] += len(batch)
                for individual_eid, _, payload, _ in batch:
                    self._fail(
                        "contact",
                        individual_eid,
                        "upsert",
                        str(e),
                        external_id=payload.get(key_name),
                        exc=e,
                    )
                raise

            for (individual_eid, _, payload, hcp), (match, contact_id, created) in zip(
                batch, bulk_results
            ):
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
        self._hcp_names_partial = True
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
            # changed individual is written (_compose_delta_hcps). An activity
            # whose individual did not change, such as a move to another
            # workplace, is not applied to the contact until the next full
            # sync; it is counted on the job, as are new individuals outside
            # the contracted workplaces.
            result["unjoined"]["activities"] = sum(
                1 for eid in activities_by_individual if eid not in individuals
            )
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
