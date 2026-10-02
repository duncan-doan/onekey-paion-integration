"""Push a small sample of OneKey HCOs and their related data into Dataverse.

The sample_ok_to_dv function uses this to prove the whole path on a few
records: OneKey search, transform, Dataverse upsert and the integration job
trace. The records go through the same engine phases as the full sync.
"""

from __future__ import annotations

import logging

from integration.job_trace import JOB_HCO, TRIGGER_TIMER
from onekey import EXACT, search_field
from onekey.parse import flatten_hco

logger = logging.getLogger(__name__)


def _related_workplace_eids(raws: list[dict], flats: list[dict]) -> set[str]:
    """Parent and relation EIDs named on the sampled workplaces."""
    related = {
        str(flat["parentWorkplaceEid"])
        for flat in flats
        if flat.get("parentWorkplaceEid")
    }
    for raw in raws:
        for relation in (raw.get("workplace") or {}).get("relations") or []:
            if relation.get("onekeyEid"):
                related.add(str(relation["onekeyEid"]))
    return related


def _main_activities(records: list[dict]) -> list[dict]:
    """One main ACTIVITY per individual, so each contact's parent is its main workplace."""
    chosen: dict[str, dict] = {}
    for record in records:
        activity = record.get("activity") or {}
        individual_eid = (record.get("individual") or {}).get(
            "individualEid"
        ) or activity.get("individualEid")
        if individual_eid and activity.get("isMainActivity"):
            chosen.setdefault(str(individual_eid), record)
    return list(chosen.values())


def sync_sample(
    engine,
    *,
    codebase: str,
    hco_limit: int = 5,
    hcp_limit: int = 50,
    trigger: int = TRIGGER_TIMER,
) -> dict:
    """Upsert a few contracted HCOs and the data linked to them.

    For each HCO: the account, its customeraddress rows, its parentaccountid,
    its HCO-HCO ts_affiliation rows, and the HCPs whose main activity is there
    (contact plus HCP-HCO ts_affiliation). A parent or related HCO outside the
    sample is linked only when it is already in Dataverse, so the sample does
    not record failures for HCOs it never loaded. ``hcp_limit`` caps the
    activities read from OneKey before main activities are picked.

    The run is traced as an "HCO Only" ts_integrationjob, which the delta
    window ignores.
    """
    raws = engine.onekey.search_hco(
        cod_base=codebase,
        only_valid=True,
        result_size=hco_limit,
        is_in_contract=True,
    )
    flats = [flatten_hco(raw) for raw in raws]
    sampled = {str(flat["workplaceEid"]) for flat in flats if flat.get("workplaceEid")}
    results = {"success": False, "mode": "sample", "codebases": [codebase], "errors": []}
    if not sampled:
        results["errors"].append(f"OneKey returned no contracted HCOs for {codebase}")
        return results

    activities = _main_activities(
        engine.onekey.search_hcp(
            cod_base=codebase,
            fields=[search_field("workplace.workplaceEid", sorted(sampled), EXACT)],
            result_size=hcp_limit,
            is_in_contract=True,
        )
    )
    logger.info(
        "Sample %s: %d HCO(s), %d HCP(s) with their main activity there",
        codebase,
        len(sampled),
        len(activities),
    )

    engine.tracer.trigger = trigger
    engine.tracer.retry_of_job_id = None
    engine._begin_job(JOB_HCO, [codebase], dry_run=False)
    aborted = False
    try:
        results["accounts"], flats, _ = engine._sync_hcos(codebase, flattened=flats)
        results["addresses"] = engine._sync_addresses(flats)

        related = _related_workplace_eids(raws, flats) - sampled
        if related:
            engine._lookup_accounts_bulk(related)
        allow = sampled | {eid for eid in related if eid in engine._account_cache}
        results["hierarchy"] = engine._sync_hierarchy(
            flats, allowed_workplace_eids=allow
        )
        results["affiliations"] = engine._sync_hco_relations(
            raws, allowed_workplace_eids=allow
        )

        results["contacts"] = engine._sync_hcps(codebase, composed=activities)
        results["activity_affiliations"] = engine._sync_activity_affiliations(
            activities
        )
        results["success"] = True
        return results
    except Exception as exc:
        aborted = True
        results["errors"].append(str(exc))
        raise
    finally:
        engine._finish_job(results, aborted=aborted)
