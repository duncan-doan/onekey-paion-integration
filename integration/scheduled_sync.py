"""Retry pending integration failures, then pull the OneKey delta.

Order:
1. Replay pending retryable failure rows.
2. Pull the delta for the watermark window.
3. Mark pending rows Resolved when that delta upserts the same source ID.
4. Advance the watermark only when the delta itself succeeded.

A failed ID that OneKey does not change again will not appear in a later
delta, so step 1 is what retries it. Step 3 clears a pending row when the
delta happens to return the same ID and the upsert succeeds.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from integration.env import REPO_ROOT, load_env
from integration.job_trace import TRIGGER_TIMER
from integration.retry_sync import resolve_pending_for_ids, retry_pending_failures

logger = logging.getLogger(__name__)


def watermark_path(directory: Path, codebase: str) -> Path:
    return Path(directory) / f"sync_watermark_{codebase.upper()}.json"


def load_watermark(directory: Path, codebase: str) -> str | None:
    path = watermark_path(directory, codebase)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("date_to")
    except (OSError, json.JSONDecodeError):
        return None


def save_watermark(directory: Path, codebase: str, date_to: str) -> None:
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = watermark_path(directory, codebase)
    path.write_text(
        json.dumps(
            {
                "codbase": codebase.upper(),
                "date_to": date_to,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def delta_window(
    watermark_dir: Path,
    codebases: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[str, str, str]:
    """Return ISO date_from, ISO date_to, and the calendar date stored as the watermark."""
    calendar_to = date_to or datetime.now().strftime("%Y-%m-%d")
    start = date_from
    if not start:
        marks = [load_watermark(watermark_dir, codebase) for codebase in codebases]
        marks = [mark for mark in marks if mark]
        if marks:
            start = min(marks)
            logger.info("Using watermark date_from=%s", start)
        else:
            start = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    iso_from = start if "T" in start else f"{start}T00:00:00Z"
    iso_to = calendar_to if "T" in calendar_to else f"{calendar_to}T23:59:59Z"
    stored_to = calendar_to.split("T", 1)[0]
    return iso_from, iso_to, stored_to


def run_retry_then_delta(
    engine,
    *,
    codebases: list[str],
    date_from: str,
    date_to: str,
    watermark_date: str | None = None,
    watermark_dir: Path | None = None,
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    retry_limit: int = 100,
    sync_addresses: bool = True,
    sync_hierarchy: bool = True,
    sync_hco_relations: bool = True,
    sync_activity_affiliations: bool = True,
    sync_picklists: bool = True,
) -> dict:
    """Run the scheduled job: retry, delta, resolve IDs the delta fixed."""
    retry = retry_pending_failures(
        engine,
        limit=retry_limit,
        dry_run=dry_run,
        trigger=trigger,
    )
    engine.succeeded_ids.clear()
    delta = engine.sync_delta(
        codebases=codebases,
        date_from=date_from,
        date_to=date_to,
        dry_run=dry_run,
        sync_addresses=sync_addresses,
        sync_hierarchy=sync_hierarchy,
        sync_hco_relations=sync_hco_relations,
        sync_activity_affiliations=sync_activity_affiliations,
        sync_picklists=sync_picklists,
    )
    resolved = 0
    if delta.get("success") and not dry_run:
        resolved = resolve_pending_for_ids(engine.dataverse, engine.succeeded_ids)
        if watermark_dir is not None and watermark_date:
            for codebase in codebases:
                save_watermark(watermark_dir, codebase, watermark_date)
    return {
        "success": bool(delta.get("success")),
        "retry": retry,
        "delta": delta,
        "resolved_by_delta": resolved,
    }


def run_from_env(
    *,
    codebases: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    watermark_dir: Path | None = None,
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    retry_limit: int = 100,
    config_dir: str | None = None,
    bulk_batch_size: int | None = None,
) -> dict:
    """Build the OneKey and Dataverse clients from the environment and run the job."""
    from dataverse import DataverseClient, DataverseConfig
    from integration import create_sync_engine
    from onekey import OneKeyClient, OneKeyConfig, OneKeySession

    load_env()
    selected = codebases or ["WUK"]
    directory = Path(watermark_dir or (REPO_ROOT / "logs"))
    iso_from, iso_to, stored_to = delta_window(
        directory, selected, date_from=date_from, date_to=date_to
    )

    onekey_config = OneKeyConfig.from_env()
    onekey = OneKeyClient(OneKeySession(onekey_config))
    dataverse_config = DataverseConfig.from_env()
    dataverse = DataverseClient(dataverse_config)
    engine = create_sync_engine(
        onekey,
        dataverse,
        config_dir,
        bulk_batch_size=bulk_batch_size,
    )
    try:
        return run_retry_then_delta(
            engine,
            codebases=selected,
            date_from=iso_from,
            date_to=iso_to,
            watermark_date=stored_to,
            watermark_dir=None if dry_run else directory,
            dry_run=dry_run,
            trigger=trigger,
            retry_limit=retry_limit,
        )
    finally:
        onekey.close()
        if hasattr(dataverse, "close"):
            dataverse.close()
