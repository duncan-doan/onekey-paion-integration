"""Azure Function App - read a sample of OneKey data and log it.

Read-only: nothing is written to Dataverse. The point is to prove that the
deployed app can reach OneKey with the credentials in App Settings, so a
failure here is a connectivity or configuration problem and nothing else.

App Settings required (the same names the CLI scripts read from .env):
    ONEKEY_ENV or ONEKEY_BASE_URL, ONEKEY_USERNAME, ONEKEY_PASSWORD,
    ONEKEY_CONNECTOR_ID
Optional:
    ONEKEY_CODBASE   territory to sample (default WUK)
    ONEKEY_SAMPLE    records per entity type (default 5)
"""

import logging
import os

import azure.functions as func

from integration import load_env
from onekey import OneKeyClient, OneKeyConfig, OneKeyError, OneKeySession
from onekey.parse import flatten_hco, flatten_hcp

app = func.FunctionApp()

# In Azure the credentials arrive as App Settings, already in os.environ.
# load_env() returns {} when there is no .env file, and never overwrites a
# real environment variable, so this is a no-op in the cloud and a
# convenience when running the app locally with `func start`.
load_env()


@app.timer_trigger(schedule="0 0 */6 * * *", arg_name="timer", run_on_startup=False)
def sample_onekey(timer: func.TimerRequest) -> None:
    """Every six hours: read a few HCOs and HCPs from OneKey and log them."""
    if timer.past_due:
        logging.warning("onekey_sample_timer_past_due")

    cod_base = os.environ.get("ONEKEY_CODBASE", "WUK")
    limit = int(os.environ.get("ONEKEY_SAMPLE", "5"))

    try:
        config = OneKeyConfig.from_env()
        logging.info(
            "onekey_connect base_url=%s connector=%s codbase=%s limit=%d",
            config.base_url, config.connector_id, cod_base, limit,
        )

        with OneKeyClient(OneKeySession(config)) as client:
            # Verify the credentials up front so bad secrets surface as an
            # auth error rather than as an empty result set.
            client.session.test_connection()

            # Both searches are by criteria, which OneKey does not account
            # against the subscription -- unlike a search by id.
            for raw in client.search_hco(
                cod_base=cod_base, only_valid=True, result_size=limit
            ):
                hco = flatten_hco(raw)
                logging.info(
                    "onekey_hco eid=%s name=%s type=%s city=%s",
                    hco.get("workplaceEid"),
                    hco.get("officialName") or hco.get("usualName"),
                    hco.get("typeLabel"),
                    hco.get("address_city"),
                )

            # search_hcp wraps entityType=ACTIVITY, so each result carries the
            # individual, the workplace and the activity linking them.
            for raw in client.search_hcp(
                cod_base=cod_base, last_name="S", result_size=limit
            ):
                hcp = flatten_hcp(raw)
                logging.info(
                    "onekey_hcp eid=%s name=%s %s specialty=%s workplace=%s",
                    hcp.get("individualEid"),
                    hcp.get("firstName"),
                    hcp.get("lastName"),
                    hcp.get("specialtyLabel"),
                    hcp.get("workplaceName"),
                )

        logging.info("onekey_sample_complete codbase=%s limit=%d", cod_base, limit)
    except OneKeyError:
        # OneKey UAT returns 503/500 during maintenance windows; log and let
        # the next tick retry rather than failing the whole invocation.
        logging.exception("onekey_sample_failed")
    except Exception:
        logging.exception("onekey_sample_unexpected_error")


@app.timer_trigger(schedule="0 0 1 * * *", arg_name="timer", run_on_startup=False)
def scheduled_onekey_sync(timer: func.TimerRequest) -> None:
    """Daily 01:00 UTC: retry pending failures, then delta.

    Stays idle unless App Setting SYNC_SCHEDULE is true. A country delta can
    run longer than host.json functionTimeout, so the CLI script
    scripts/run_scheduled_sync.py is the job to schedule outside Azure when
    the window is larger than 30 minutes.
    """
    enabled = os.environ.get("SYNC_SCHEDULE", "").strip().lower()
    if enabled not in {"1", "true", "yes"}:
        logging.info("scheduled_sync_skipped SYNC_SCHEDULE is not enabled")
        return
    if timer.past_due:
        logging.warning("scheduled_sync_timer_past_due")

    from integration.scheduled_sync import run_from_env

    codebases = [
        part.strip().upper()
        for part in os.environ.get("ONEKEY_CODBASE", "WUK").split(",")
        if part.strip()
    ]
    logging.info("scheduled_sync_start codebases=%s", ",".join(codebases))
    result = run_from_env(codebases=codebases)
    logging.info(
        "scheduled_sync_complete success=%s retry_selected=%s resolved_by_delta=%s",
        result.get("success"),
        (result.get("retry") or {}).get("selected"),
        result.get("resolved_by_delta"),
    )
