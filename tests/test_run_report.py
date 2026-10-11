"""Offline tests for a scheduled run's total time."""

from datetime import datetime, timezone

from integration.job_trace import (
    JOB_DELTA,
    JOB_FULL,
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
)
from integration.run_report import (
    STATE_RUNNING,
    STATE_STOPPED,
    TYPE_RETRY,
    fetch_run_jobs,
    format_duration,
    latest_run_id,
    run_queued_at,
    run_size,
    summarize_run,
)

RUN = "delta-20261012T000001Z-2"
NOW = datetime(2026, 10, 12, 1, 30, 0, tzinfo=timezone.utc)


def job(codebase, status, started, *, completed=None, duration=None, beat=None,
        job_type=JOB_DELTA, run=RUN, retry_of=None, errors=None):
    return {
        "ts_jobid": f"job-{codebase}",
        "ts_name": f"{codebase} Delta 2026-10-12 00:00 run={run}",
        "ts_jobtype": job_type,
        "ts_status": status,
        "ts_codebase": codebase,
        "ts_startedon": started,
        "ts_completedon": completed,
        "ts_durationseconds": duration,
        "ts_lastheartbeat": beat or completed or started,
        "ts_errorsummary": errors,
        "_ts_retryofjob_value": retry_of,
    }


WDK_DONE = job("WDK", STATUS_SUCCEEDED, "2026-10-12T00:00:14Z", completed="2026-10-12T00:00:43Z", duration=29)
WNL_DONE = job("WNL", STATUS_SUCCEEDED, "2026-10-12T00:00:51Z", completed="2026-10-12T00:02:12Z", duration=81)


class Jobs:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def retrieve_multiple(self, entity_set, **kwargs):
        self.queries.append(kwargs)
        return list(self.rows)


def test_the_run_id_carries_its_queue_time_and_country_count():
    assert run_queued_at(RUN) == datetime(2026, 10, 12, 0, 0, 1, tzinfo=timezone.utc)
    assert run_size(RUN) == 2
    # Ids from before the count was added still give their queue time.
    assert run_queued_at("delta-20261012T000001Z") == datetime(2026, 10, 12, 0, 0, 1, tzinfo=timezone.utc)
    assert run_size("delta-20261012T000001Z") is None
    assert run_queued_at("manual") is None


def test_total_runs_from_queueing_to_the_last_job_end():
    summary = summarize_run(RUN, [WDK_DONE, WNL_DONE], now=NOW)
    assert summary["complete"] is True
    assert summary["total_s"] == 131  # 00:00:01 to 00:02:12
    assert summary["work_s"] == 110
    assert summary["finished"] == ["WDK", "WNL"]


def test_a_country_not_started_yet_keeps_the_run_open():
    summary = summarize_run(RUN, [WDK_DONE], now=NOW)
    assert summary["complete"] is False
    assert summary["running"] == 0
    assert summary["total_s"] == 5399  # queued to now, not to WDK's end


def test_a_failed_job_does_not_finish_its_country():
    failed = job("WNL", STATUS_FAILED, "2026-10-12T00:00:51Z", completed="2026-10-12T00:01:10Z", duration=19)
    stopped_early = job("WNL", STATUS_PARTIAL, "2026-10-12T00:00:51Z", completed="2026-10-12T00:01:10Z",
                        duration=19, errors="WNL: OneKey 503")
    for wnl in (failed, stopped_early):
        assert summarize_run(RUN, [WDK_DONE, wnl], now=NOW)["complete"] is False


def test_record_failures_still_finish_a_country():
    record_failures = job("WNL", STATUS_PARTIAL, "2026-10-12T00:00:51Z", completed="2026-10-12T00:02:12Z", duration=81)
    assert summarize_run(RUN, [WDK_DONE, record_failures], now=NOW)["complete"] is True


def test_a_retry_job_is_labelled_retry_and_does_not_finish_its_country():
    retry = job("WNL", STATUS_SUCCEEDED, "2026-10-12T00:00:20Z", completed="2026-10-12T00:00:50Z",
                duration=30, job_type=JOB_FULL, retry_of="parent-guid")
    summary = summarize_run(RUN, [WDK_DONE, retry], now=NOW)
    assert summary["jobs"][1]["type"] == TYPE_RETRY
    assert summary["finished"] == ["WDK"]
    assert summary["complete"] is False


def test_a_running_job_makes_the_total_the_time_so_far():
    running = job("WNL", STATUS_RUNNING, "2026-10-12T01:00:00Z", beat="2026-10-12T01:28:00Z", job_type=JOB_FULL)
    summary = summarize_run(RUN, [WDK_DONE, running], now=NOW)
    assert summary["complete"] is False
    assert summary["running"] == 1
    assert summary["total_s"] == 5399
    assert summary["jobs"][1]["state"] == STATE_RUNNING
    assert summary["jobs"][1]["duration_s"] == 1800


def test_a_killed_job_ends_at_its_last_heartbeat():
    killed = job("WNL", STATUS_RUNNING, "2026-10-12T00:00:14Z", beat="2026-10-12T01:00:14Z")
    summary = summarize_run(RUN, [WDK_DONE, killed], now=NOW)
    row = summary["jobs"][1]
    assert row["state"] == STATE_STOPPED
    assert row["duration_s"] == 3600
    assert summary["stopped"] == 1
    assert summary["complete"] is False  # its message is delivered again


def test_without_a_country_count_the_run_is_complete_when_nothing_runs():
    old = "delta-20261012T000001Z"
    summary = summarize_run(old, [job("WDK", STATUS_SUCCEEDED, "2026-10-12T00:00:14Z",
                                      completed="2026-10-12T00:00:43Z", duration=29, run=old)], now=NOW)
    assert summary["expected"] is None
    assert summary["complete"] is True
    assert summary["total_s"] == 42


def test_fetch_keeps_only_the_exact_run():
    dataverse = Jobs([WDK_DONE, job("WNO", STATUS_SUCCEEDED, "x", run=RUN + "9")])
    rows = fetch_run_jobs(dataverse, RUN)
    assert [row["ts_codebase"] for row in rows] == ["WDK"]
    assert dataverse.queries[0]["filter"] == f"contains(ts_name, 'run={RUN}')"
    assert "_ts_retryofjob_value" in dataverse.queries[0]["select"]


def test_latest_run_id_reads_the_newest_job_name():
    dataverse = Jobs([WDK_DONE])
    assert latest_run_id(dataverse, "delta") == RUN
    assert dataverse.queries[0]["filter"] == "contains(ts_name, 'run=delta-')"
    assert latest_run_id(Jobs([])) is None


def test_format_duration():
    assert format_duration(42) == "42s"
    assert format_duration(245) == "4m 05s"
    assert format_duration(11520) == "3h 12m"
