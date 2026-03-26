"""
Sensors: lightweight orchestration + minimal failure audit.

What each sensor does:
  1. log_success_to_snowflake — logs to Dagster logs only (METRICS written by assets)
  2. log_failure_to_snowflake — logs FAILURE to Snowflake DAGSTER_JOB_RUNS
     + triggers dbt retry if applicable
     (single fast INSERT — fits within 60s sensor timeout)
"""

from dagster import (
    RunStatusSensorContext,
    run_status_sensor,
    DagsterRunStatus,
    DefaultSensorStatus,
)

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=120,
)
def log_success_to_snowflake(context: RunStatusSensorContext):
    """
    Fires after ANY job succeeds.
    Just logs to Dagster — actual METRICS are written by assets:
      - Ingestion asset writes to DAGSTER_JOB_RUNS + LAYER_ROW_COUNTS + SOURCE_DML_AUDIT
      - Post-dbt asset writes to DAGSTER_JOB_RUNS + LAYER_ROW_COUNTS + DBT_MODEL_RUNS
    """
    job = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    context.log.info(f"  ✅ SUCCESS: Job '{job}' completed (Run: {run_id})")

@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=120,
)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    """
    Fires after ANY job fails. Does two things:
      1. Writes a FAILURE row to DAGSTER_JOB_RUNS in Snowflake (single fast INSERT)
      2. Triggers dbt retry job if the dbt job failed (lightweight HTTP POST)

    NOTE: Ingestion failures are ALSO logged by the ingestion asset's except block.
    This sensor catches failures from ALL jobs (dbt, post-dbt metrics, etc.)
    that don't have their own failure logging.
    """
    job = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    context.log.info(f"  ❌ FAILURE: Job '{job}' failed (Run: {run_id})")

    # ── 1. Log FAILURE to Snowflake DAGSTER_JOB_RUNS ──
    # Single INSERT — fast enough for sensor timeout (< 15 seconds)
    try:
        import os
        from dagster_code.snowflake_client import get_connection
        from dagster_code.config import TZ_NOW

        # Get error message if available
        error_msg = "Unknown error"
        try:
            if context.failure_event and context.failure_event.step_failure_data:
                error_msg = str(context.failure_event.step_failure_data.error.message)[:1000]
        except Exception:
            pass

        conn = get_connection("METRICS")
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO DAGSTER_JOB_RUNS(RUN_ID, JOB_NAME, STATUS, "
            f"START_TIME, END_TIME, ERROR_MESSAGE, LOGGED_AT) "
            f"VALUES(%s, %s, 'FAILURE', {TZ_NOW}, {TZ_NOW}, %s, {TZ_NOW})",
            (run_id, job, error_msg),
        )
        conn.commit()
        conn.close()
        context.log.info(f"  FAILURE logged to DAGSTER_JOB_RUNS ✓")
    except Exception as e:
        context.log.warning(f"  Could not log failure to Snowflake: {e}")

    # ── 2. Trigger dbt retry if dbt job failed ──
    if job == "run_dbt_cloud_job":
        try:
            import requests
            retry_id = os.getenv("DBT_RETRY_JOB_ID")
            if retry_id:
                resp = requests.post(
                    f"{os.getenv('DBT_CLOUD_HOST')}/api/v2/accounts/"
                    f"{os.getenv('DBT_CLOUD_ACCOUNT_ID')}/jobs/{retry_id}/run/",
                    headers={
                        "Authorization": f"Token {os.getenv('DBT_CLOUD_API_TOKEN')}",
                        "Content-Type": "application/json",
                    },
                    json={"cause": "Dagster+ auto-retry on dbt failure"},
                    timeout=15,
                )
                resp.raise_for_status()
                context.log.info(
                    f"  Retry triggered: dbt Run ID {resp.json()['data']['id']}"
                )
        except Exception as e:
            context.log.warning(f"  Retry error: {e}")