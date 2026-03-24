"""Sensors: lightweight orchestration only — no Snowflake calls.
   All heavy work (audit, row counts, dbt results) is done inside assets."""

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
    """Log success info to Dagster logs only — actual METRICS written by assets."""
    job = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    context.log.info(f"  ✅ SUCCESS: Job '{job}' completed (Run: {run_id})")

@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=120,
)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    """Log failure + trigger dbt retry (lightweight HTTP call only)."""
    job = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    context.log.info(f"  ❌ FAILURE: Job '{job}' failed (Run: {run_id})")

    if job == "run_dbt_cloud_job":
        try:
            import os
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