"""Dagster Definitions — entry point loaded by dagster_cloud.yaml."""

import os
from dagster import (
    Definitions,
    RunStatusSensorContext,
    run_status_sensor,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
)
from dagster_dbt import dbt_cloud_resource, load_assets_from_dbt_cloud_job

from dagster_code.assets.ingestion import ingest_daily_data
from dagster_code.jobs import ingestion_job, dbt_job, daily_ingestion_schedule
from dagster_code.sensors import log_success_to_snowflake, log_failure_to_snowflake

# ── dbt Cloud connection ──────────────────────────────
dbt_cloud_conn = dbt_cloud_resource.configured({
    "auth_token": os.getenv("DBT_CLOUD_API_TOKEN"),
    "account_id": int(os.getenv("DBT_CLOUD_ACCOUNT_ID", "0")),
    "dbt_cloud_host": os.getenv("DBT_CLOUD_HOST", "https://cloud.getdbt.com"),
})

dbt_cloud_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_conn,
    job_id=int(os.getenv("DBT_JOB_ID", "0")),
)


# ── Sensor: trigger dbt after ingestion ───────────────
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
    monitored_jobs=[ingestion_job],
    request_job=dbt_job,
    name="trigger_dbt_after_ingestion",
)
def trigger_dbt_after_ingestion(context: RunStatusSensorContext):
    """Fire dbt Cloud job only after ingestion succeeds."""
    context.log.info("  Ingestion succeeded → triggering dbt Cloud job")
    return RunRequest()


# ── Definitions (Dagster entry point) ─────────────────
defs = Definitions(
    assets=[ingest_daily_data, dbt_cloud_assets],
    jobs=[ingestion_job, dbt_job],
    schedules=[daily_ingestion_schedule],
    sensors=[
        log_success_to_snowflake,
        log_failure_to_snowflake,
        trigger_dbt_after_ingestion,
    ],
)