"""Dagster Definitions — entry point loaded by dagster_cloud.yaml.

Pipeline flow:
  1. Schedule/Manual → run_ingestion_job (ingests + logs to METRICS)
  2. Sensor → run_dbt_cloud_job (triggers dbt Cloud)
  3. Sensor → run_post_dbt_metrics_job (logs dbt results to METRICS)
  4. Alerts → Dagster+ built-in email alerts
"""

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
from dagster_code.assets.post_dbt_logging import log_dbt_results_to_metrics
from dagster_code.jobs import (
    ingestion_job,
    dbt_job,
    post_dbt_metrics_job,
    daily_ingestion_schedule,
)
from dagster_code.sensors import log_success_to_snowflake, log_failure_to_snowflake

# ── dbt Cloud connection ──
dbt_cloud_conn = dbt_cloud_resource.configured({
    "auth_token": os.getenv("DBT_CLOUD_API_TOKEN"),
    "account_id": int(os.getenv("DBT_CLOUD_ACCOUNT_ID", "0")),
    "dbt_cloud_host": os.getenv("DBT_CLOUD_HOST", "https://cloud.getdbt.com"),
})

dbt_cloud_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_conn,
    job_id=int(os.getenv("DBT_JOB_ID", "0")),
)

# ── Sensor: trigger dbt after ingestion succeeds ──
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
    monitored_jobs=[ingestion_job],
    request_job=dbt_job,
    name="trigger_dbt_after_ingestion",
)
def trigger_dbt_after_ingestion(context: RunStatusSensorContext):
    context.log.info("  Ingestion succeeded → triggering dbt Cloud job")
    return RunRequest()

# ── Sensor: trigger post-dbt metrics logging after dbt succeeds ──
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
    monitored_jobs=[dbt_job],
    request_job=post_dbt_metrics_job,
    name="trigger_metrics_after_dbt",
)
def trigger_metrics_after_dbt(context: RunStatusSensorContext):
    context.log.info("  dbt succeeded → logging metrics to Snowflake")
    return RunRequest()

# ── Definitions ──
defs = Definitions(
    assets=[ingest_daily_data, dbt_cloud_assets, log_dbt_results_to_metrics],
    jobs=[ingestion_job, dbt_job, post_dbt_metrics_job],
    schedules=[daily_ingestion_schedule],
    sensors=[
        log_success_to_snowflake,
        log_failure_to_snowflake,
        trigger_dbt_after_ingestion,
        trigger_metrics_after_dbt,
    ],
)