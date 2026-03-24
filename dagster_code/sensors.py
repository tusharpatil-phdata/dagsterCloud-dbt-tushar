"""Sensors: success/failure audit logging (lightweight — no timeout)."""

from dagster import RunStatusSensorContext, run_status_sensor, DagsterRunStatus, DefaultSensorStatus
from dagster_code.helpers.audit import write_run_to_snowflake, log_source_counts, log_all_layer_counts
from dagster_code.helpers.dbt_cloud import trigger_retry

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, default_status=DefaultSensorStatus.RUNNING)
def log_success_to_snowflake(context: RunStatusSensorContext):
    """On success: log run status to DAGSTER_JOB_RUNS. Row counts logged separately."""
    try:
        write_run_to_snowflake(context, "SUCCESS")
    except Exception as e:
        context.log.warning(f"  Audit log error: {e}")

    job = context.dagster_run.job_name
    if job == "run_ingestion_job":
        try:
            log_source_counts(context)
        except Exception as e:
            context.log.warning(f"  Source count error: {e}")
    elif job == "run_dbt_cloud_job":
        try:
            log_all_layer_counts(context)
        except Exception as e:
            context.log.warning(f"  Layer count error: {e}")
        # NOTE: fetch_and_log_dbt_results removed from sensor to avoid timeout
        # dbt model results can be viewed directly in dbt Cloud UI

@run_status_sensor(run_status=DagsterRunStatus.FAILURE, default_status=DefaultSensorStatus.RUNNING)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    """On failure: log error + auto-retry dbt if applicable."""
    err = None
    try:
        if context.failure_event and context.failure_event.step_failure_data:
            err = {"error_message": context.failure_event.step_failure_data.error.message}
    except Exception:
        pass

    try:
        write_run_to_snowflake(context, "FAILURE", err)
    except Exception as e:
        context.log.warning(f"  Audit log error: {e}")

    if context.dagster_run.job_name == "run_dbt_cloud_job":
        try:
            trigger_retry(context)
        except Exception as e:
            context.log.warning(f"  Retry trigger error: {e}")
    else:
        context.log.info("  Ingestion failed → dbt will NOT run")

'''
"""Sensors: success/failure audit logging."""

from dagster import RunStatusSensorContext, run_status_sensor, DagsterRunStatus, DefaultSensorStatus
from dagster_code.helpers.audit import write_run_to_snowflake, log_source_counts, log_all_layer_counts
from dagster_code.helpers.dbt_cloud import trigger_retry, fetch_and_log_dbt_results


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, default_status=DefaultSensorStatus.RUNNING)
def log_success_to_snowflake(context: RunStatusSensorContext):
    """On success: log run + layer-appropriate counts + dbt model results."""
    write_run_to_snowflake(context, "SUCCESS")
    job = context.dagster_run.job_name
    if job == "run_ingestion_job":
        try:
            log_source_counts(context)
        except Exception as e:
            context.log.warning(f"  Source count error: {e}")
    elif job == "run_dbt_cloud_job":
        try:
            log_all_layer_counts(context)
            fetch_and_log_dbt_results(context)
        except Exception as e:
            context.log.warning(f"  Post-dbt logging error: {e}")


@run_status_sensor(run_status=DagsterRunStatus.FAILURE, default_status=DefaultSensorStatus.RUNNING)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    """On failure: log error + auto-retry dbt if applicable."""
    err = None
    if context.failure_event and context.failure_event.step_failure_data:
        err = {"error_message": context.failure_event.step_failure_data.error.message}
    write_run_to_snowflake(context, "FAILURE", err)
    if context.dagster_run.job_name == "run_dbt_cloud_job":
        try:
            trigger_retry(context)
        except Exception as e:
            context.log.warning(f"  Retry trigger error: {e}")
    else:
        context.log.info("  Ingestion failed → dbt will NOT run (sensor will not fire)")
'''