"""Job and schedule definitions for the medallion ELT pipeline."""

from dagster import define_asset_job, AssetSelection, AssetKey, ScheduleDefinition

# ── Jobs ──
ingestion_job = define_asset_job(
    name="run_ingestion_job",
    selection=AssetSelection.keys(AssetKey("ingest_daily_data")),
    description="ADLS → STAGE → Validate → Threshold → MERGE → DML Audit → METRICS",
)

dbt_job = define_asset_job(
    name="run_dbt_cloud_job",
    selection=AssetSelection.all()
    - AssetSelection.keys(AssetKey("ingest_daily_data"))
    - AssetSelection.keys(AssetKey("log_dbt_results_to_metrics")),
    description="Trigger dbt Cloud production job (all medallion layers)",
)

post_dbt_metrics_job = define_asset_job(
    name="run_post_dbt_metrics_job",
    selection=AssetSelection.keys(AssetKey("log_dbt_results_to_metrics")),
    description="Log dbt model results + all layer row counts to METRICS",
)

# ── Schedule ──
daily_ingestion_schedule = ScheduleDefinition(
    job=ingestion_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    description="Daily 6 AM UTC ingestion run",
)