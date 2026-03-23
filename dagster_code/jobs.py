"""Job and schedule definitions for the medallion ELT pipeline."""

from dagster import define_asset_job, AssetSelection, AssetKey, ScheduleDefinition

# ── Jobs ──
ingestion_job = define_asset_job(
    name="run_ingestion_job",
    selection=AssetSelection.keys(AssetKey("ingest_daily_data")),
    description="Run ADLS → STAGE → Validate → Threshold → MERGE into SOURCE",
)

dbt_job = define_asset_job(
    name="run_dbt_cloud_job",
    selection=AssetSelection.all() - AssetSelection.keys(AssetKey("ingest_daily_data")),
    description="Trigger dbt Cloud production job (all medallion layers)",
)

# ── Schedule ──
daily_ingestion_schedule = ScheduleDefinition(
    job=ingestion_job,
    cron_schedule="0 6 * * *",       # 6 AM UTC daily
    execution_timezone="UTC",
    description="Daily 6 AM UTC ingestion run",
)