"""Audit helpers: write Dagster run status, log row counts."""

import json
from dagster import RunStatusSensorContext
from dagster_code.config import TZ_NOW, ALL_LAYERS, TABLES
from dagster_code.snowflake_client import get_connection


def write_run_to_snowflake(context: RunStatusSensorContext, status: str, error_msg=None):
    """Log a Dagster run to METRICS.DAGSTER_JOB_RUNS."""
    conn = get_connection("METRICS")
    try:
        cur = conn.cursor()
        stats = context.instance.get_run_stats(context.dagster_run.run_id)
        cur.execute(
            f"INSERT INTO DAGSTER_JOB_RUNS VALUES(%s,%s,%s,"
            f"CONVERT_TIMEZONE('UTC','Asia/Kolkata',TO_TIMESTAMP_NTZ(%s)),"
            f"CONVERT_TIMEZONE('UTC','Asia/Kolkata',TO_TIMESTAMP_NTZ(%s)),"
            f"%s,{TZ_NOW})",
            (context.dagster_run.run_id, context.dagster_run.job_name, status,
             stats.start_time, stats.end_time,
             json.dumps(error_msg) if error_msg else None),
        )
        conn.commit()
        context.log.info(f"  Logged {status} → DAGSTER_JOB_RUNS")
    except Exception as e:
        context.log.error(f"  Audit log failed: {e}")
    finally:
        conn.close()


def log_source_counts(context: RunStatusSensorContext):
    """Log SOURCE table row counts after ingestion."""
    conn = get_connection("SOURCE")
    try:
        cur = conn.cursor()
        run_id = context.dagster_run.run_id
        for t in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM SOURCE.{t.name}")
            cnt = cur.fetchone()[0]
            cur.execute(
                f"INSERT INTO METRICS.LAYER_ROW_COUNTS VALUES(%s,'SOURCE',%s,0,%s,%s,{TZ_NOW})",
                (run_id, t.name, cnt, cnt),
            )
        conn.commit()
        context.log.info("  SOURCE counts → LAYER_ROW_COUNTS ✓")
    except Exception as e:
        context.log.error(f"  Source count error: {e}")
    finally:
        conn.close()


def log_all_layer_counts(context: RunStatusSensorContext):
    """Log row counts across all medallion layers after dbt."""
    conn = get_connection("SOURCE")
    try:
        cur = conn.cursor()
        run_id = context.dagster_run.run_id
        for schema, tbl in ALL_LAYERS:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{tbl}")
                cnt = cur.fetchone()[0]
            except Exception:
                cnt = -1  # table may not exist yet on first run
            cur.execute(
                f"INSERT INTO METRICS.LAYER_ROW_COUNTS VALUES(%s,%s,%s,0,%s,%s,{TZ_NOW})",
                (run_id, schema, tbl, cnt, cnt),
            )
        conn.commit()
        context.log.info("  All layer counts → LAYER_ROW_COUNTS ✓")
    except Exception as e:
        context.log.error(f"  Layer count error: {e}")
    finally:
        conn.close()