"""Post-dbt asset: logs dbt model results + LZ/STAGING/DBO row counts to METRICS.
   SOURCE counts are already logged by the ingestion asset — no duplication."""

import os
import requests
from dagster import asset, AssetKey, Output
from dagster_code.config import DBT_LAYERS, TZ_NOW
from dagster_code.snowflake_client import get_connection

@asset(
    key=AssetKey("log_dbt_results_to_metrics"),
    group_name="audit",
    compute_kind="snowflake",
    description="Log dbt model results + LZ/STAGING/DBO row counts (with delta) to METRICS",
)
def log_dbt_results_to_metrics(context):
    conn = get_connection("METRICS")
    try:
        cur = conn.cursor()
        run_id = context.run_id

        # ── 1. Log dbt job run ──
        context.log.info("=" * 55 + "\n  LOG DBT JOB RUN\n" + "=" * 55)
        cur.execute(
            f"INSERT INTO DAGSTER_JOB_RUNS(RUN_ID, JOB_NAME, STATUS, "
            f"START_TIME, END_TIME, ERROR_MESSAGE, LOGGED_AT) "
            f"VALUES(%s, 'run_dbt_cloud_job', 'SUCCESS', "
            f"{TZ_NOW}, {TZ_NOW}, NULL, {TZ_NOW})",
            (run_id,),
        )
        context.log.info("  dbt job run → DAGSTER_JOB_RUNS ✓")

        # ── 2. Log LZ / STAGING / DBO row counts (with delta from previous run) ──
        context.log.info("=" * 55 + "\n  LOG LZ / STAGING / DBO ROW COUNTS\n" + "=" * 55)
        for schema, tbl in DBT_LAYERS:
            # Current count
            try:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{tbl}")
                rows_after = cur.fetchone()[0]
            except Exception:
                rows_after = -1

            # Previous count from last run
            cur.execute(
                "SELECT ROWS_AFTER FROM LAYER_ROW_COUNTS "
                "WHERE SCHEMA_NAME = %s AND TABLE_NAME = %s "
                "ORDER BY LOGGED_AT DESC LIMIT 1",
                (schema, tbl),
            )
            prev = cur.fetchone()
            rows_before = prev[0] if prev else 0
            rows_added = rows_after - rows_before

            cur.execute(
                f"INSERT INTO LAYER_ROW_COUNTS(DAGSTER_RUN_ID, SCHEMA_NAME, TABLE_NAME, "
                f"ROWS_BEFORE, ROWS_AFTER, ROWS_ADDED, LOGGED_AT) "
                f"VALUES(%s, %s, %s, %s, %s, %s, {TZ_NOW})",
                (run_id, schema, tbl, rows_before, rows_after, rows_added),
            )

            # Log meaningful delta
            if rows_before == 0 and rows_after > 0:
                status = "INITIAL LOAD"
            elif rows_added > 0:
                status = f"+{rows_added:,} new"
            elif rows_added == 0:
                status = "no change"
            else:
                status = f"{rows_added:,} removed"
            context.log.info(f"  {schema}.{tbl}: {rows_before:,} → {rows_after:,} ({status})")

        # ── 3. Fetch + log dbt Cloud model results ──
        context.log.info("=" * 55 + "\n  FETCH DBT MODEL RESULTS\n" + "=" * 55)
        try:
            host = os.getenv("DBT_CLOUD_HOST")
            acct = os.getenv("DBT_CLOUD_ACCOUNT_ID")
            token = os.getenv("DBT_CLOUD_API_TOKEN")
            job_id = os.getenv("DBT_JOB_ID")
            headers = {"Authorization": f"Token {token}"}

            runs_resp = requests.get(
                f"{host}/api/v2/accounts/{acct}/runs/"
                f"?job_definition_id={job_id}&order_by=-id&limit=1",
                headers=headers, timeout=20,
            )
            runs_resp.raise_for_status()
            dbt_run_id = runs_resp.json()["data"][0]["id"]

            arts_resp = requests.get(
                f"{host}/api/v2/accounts/{acct}/runs/{dbt_run_id}/artifacts/run_results.json",
                headers=headers, timeout=20,
            )
            arts_resp.raise_for_status()
            results = arts_resp.json()["results"]

            for r in results:
                rows = r.get("adapter_response", {}).get("rows_affected")
                cur.execute(
                    f"INSERT INTO DBT_MODEL_RUNS(DAGSTER_RUN_ID, DBT_CLOUD_RUN_ID, MODEL_NAME, "
                    f"STATUS, ROWS_AFFECTED, EXECUTION_TIME, LOGGED_AT) "
                    f"VALUES(%s, %s, %s, %s, %s, %s, {TZ_NOW})",
                    (run_id, dbt_run_id, r["unique_id"],
                     r["status"], rows, round(r["execution_time"], 2)),
                )

            passed = sum(1 for r in results if r["status"] in ("success", "pass"))
            failed = sum(1 for r in results if r["status"] == "error")
            context.log.info(f"  dbt: {passed} passed, {failed} failed / {len(results)} total → DBT_MODEL_RUNS ✓")
        except Exception as e:
            context.log.warning(f"  dbt results fetch error (non-fatal): {e}")

        conn.commit()
        context.log.info("  All METRICS logged successfully ✓")
        return Output(None, metadata={"dbt_layers_logged": len(DBT_LAYERS)})

    except Exception as e:
        context.log.error(f"  METRICS logging error: {e}")
        raise
    finally:
        conn.close()