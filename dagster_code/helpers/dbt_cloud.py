"""dbt Cloud helpers: trigger retry job, fetch and log per-model results."""

import os
import requests
from dagster import RunStatusSensorContext
from dagster_code.config import TZ_NOW
from dagster_code.snowflake_client import get_connection


def trigger_retry(context: RunStatusSensorContext):
    """Trigger dagsterCloud-dbt-retry-job (failed models only)."""
    retry_id = os.getenv("DBT_RETRY_JOB_ID")
    if not retry_id:
        context.log.warning("  DBT_RETRY_JOB_ID not set — skipping retry")
        return
    resp = requests.post(
        f"{os.getenv('DBT_CLOUD_HOST')}/api/v2/accounts/{os.getenv('DBT_CLOUD_ACCOUNT_ID')}/jobs/{retry_id}/run/",
        headers={"Authorization": f"Token {os.getenv('DBT_CLOUD_API_TOKEN')}", "Content-Type": "application/json"},
        json={"cause": "Dagster+ auto-retry on dbt failure"},
        timeout=30,
    )
    resp.raise_for_status()
    context.log.info(f"  Retry triggered → dbt Cloud Run ID: {resp.json()['data']['id']}")


def fetch_and_log_dbt_results(context: RunStatusSensorContext):
    """Fetch per-model results from latest dbt Cloud run → log to DBT_MODEL_RUNS."""
    host, acct, token, job_id = (
        os.getenv("DBT_CLOUD_HOST"), os.getenv("DBT_CLOUD_ACCOUNT_ID"),
        os.getenv("DBT_CLOUD_API_TOKEN"), os.getenv("DBT_JOB_ID"),
    )
    if not all([host, acct, token, job_id]):
        context.log.warning("  dbt Cloud env vars incomplete — skipping model logging")
        return

    headers = {"Authorization": f"Token {token}"}
    runs = requests.get(
        f"{host}/api/v2/accounts/{acct}/runs/?job_definition_id={job_id}&order_by=-id&limit=1",
        headers=headers, timeout=20,
    )
    runs.raise_for_status()
    dbt_run_id = runs.json()["data"][0]["id"]

    arts = requests.get(
        f"{host}/api/v2/accounts/{acct}/runs/{dbt_run_id}/artifacts/run_results.json",
        headers=headers, timeout=20,
    )
    arts.raise_for_status()
    results = arts.json()["results"]

    conn = get_connection("METRICS")
    try:
        cur = conn.cursor()
        dag_run_id = context.dagster_run.run_id
        for r in results:
            rows = r.get("adapter_response", {}).get("rows_affected")
            cur.execute(
                f"INSERT INTO DBT_MODEL_RUNS VALUES(%s,%s,%s,%s,%s,%s,{TZ_NOW})",
                (dag_run_id, dbt_run_id, r["unique_id"],
                 r["status"], rows, round(r["execution_time"], 2)),
            )
        conn.commit()
        passed = sum(1 for r in results if r["status"] in ("success", "pass"))
        failed = sum(1 for r in results if r["status"] == "error")
        context.log.info(f"  dbt results: {passed} passed, {failed} failed / {len(results)} total → DBT_MODEL_RUNS ✓")
    except Exception as e:
        context.log.error(f"  dbt results log error: {e}")
    finally:
        conn.close()