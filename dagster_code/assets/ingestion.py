"""
Ingestion asset: ADLS → STAGE → Validate → Threshold → MERGE → DML Audit → METRICS.

This is the FIRST step in the pipeline. It:
  1. COPYs CSV files from ADLS Gen2 into Snowflake *_STAGE tables
  2. Validates STAGE data (non-empty, key columns not NULL)
  3. Checks row-count thresholds (prevents bad data from overwriting SOURCE)
  4. MERGEs validated STAGE data into SOURCE tables (upsert + delete sync)
  5. Records per-table INSERT/UPDATE/DELETE counts from Snowflake streams
  6. Logs the job run + SOURCE row counts (with delta) to METRICS tables

If validation or thresholds fail → SOURCE stays untouched, dbt will NOT run.
"""

from dagster import asset, AssetKey, Output, RetryPolicy
from dagster_code.config import TABLES, COMPOUND_PK, TZ_NOW
from dagster_code.snowflake_client import get_connection

# ═══════════════════════════════════════════════════════
# SQL BUILDERS
# ═══════════════════════════════════════════════════════
# These functions generate MERGE and DELETE SQL dynamically from TableSpec.
# No hardcoded SQL blocks — adding a new table only requires a new TableSpec in config.py.

def _merge_on_clause(t) -> str:
    """
    Build the ON clause for MERGE.
    Most tables use a single PK (e.g. ON tgt.ID = src.ID).
    SUPPLY uses compound PK (ON tgt.ID = src.ID AND tgt.SKU = src.SKU).
    """
    cpk = COMPOUND_PK.get(t.name)
    if cpk:
        return " AND ".join(f"tgt.{c}=src.{c}" for c in cpk)
    return f"tgt.{t.pk}=src.{t.pk}"

def _delete_where(t) -> str:
    """
    Build the WHERE clause for DELETE sync.
    Same PK logic as MERGE — ensures delete uses the correct key columns.
    """
    cpk = COMPOUND_PK.get(t.name)
    if cpk:
        return " AND ".join(f"src.{c}=tgt.{c}" for c in cpk)
    return f"src.{t.pk}=tgt.{t.pk}"

def _build_merge(t) -> str:
    """
    Generate a complete MERGE statement from TableSpec.

    Example output for CUSTOMER:
      MERGE INTO CUSTOMER AS tgt USING CUSTOMER_STAGE AS src ON tgt.ID=src.ID
      WHEN MATCHED THEN UPDATE SET tgt.NAME=src.NAME
      WHEN NOT MATCHED THEN INSERT (ID,NAME) VALUES (src.ID,src.NAME)
    """
    on = _merge_on_clause(t)
    sets = ", ".join(f"tgt.{c}=src.{c}" for c in t.merge_set)
    cols = ", ".join(t.all_cols)
    vals = ", ".join(f"src.{c}" for c in t.all_cols)
    return (
        f"MERGE INTO {t.name} AS tgt USING {t.name}_STAGE AS src ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {sets} "
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
    )

def _build_delete(t) -> str:
    """
    Generate DELETE sync statement.
    Removes rows from SOURCE that no longer exist in STAGE.
    This ensures SOURCE mirrors the ADLS CSV data exactly (full-refresh sync).

    Example output for CUSTOMER:
      DELETE FROM CUSTOMER tgt WHERE NOT EXISTS
        (SELECT 1 FROM CUSTOMER_STAGE src WHERE src.ID=tgt.ID)
    """
    where = _delete_where(t)
    return (
        f"DELETE FROM {t.name} tgt "
        f"WHERE NOT EXISTS (SELECT 1 FROM {t.name}_STAGE src WHERE {where})"
    )

# ═══════════════════════════════════════════════════════
# MAIN INGESTION ASSET
# ═══════════════════════════════════════════════════════

@asset(
    key=AssetKey("ingest_daily_data"),
    group_name="ingestion",
    compute_kind="snowflake",
    description="ADLS → STAGE → Validate → Threshold → MERGE → DML Audit → Log to METRICS",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def ingest_daily_data(context):
    """
    Full ingestion pipeline — runs as a single Dagster asset.

    On success:
      - SOURCE tables are updated with latest ADLS data
      - METRICS tables have DML audit + job run + row counts
      - Sensor triggers dbt Cloud job next

    On failure:
      - SOURCE tables remain untouched (last good version)
      - Sensor does NOT trigger dbt (no bad data flows downstream)
      - Dagster+ email alert fires (configured in Dagster+ UI)
    """
    conn = get_connection("SOURCE")
    try:
        cur = conn.cursor()

        # ════════════════════════════════════════════════
        # STEP 1: Snapshot previous SOURCE counts + COPY from ADLS
        # ════════════════════════════════════════════════
        # WHY: We capture BEFORE counts so we can:
        #   - Calculate deltas in Step 3 (threshold check)
        #   - Log meaningful before→after in Step 6 (METRICS)
        # THEN: Truncate each *_STAGE table and COPY fresh data from ADLS.
        context.log.info("=" * 55 + "\n  STEP 1: COPY FROM ADLS INTO *_STAGE\n" + "=" * 55)

        # Capture current SOURCE counts (these become ROWS_BEFORE in METRICS)
        prev = {}
        for t in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM SOURCE.{t.name}")
            prev[t.name] = cur.fetchone()[0] or 0
            context.log.info(f"  Previous SOURCE.{t.name}: {prev[t.name]:,}")

        # Truncate STAGE tables (clean slate before each COPY)
        for t in TABLES:
            cur.execute(f"TRUNCATE TABLE {t.name}_STAGE")

        # COPY from ADLS external stage into *_STAGE tables
        for t in TABLES:
            cur.execute(
                f"COPY INTO {t.name}_STAGE "
                f"FROM @SOURCE.ADLS_RAW_STAGE/{t.csv_file} "
                f"FILE_FORMAT=(FORMAT_NAME=SOURCE.CSV_FORMAT)"
            )
            context.log.info(f"  COPY → {t.name}_STAGE ✓")

        conn.commit()
        context.log.info("  All STAGE tables loaded from ADLS ✓")

        # ════════════════════════════════════════════════
        # STEP 2: Validate STAGE tables
        # ════════════════════════════════════════════════
        # WHY: Prevents bad data from reaching SOURCE.
        # Two checks per table:
        #   a) Table is not empty (COPY actually loaded data)
        #   b) Key columns have no NULLs (data quality gate)
        # If ANY check fails → Exception → SOURCE untouched → dbt won't run.
        context.log.info("=" * 55 + "\n  STEP 2: VALIDATE *_STAGE TABLES\n" + "=" * 55)

        errors, stage_counts = [], {}
        for t in TABLES:
            # Check a: table must not be empty after COPY
            cur.execute(f"SELECT COUNT(*) FROM SOURCE.{t.name}_STAGE")
            c = cur.fetchone()[0] or 0
            stage_counts[t.name] = c
            context.log.info(f"  {t.name}_STAGE: {c:,} rows")
            if c == 0:
                errors.append(f"{t.name}_STAGE is empty after COPY")

            # Check b: key columns must not have NULLs
            for col in t.key_cols:
                cur.execute(
                    f"SELECT COUNT(*) FROM SOURCE.{t.name}_STAGE WHERE {col} IS NULL"
                )
                n = cur.fetchone()[0] or 0
                if n > 0:
                    errors.append(f"{t.name}_STAGE.{col} has {n} NULLs")

        if errors:
            raise Exception(
                f"STAGE validation failed — SOURCE untouched. Issues: {'; '.join(errors)}"
            )
        context.log.info("  Validation passed ✓")

        # ════════════════════════════════════════════════
        # STEP 3: Threshold checks (anomaly detection)
        # ════════════════════════════════════════════════
        # WHY: Prevents catastrophic data issues like:
        #   - A corrupted/empty CSV wiping out SOURCE (delete threshold)
        #   - A duplicated CSV suddenly doubling row counts (insert threshold)
        # HOW: Compares STAGE counts vs previous SOURCE counts using
        #      configurable % limits from METRICS.THRESHOLD_CONFIG.
        # If any threshold is breached → Exception → SOURCE untouched.
        context.log.info("=" * 55 + "\n  STEP 3: THRESHOLD CHECKS\n" + "=" * 55)

        # Load threshold config from Snowflake
        cur.execute(
            "SELECT TABLE_NAME, MAX_INSERT_PCT, MAX_DELETE_PCT "
            "FROM METRICS.THRESHOLD_CONFIG"
        )
        thresholds = {r[0]: {"ins": r[1], "del": r[2]} for r in cur.fetchall()}

        breaches = []
        for t in TABLES:
            old_c, new_c = prev[t.name], stage_counts[t.name]

            # Skip threshold check on initial load (no previous data to compare against)
            if old_c == 0:
                context.log.info(f"  {t.name}: initial load (no threshold check)")
                continue

            if new_c > old_c:
                # More rows than before → check insert % threshold
                pct = round((new_c - old_c) / old_c * 100, 1)
                lim = thresholds.get(t.name, {}).get("ins", 100)
                context.log.info(f"  {t.name}: {old_c:,} → {new_c:,} (+{pct}%) [limit: {lim}%]")
                if pct > lim:
                    breaches.append(f"INSERT {t.name}: {pct}% exceeds {lim}%")

            elif new_c < old_c:
                # Fewer rows than before → check delete % threshold
                pct = round((old_c - new_c) / old_c * 100, 1)
                lim = thresholds.get(t.name, {}).get("del", 100)
                context.log.info(f"  {t.name}: {old_c:,} → {new_c:,} (-{pct}%) [limit: {lim}%]")
                if pct > lim:
                    breaches.append(f"DELETE {t.name}: {pct}% exceeds {lim}%")

            else:
                context.log.info(f"  {t.name}: {old_c:,} → {new_c:,} (no change)")

        if breaches:
            raise Exception(
                f"Threshold breached — SOURCE untouched. {'; '.join(breaches)}"
            )
        context.log.info("  All thresholds OK ✓")

        # ════════════════════════════════════════════════
        # STEP 4: MERGE + DELETE sync
        # ════════════════════════════════════════════════
        # WHY: Applies the validated STAGE data to SOURCE tables.
        # MERGE: Upserts — inserts new rows, updates existing rows.
        # DELETE: Removes rows from SOURCE that no longer exist in STAGE.
        #   Together this gives a full-refresh sync (SOURCE mirrors ADLS exactly).
        # SQL is generated dynamically from TableSpec — no copy-pasted SQL blocks.
        context.log.info("=" * 55 + "\n  STEP 4: MERGE + DELETE SYNC\n" + "=" * 55)

        # Run MERGE for all tables first
        for t in TABLES:
            cur.execute(_build_merge(t))
            context.log.info(f"  MERGE → {t.name} ✓")

        # Then run DELETE sync for all tables
        for t in TABLES:
            cur.execute(_build_delete(t))
            context.log.info(f"  DELETE sync → {t.name} ✓")

        conn.commit()
        context.log.info("  MERGE + DELETE sync completed ✓")

        # ════════════════════════════════════════════════
        # STEP 5: DML audit via Snowflake streams
        # ════════════════════════════════════════════════
        # WHY: Provides a per-table, per-run audit trail of exactly what changed.
        # HOW: Each SOURCE table has a Snowflake STREAM (*_CHANGES) that captures
        #      row-level changes since the last stream read.
        #      We query each stream for INSERT/UPDATE/DELETE counts and log them
        #      to METRICS.SOURCE_DML_AUDIT.
        # NOTE: Reading a stream consumes it — counts reset after each read.
        #       This is by design (each run gets its own audit entry).
        context.log.info("=" * 55 + "\n  STEP 5: DML AUDIT\n" + "=" * 55)

        try:
            run_id = context.run_id
            for t in TABLES:
                # Query the stream for change counts
                # METADATA$ISUPDATE: true for UPDATE (appears as DELETE+INSERT pair)
                # METADATA$ACTION: 'INSERT' or 'DELETE' for pure inserts/deletes
                # UPDATE count is divided by 2 because each UPDATE = 1 DELETE + 1 INSERT
                cur.execute(f"""
                    SELECT
                      COALESCE(SUM(IFF(METADATA$ISUPDATE,1,0))/2,0),
                      COALESCE(SUM(IFF(NOT METADATA$ISUPDATE AND METADATA$ACTION='INSERT',1,0)),0),
                      COALESCE(SUM(IFF(NOT METADATA$ISUPDATE AND METADATA$ACTION='DELETE',1,0)),0)
                    FROM SOURCE.{t.name}_CHANGES
                """)
                upd, ins, dlt = cur.fetchone()

                # Write to METRICS.SOURCE_DML_AUDIT
                cur.execute(
                    f"INSERT INTO METRICS.SOURCE_DML_AUDIT VALUES(%s,%s,%s,%s,%s,{TZ_NOW})",
                    (run_id, t.name, ins, upd, dlt),
                )
                context.log.info(
                    f"  {t.name}: +{int(ins)} inserted, ~{int(upd)} updated, -{int(dlt)} deleted"
                )

            conn.commit()
            context.log.info("  DML audit → METRICS.SOURCE_DML_AUDIT ✓")
        except Exception as e:
            # Non-fatal — DML audit failure should not block the pipeline
            context.log.warning(f"  DML audit skipped: {e}")

        # ════════════════════════════════════════════════
        # STEP 6: Log job run + SOURCE row counts to METRICS
        # ════════════════════════════════════════════════
        # WHY: Creates a queryable audit trail in Snowflake.
        # WHAT:
        #   DAGSTER_JOB_RUNS — one row per job execution (run_id, job name, status)
        #   LAYER_ROW_COUNTS — one row per SOURCE table with before/after/delta
        #
        # IMPORTANT:
        #   - Only SOURCE counts are logged here
        #   - LZ/STAGING/DBO counts are logged by the post-dbt asset
        #     (log_dbt_results_to_metrics) to avoid duplicate entries
        #   - The `prev` dict from Step 1 provides accurate ROWS_BEFORE values
        context.log.info("=" * 55 + "\n  STEP 6: LOG RUN + SOURCE COUNTS TO METRICS\n" + "=" * 55)

        try:
            run_id = context.run_id

            # Log the job run itself to DAGSTER_JOB_RUNS
            cur.execute(
                f"INSERT INTO METRICS.DAGSTER_JOB_RUNS(RUN_ID, JOB_NAME, STATUS, "
                f"START_TIME, END_TIME, ERROR_MESSAGE, LOGGED_AT) "
                f"VALUES(%s, 'run_ingestion_job', 'SUCCESS', "
                f"{TZ_NOW}, {TZ_NOW}, NULL, {TZ_NOW})",
                (run_id,),
            )

            # Log per-table SOURCE row counts with before/after delta
            for t in TABLES:
                cur.execute(f"SELECT COUNT(*) FROM SOURCE.{t.name}")
                after = cur.fetchone()[0]
                before = prev[t.name]  # captured in Step 1
                added = after - before

                # Human-readable status for Dagster logs
                if before == 0 and after > 0:
                    status = "INITIAL LOAD"
                elif added > 0:
                    status = f"+{added:,} new"
                elif added == 0:
                    status = "no change"
                else:
                    status = f"{added:,} removed"

                # Write to METRICS.LAYER_ROW_COUNTS
                cur.execute(
                    f"INSERT INTO METRICS.LAYER_ROW_COUNTS(DAGSTER_RUN_ID, SCHEMA_NAME, "
                    f"TABLE_NAME, ROWS_BEFORE, ROWS_AFTER, ROWS_ADDED, LOGGED_AT) "
                    f"VALUES(%s, 'SOURCE', %s, %s, %s, %s, {TZ_NOW})",
                    (run_id, t.name, before, after, added),
                )
                context.log.info(f"  SOURCE.{t.name}: {before:,} → {after:,} ({status})")

            conn.commit()
            context.log.info("  Job run + source counts → METRICS ✓")
        except Exception as e:
            # Non-fatal — METRICS failure should not block the pipeline
            context.log.warning(f"  METRICS logging error: {e}")

        # ════════════════════════════════════════════════
        # RETURN: Output with metadata (visible in Dagster UI)
        # ════════════════════════════════════════════════
        return Output(None, metadata={
            "tables_processed": len(TABLES),
            "run_id": context.run_id,
        })

    except Exception as e:
        # ── Log FAILURE to METRICS before re-raising ──
        # This ensures DAGSTER_JOB_RUNS has a record even when ingestion fails.
        # Without this, only SUCCESS runs are audited — failures would be invisible
        # in Snowflake (only visible in Dagster UI).
        try:
            fail_conn = get_connection("METRICS")
            fail_cur = fail_conn.cursor()
            fail_cur.execute(
                f"INSERT INTO DAGSTER_JOB_RUNS(RUN_ID, JOB_NAME, STATUS, "
                f"START_TIME, END_TIME, ERROR_MESSAGE, LOGGED_AT) "
                f"VALUES(%s, 'run_ingestion_job', 'FAILURE', "
                f"{TZ_NOW}, {TZ_NOW}, %s, {TZ_NOW})",
                (context.run_id, str(e)[:1000]),
            )
            fail_conn.commit()
            fail_conn.close()
            context.log.info("  FAILURE logged to DAGSTER_JOB_RUNS")
        except Exception as log_err:
            context.log.warning(f"  Could not log failure: {log_err}")
        # Re-raise so Dagster marks the run as FAILED
        # This prevents the dbt sensor from firing
        raise
    finally:
        # ALWAYS close the main connection — even on error
        conn.close()