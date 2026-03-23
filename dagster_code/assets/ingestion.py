"""Ingestion asset: ADLS → STAGE → Validate → Threshold → MERGE → DML Audit."""

from dagster import asset, AssetKey, Output, RetryPolicy
from dagster_code.config import TABLES, COMPOUND_PK, TZ_NOW
from dagster_code.snowflake_client import get_connection


def _merge_on_clause(t) -> str:
    """Build MERGE ON clause — handles compound PKs."""
    cpk = COMPOUND_PK.get(t.name)
    if cpk:
        return " AND ".join(f"tgt.{c}=src.{c}" for c in cpk)
    return f"tgt.{t.pk}=src.{t.pk}"


def _delete_where(t) -> str:
    """Build DELETE NOT EXISTS WHERE clause."""
    cpk = COMPOUND_PK.get(t.name)
    if cpk:
        return " AND ".join(f"src.{c}=tgt.{c}" for c in cpk)
    return f"src.{t.pk}=tgt.{t.pk}"


def _build_merge(t) -> str:
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
    where = _delete_where(t)
    return f"DELETE FROM {t.name} tgt WHERE NOT EXISTS (SELECT 1 FROM {t.name}_STAGE src WHERE {where})"


@asset(
    key=AssetKey("ingest_daily_data"),
    group_name="ingestion",
    compute_kind="snowflake",
    description="ADLS → STAGE → Validate → Threshold → MERGE into SOURCE → DML Audit",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def ingest_daily_data(context):
    conn = get_connection("SOURCE")
    try:
        cur = conn.cursor()

        # ── STEP 1: Snapshot previous counts + COPY from ADLS ──
        context.log.info("=" * 55 + "\n  STEP 1: COPY FROM ADLS INTO *_STAGE\n" + "=" * 55)
        prev = {}
        for t in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM SOURCE.{t.name}")
            prev[t.name] = cur.fetchone()[0] or 0
            context.log.info(f"  Previous SOURCE.{t.name}: {prev[t.name]:,}")

        for t in TABLES:
            cur.execute(f"TRUNCATE TABLE {t.name}_STAGE")
            cur.execute(
                f"COPY INTO {t.name}_STAGE "
                f"FROM @SOURCE.ADLS_RAW_STAGE/{t.csv_file} "
                f"FILE_FORMAT=(FORMAT_NAME=SOURCE.CSV_FORMAT)"
            )
        conn.commit()
        context.log.info("  All STAGE tables loaded from ADLS ✓")

        # ── STEP 2: Validate STAGE tables ──
        context.log.info("=" * 55 + "\n  STEP 2: VALIDATE *_STAGE TABLES\n" + "=" * 55)
        errors, stage_counts = [], {}
        for t in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM SOURCE.{t.name}_STAGE")
            c = cur.fetchone()[0] or 0
            stage_counts[t.name] = c
            if c == 0:
                errors.append(f"{t.name}_STAGE is empty after COPY")
            for col in t.key_cols:
                cur.execute(f"SELECT COUNT(*) FROM SOURCE.{t.name}_STAGE WHERE {col} IS NULL")
                n = cur.fetchone()[0] or 0
                if n > 0:
                    errors.append(f"{t.name}_STAGE.{col} has {n} NULLs")
        if errors:
            raise Exception(
                f"STAGE validation failed — SOURCE untouched. Issues: {'; '.join(errors)}"
            )
        context.log.info("  Validation passed ✓")

        # ── STEP 3: Threshold checks ──
        context.log.info("=" * 55 + "\n  STEP 3: THRESHOLD CHECKS\n" + "=" * 55)
        cur.execute(
            "SELECT TABLE_NAME, MAX_INSERT_PCT, MAX_DELETE_PCT FROM METRICS.THRESHOLD_CONFIG"
        )
        thresholds = {r[0]: {"ins": r[1], "del": r[2]} for r in cur.fetchall()}
        breaches = []
        for t in TABLES:
            old_c, new_c = prev[t.name], stage_counts[t.name]
            if old_c == 0:
                context.log.info(f"  {t.name}: initial load (no threshold check)")
                continue
            if new_c > old_c:
                pct = round((new_c - old_c) / old_c * 100, 1)
                lim = thresholds.get(t.name, {}).get("ins", 100)
                if pct > lim:
                    breaches.append(f"INSERT {t.name}: {pct}% exceeds {lim}%")
            elif new_c < old_c:
                pct = round((old_c - new_c) / old_c * 100, 1)
                lim = thresholds.get(t.name, {}).get("del", 100)
                if pct > lim:
                    breaches.append(f"DELETE {t.name}: {pct}% exceeds {lim}%")
        if breaches:
            raise Exception(
                f"Threshold breached — SOURCE untouched. {'; '.join(breaches)}"
            )
        context.log.info("  All thresholds OK ✓")

        # ── STEP 4: MERGE + DELETE sync ──
        context.log.info("=" * 55 + "\n  STEP 4: MERGE + DELETE SYNC\n" + "=" * 55)
        for t in TABLES:
            cur.execute(_build_merge(t))
            context.log.info(f"  MERGE → {t.name} ✓")
        for t in TABLES:
            cur.execute(_build_delete(t))
            context.log.info(f"  DELETE sync → {t.name} ✓")
        conn.commit()
        context.log.info("  MERGE + DELETE sync completed ✓")

        # ── STEP 5: DML audit via Snowflake streams ──
        try:
            run_id = context.run_id
            for t in TABLES:
                cur.execute(f"""
                    SELECT
                      COALESCE(SUM(IFF(METADATA$ISUPDATE,1,0))/2,0),
                      COALESCE(SUM(IFF(NOT METADATA$ISUPDATE AND METADATA$ACTION='INSERT',1,0)),0),
                      COALESCE(SUM(IFF(NOT METADATA$ISUPDATE AND METADATA$ACTION='DELETE',1,0)),0)
                    FROM SOURCE.{t.name}_CHANGES
                """)
                upd, ins, dlt = cur.fetchone()
                cur.execute(
                    f"INSERT INTO METRICS.SOURCE_DML_AUDIT VALUES(%s,%s,%s,%s,%s,{TZ_NOW})",
                    (run_id, t.name, ins, upd, dlt),
                )
            conn.commit()
            context.log.info("  DML audit written to METRICS.SOURCE_DML_AUDIT ✓")
        except Exception as e:
            context.log.warning(f"  DML audit skipped: {e}")

        return Output(None, metadata={"tables_processed": len(TABLES)})

    except Exception:
        raise
    finally:
        conn.close()