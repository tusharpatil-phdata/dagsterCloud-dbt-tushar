"""
Centralized configuration for the medallion ELT pipeline.

This file is the SINGLE SOURCE OF TRUTH for:
  - Database name
  - Timezone conversion SQL snippet
  - Table registry (TableSpec) — defines every SOURCE table and its metadata
  - Compound primary keys — for tables with multi-column MERGE ON
  - DBT_LAYERS — LZ/STAGING/DBO tables logged by the post-dbt metrics asset
    (SOURCE tables are logged separately by the ingestion asset — no duplicates)
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple

# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════
# Single database — all schemas (SOURCE, LZ, STAGING, DBO, METRICS) live here.
DB = "DAGSTER_DBT_KIEWIT_DB_CLOUD"

# ═══════════════════════════════════════════════════════
# TIMEZONE
# ═══════════════════════════════════════════════════════
# SQL snippet reused in every INSERT INTO METRICS.*
# Converts Snowflake server time (UTC) to IST for consistent audit timestamps.
TZ_NOW = "CONVERT_TIMEZONE('America/Los_Angeles','Asia/Kolkata',CURRENT_TIMESTAMP())"

# ═══════════════════════════════════════════════════════
# TABLE REGISTRY (TableSpec)
# ═══════════════════════════════════════════════════════
# Each TableSpec defines ONE source table and everything needed to:
#   - COPY from ADLS into *_STAGE
#   - VALIDATE (non-null key columns)
#   - MERGE from *_STAGE into SOURCE
#   - DELETE sync (remove rows from SOURCE missing in STAGE)
#   - DML audit via Snowflake streams
#
# Adding a new CSV? Just add one TableSpec here — ingestion.py picks it up automatically.

@dataclass(frozen=True)
class TableSpec:
    """Metadata for one SOURCE table used throughout the ingestion pipeline."""

    name: str
    # Snowflake table name in SOURCE schema (e.g. "CUSTOMER")

    csv_file: str
    # Corresponding CSV file name in ADLS raw_data/ folder (e.g. "customer.csv")

    pk: str
    # Primary key column used in MERGE ON clause (e.g. "ID" or "SKU")
    # For tables with compound PKs, see COMPOUND_PK below

    key_cols: List[str]
    # Columns validated for NULLs after COPY into STAGE
    # If any of these are NULL, the entire ingestion fails (SOURCE untouched)

    merge_set: List[str]
    # Columns updated in MERGE WHEN MATCHED THEN UPDATE SET
    # Excludes the PK column(s) — those are used in the ON clause

    all_cols: List[str]
    # All columns used in MERGE WHEN NOT MATCHED THEN INSERT
    # Includes PK + all other columns

# ─── Register all 8 source tables ─────────────────────
TABLES: List[TableSpec] = [

    # CUSTOMER — 2 columns, simple PK on ID
    TableSpec(
        name="CUSTOMER",
        csv_file="customer.csv",
        pk="ID",
        key_cols=["ID", "NAME"],
        merge_set=["NAME"],
        all_cols=["ID", "NAME"],
    ),

    # ORDER_DETAIL — 7 columns, order headers with financials
    TableSpec(
        name="ORDER_DETAIL",
        csv_file="order_detail.csv",
        pk="ID",
        key_cols=["ID", "CUSTOMER", "STORE_ID"],
        merge_set=["CUSTOMER", "ORDERED_AT", "STORE_ID", "SUBTOTAL", "TAX_PAID", "ORDER_TOTAL"],
        all_cols=["ID", "CUSTOMER", "ORDERED_AT", "STORE_ID", "SUBTOTAL", "TAX_PAID", "ORDER_TOTAL"],
    ),

    # ORDER_ITEM — 3 columns, line items per order
    TableSpec(
        name="ORDER_ITEM",
        csv_file="order_item.csv",
        pk="ID",
        key_cols=["ID", "ORDER_ID", "SKU"],
        merge_set=["ORDER_ID", "SKU"],
        all_cols=["ID", "ORDER_ID", "SKU"],
    ),

    # PRODUCT — 5 columns, PK is SKU (not ID)
    TableSpec(
        name="PRODUCT",
        csv_file="product.csv",
        pk="SKU",
        key_cols=["SKU", "NAME"],
        merge_set=["NAME", "TYPE", "PRICE", "DESCRIPTION"],
        all_cols=["SKU", "NAME", "TYPE", "PRICE", "DESCRIPTION"],
    ),

    # STORE — 4 columns
    TableSpec(
        name="STORE",
        csv_file="store.csv",
        pk="ID",
        key_cols=["ID", "NAME"],
        merge_set=["NAME", "OPENED_AT", "TAX_RATE"],
        all_cols=["ID", "NAME", "OPENED_AT", "TAX_RATE"],
    ),

    # SUPPLY — 5 columns, compound PK (ID + SKU) — see COMPOUND_PK below
    TableSpec(
        name="SUPPLY",
        csv_file="supply.csv",
        pk="ID",
        key_cols=["ID", "SKU", "NAME"],
        merge_set=["NAME", "COST", "PERISHABLE"],
        all_cols=["ID", "NAME", "COST", "PERISHABLE", "SKU"],
    ),

    # DISCOUNT — 5 columns, discount periods per customer
    TableSpec(
        name="DISCOUNT",
        csv_file="discounts.csv",
        pk="ID",
        key_cols=["ID", "CUSTOMER_ID"],
        merge_set=["CUSTOMER_ID", "PERCENT", "VALID_FROM", "VALID_TO"],
        all_cols=["ID", "CUSTOMER_ID", "PERCENT", "VALID_FROM", "VALID_TO"],
    ),

    # LOYALTY_POINTS — 6 columns, points balance per customer
    TableSpec(
        name="LOYALTY_POINTS",
        csv_file="loyalty_points.csv",
        pk="ID",
        key_cols=["ID", "CUSTOMER_ID"],
        merge_set=["CUSTOMER_ID", "POINTS_BALANCE", "POINTS_EARNED", "POINTS_REDEEMED", "AS_OF_DATE"],
        all_cols=["ID", "CUSTOMER_ID", "POINTS_BALANCE", "POINTS_EARNED", "POINTS_REDEEMED", "AS_OF_DATE"],
    ),
]

# ═══════════════════════════════════════════════════════
# COMPOUND PRIMARY KEYS
# ═══════════════════════════════════════════════════════
# Some tables need multi-column MERGE ON and DELETE WHERE clauses.
# If a table name is listed here, MERGE uses ALL columns in the tuple
# instead of just the single `pk` field from TableSpec.
#
# Example: SUPPLY has duplicate IDs across SKUs, so MERGE ON uses (ID, SKU).
COMPOUND_PK: Dict[str, Tuple[str, ...]] = {
    "SUPPLY": ("ID", "SKU"),
}

# ═══════════════════════════════════════════════════════
# DBT LAYERS (for post-dbt row count logging)
# ═══════════════════════════════════════════════════════
# Only LZ, STAGING, DBO tables — SOURCE is logged by the ingestion asset.
# This separation prevents duplicate SOURCE rows in LAYER_ROW_COUNTS.
#
# Grouped by entity for readability:
#   (schema, table_name) — must match actual Snowflake table/view names
DBT_LAYERS: List[Tuple[str, str]] = [
    # Customer entity
    ("LZ", "RAW_CUSTOMER"),
    ("STAGING", "STG_CUSTOMER"),
    ("DBO", "DIM_CUSTOMER"),

    # Order Detail entity
    ("LZ", "RAW_ORDER_DETAIL"),
    ("STAGING", "STG_ORDER_DETAIL"),
    ("DBO", "FCT_ORDER_DETAIL"),

    # Order Item entity
    ("LZ", "RAW_ORDER_ITEM"),
    ("STAGING", "STG_ORDER_ITEM"),
    ("DBO", "FCT_ORDER_ITEM"),

    # Product entity
    ("LZ", "RAW_PRODUCT"),
    ("STAGING", "STG_PRODUCT"),
    ("DBO", "DIM_PRODUCT"),

    # Store entity
    ("LZ", "RAW_STORE"),
    ("STAGING", "STG_STORE"),
    ("DBO", "DIM_STORE"),

    # Supply entity
    ("LZ", "RAW_SUPPLY"),
    ("STAGING", "STG_SUPPLY"),
    ("DBO", "DIM_SUPPLY"),

    # Discount entity
    ("LZ", "RAW_DISCOUNT"),
    ("STAGING", "STG_DISCOUNT"),
    ("DBO", "DIM_DISCOUNT"),

    # Loyalty Points entity
    ("LZ", "RAW_LOYALTY_POINTS"),
    ("STAGING", "STG_LOYALTY_POINTS"),
    ("DBO", "DIM_LOYALTY_POINTS"),
]