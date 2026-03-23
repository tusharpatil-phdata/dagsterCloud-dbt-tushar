"""Centralized configuration for the medallion ELT pipeline."""

from dataclasses import dataclass
from typing import List, Tuple

# ─── Database (single DB, no sandbox) ─────────────────
DB = "DAGSTER_DBT_KIEWIT_DB_CLOUD"
TZ_NOW = "CONVERT_TIMEZONE('UTC','Asia/Kolkata',CURRENT_TIMESTAMP())"


# ─── Table Registry ───────────────────────────────────
@dataclass(frozen=True)
class TableSpec:
    """Defines one source table and all metadata needed for ingestion."""
    name: str              # e.g. "CUSTOMER"
    csv_file: str          # e.g. "customer.csv"
    pk: str                # primary key column for MERGE ON
    key_cols: List[str]    # columns to validate (non-null checks)
    merge_set: List[str]   # columns to SET on MERGE WHEN MATCHED
    all_cols: List[str]    # all columns (for INSERT on MERGE NOT MATCHED)


TABLES: List[TableSpec] = [
    TableSpec("CUSTOMER",       "customer.csv",       "ID",  ["ID","NAME"],
              ["NAME"], ["ID","NAME"]),
    TableSpec("ORDER_DETAIL",   "order_detail.csv",   "ID",  ["ID","CUSTOMER","STORE_ID"],
              ["CUSTOMER","ORDERED_AT","STORE_ID","SUBTOTAL","TAX_PAID","ORDER_TOTAL"],
              ["ID","CUSTOMER","ORDERED_AT","STORE_ID","SUBTOTAL","TAX_PAID","ORDER_TOTAL"]),
    TableSpec("ORDER_ITEM",     "order_item.csv",     "ID",  ["ID","ORDER_ID","SKU"],
              ["ORDER_ID","SKU"], ["ID","ORDER_ID","SKU"]),
    TableSpec("PRODUCT",        "product.csv",        "SKU", ["SKU","NAME"],
              ["NAME","TYPE","PRICE","DESCRIPTION"],
              ["SKU","NAME","TYPE","PRICE","DESCRIPTION"]),
    TableSpec("STORE",          "store.csv",          "ID",  ["ID","NAME"],
              ["NAME","OPENED_AT","TAX_RATE"], ["ID","NAME","OPENED_AT","TAX_RATE"]),
    TableSpec("SUPPLY",         "supply.csv",         "ID",  ["ID","SKU","NAME"],
              ["NAME","COST","PERISHABLE"],
              ["ID","NAME","COST","PERISHABLE","SKU"]),
    TableSpec("DISCOUNT",       "discounts.csv",      "ID",  ["ID","CUSTOMER_ID"],
              ["CUSTOMER_ID","PERCENT","VALID_FROM","VALID_TO"],
              ["ID","CUSTOMER_ID","PERCENT","VALID_FROM","VALID_TO"]),
    TableSpec("LOYALTY_POINTS", "loyalty_points.csv", "ID",  ["ID","CUSTOMER_ID"],
              ["CUSTOMER_ID","POINTS_BALANCE","POINTS_EARNED","POINTS_REDEEMED","AS_OF_DATE"],
              ["ID","CUSTOMER_ID","POINTS_BALANCE","POINTS_EARNED","POINTS_REDEEMED","AS_OF_DATE"]),
]

# Tables with compound primary key for MERGE ON clause
COMPOUND_PK = {"SUPPLY": ("ID", "SKU")}

# All layers for post-dbt row count logging
ALL_LAYERS: List[Tuple[str, str]] = [
    ("SOURCE","CUSTOMER"),("LZ","RAW_CUSTOMER"),("STAGING","STG_CUSTOMER"),("DBO","DIM_CUSTOMER"),
    ("SOURCE","ORDER_DETAIL"),("LZ","RAW_ORDER_DETAIL"),("STAGING","STG_ORDER_DETAIL"),("DBO","FCT_ORDER_DETAIL"),
    ("SOURCE","ORDER_ITEM"),("LZ","RAW_ORDER_ITEM"),("STAGING","STG_ORDER_ITEM"),("DBO","FCT_ORDER_ITEM"),
    ("SOURCE","PRODUCT"),("LZ","RAW_PRODUCT"),("STAGING","STG_PRODUCT"),("DBO","DIM_PRODUCT"),
    ("SOURCE","STORE"),("LZ","RAW_STORE"),("STAGING","STG_STORE"),("DBO","DIM_STORE"),
    ("SOURCE","SUPPLY"),("LZ","RAW_SUPPLY"),("STAGING","STG_SUPPLY"),("DBO","DIM_SUPPLY"),
    ("SOURCE","DISCOUNT"),("LZ","RAW_DISCOUNT"),("STAGING","STG_DISCOUNT"),("DBO","DIM_DISCOUNT"),
    ("SOURCE","LOYALTY_POINTS"),("LZ","RAW_LOYALTY_POINTS"),("STAGING","STG_LOYALTY_POINTS"),("DBO","DIM_LOYALTY_POINTS"),
]