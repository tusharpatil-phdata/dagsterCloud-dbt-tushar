"""
Snowflake connection helper — RSA key pair auth, single DB.

This module provides a reusable get_connection() function that:
  - Reads the RSA private key PEM from the SNOWFLAKE_PRIVATE_KEY_PEM env var
  - Deserializes it into DER bytes (what the Snowflake connector expects)
  - Returns an authenticated connection to DAGSTER_DBT_KIEWIT_DB_CLOUD

Usage:
    from dagster_code.snowflake_client import get_connection
    conn = get_connection("SOURCE")   # connects to SOURCE schema
    conn = get_connection("METRICS")  # connects to METRICS schema
"""

import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dagster_code.config import DB

def _load_private_key() -> bytes:
    """
    Load RSA private key from SNOWFLAKE_PRIVATE_KEY_PEM env var.

    The env var must contain the FULL PEM content including:
      -----BEGIN PRIVATE KEY-----
      ... base64 key data ...
      -----END PRIVATE KEY-----

    Returns DER-encoded bytes (Snowflake connector requires this format).
    Raises ValueError if the env var is empty or not set.
    """
    pem = os.getenv("SNOWFLAKE_PRIVATE_KEY_PEM", "")
    if not pem.strip():
        raise ValueError(
            "SNOWFLAKE_PRIVATE_KEY_PEM is empty or not set. "
            "Set it in Dagster+ → Deployment → Environment Variables."
        )
    # Deserialize PEM text → private key object (no passphrase)
    key = serialization.load_pem_private_key(
        pem.encode("utf-8"), password=None, backend=default_backend()
    )
    # Convert to DER bytes (what snowflake-connector-python expects)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def get_connection(schema: str = "SOURCE") -> snowflake.connector.SnowflakeConnection:
    """
    Return an authenticated Snowflake connection using RSA key pair.

    Args:
        schema: Which schema to connect to (default: "SOURCE").
                Common values: "SOURCE", "METRICS", "LZ", "STAGING", "DBO"

    All connections go to the same database (DB constant from config.py).
    Account, user, and warehouse are read from env vars set in Dagster+.
    """
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=_load_private_key(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=DB,
        schema=schema,
    )