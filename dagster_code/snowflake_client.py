"""Snowflake connection helper — RSA key pair auth, single DB."""

import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dagster_code.config import DB


def _load_private_key() -> bytes:
    """Deserialize RSA private key PEM (no passphrase) → DER bytes."""
    pem = os.getenv("SNOWFLAKE_PRIVATE_KEY_PEM", "")
    if not pem.strip():
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PEM is empty or not set")
    key = serialization.load_pem_private_key(
        pem.encode("utf-8"), password=None, backend=default_backend()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection(schema: str = "SOURCE") -> snowflake.connector.SnowflakeConnection:
    """Return an authenticated Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=_load_private_key(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=DB,
        schema=schema,
    )