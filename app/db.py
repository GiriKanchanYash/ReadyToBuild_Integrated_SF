import base64
import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


class SnowflakeConfigError(RuntimeError):
    """Snowflake credential/configuration problem.

    Subclasses RuntimeError so existing callers behave the same; app.main maps
    it to a readable API response. Fabric code never raises it, so the Fabric
    data source's error handling is unaffected.
    """


@lru_cache(maxsize=4)
def _parse_private_key(data: bytes, passphrase: str) -> bytes:
    from cryptography.hazmat.primitives import serialization

    pwd = passphrase.encode() if passphrase else None
    try:
        if data.lstrip().startswith(b"-----BEGIN"):
            key = serialization.load_pem_private_key(data, password=pwd)
        else:
            key = serialization.load_der_private_key(data, password=pwd)
    except Exception as exc:
        raise SnowflakeConfigError(
            f"Snowflake private key could not be parsed ({exc}). "
            "SNOWFLAKE_PRIVATE_KEY_B64 must be the base64 of the rsa_key.p8 file "
            "itself: [Convert]::ToBase64String([IO.File]::ReadAllBytes('rsa_key.p8'))."
        ) from exc
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _private_key_der() -> bytes | None:
    """Key-pair credential, or None when only a password is configured.

    Snowflake requires MFA for password sign-in, which a background service
    cannot satisfy, so deployments authenticate with an RSA key instead.
    """
    b64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_B64", "").strip()
    pem = os.getenv("SNOWFLAKE_PRIVATE_KEY", "").strip()
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()

    if b64:
        compact = "".join(b64.split())
        try:
            material = base64.b64decode(compact + "=" * (-len(compact) % 4))
        except Exception as exc:
            raise SnowflakeConfigError(
                f"SNOWFLAKE_PRIVATE_KEY_B64 is not valid base64 ({exc}). Got "
                f"{len(compact)} characters; the base64 of a 2048-bit rsa_key.p8 is "
                "about 2272. The value was most likely truncated when pasted."
            ) from exc
    elif pem:
        # Azure App Settings hold a single line, so newlines arrive escaped.
        material = pem.replace("\\n", "\n").encode()
    elif key_path:
        if not Path(key_path).is_file():
            raise SnowflakeConfigError(
                f"SNOWFLAKE_PRIVATE_KEY_PATH points at {key_path!r}, which does not "
                "exist. A local Windows path cannot work on the Linux App Service; "
                "set SNOWFLAKE_PRIVATE_KEY_B64 there instead and remove this setting."
            )
        material = Path(key_path).read_bytes()
    else:
        return None

    return _parse_private_key(
        material, os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip()
    )


_CONN_PARAMS: dict[str, Any] = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "CORTEX_ANALYST_DEV_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "CLEAR_TO_BUILD_DEV"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "INFORMATION_MART"),
    "role": os.getenv("SNOWFLAKE_ROLE", ""),
}

DB = _CONN_PARAMS["database"]
SCHEMA = _CONN_PARAMS["schema"]


def _auth_params() -> dict[str, Any]:
    """Resolved per connection so a malformed key fails the request, not the import."""
    private_key = _private_key_der()
    if private_key:
        return {"private_key": private_key}
    password = os.getenv("SNOWFLAKE_PASSWORD", "")
    if not password:
        raise SnowflakeConfigError(
            "No Snowflake credential is configured. Set SNOWFLAKE_PRIVATE_KEY_B64 to "
            "the base64 of rsa_key.p8 (about 2272 characters). SNOWFLAKE_PASSWORD is "
            "not a usable fallback because the account enforces MFA on password "
            "sign-in."
        )
    return {"password": password}


@contextmanager
def get_connection():
    params = {**_CONN_PARAMS, **_auth_params()}
    conn = snowflake.connector.connect(**{k: v for k, v in params.items() if v})
    try:
        yield conn
    finally:
        conn.close()


def run_query_on_conn(conn, sql: str, params: tuple | None = None) -> list[dict]:
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def run_query(sql: str, params: tuple | None = None) -> list[dict]:
    with get_connection() as conn:
        return run_query_on_conn(conn, sql, params)
