import os
from contextlib import contextmanager
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

_CONN_PARAMS = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "CORTEX_ANALYST_DEV_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "CLEAR_TO_BUILD_DEV"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "INFORMATION_MART"),
    "role": os.getenv("SNOWFLAKE_ROLE", ""),
}

DB = _CONN_PARAMS["database"]
SCHEMA = _CONN_PARAMS["schema"]


@contextmanager
def get_connection():
    conn = snowflake.connector.connect(**{k: v for k, v in _CONN_PARAMS.items() if v})
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
