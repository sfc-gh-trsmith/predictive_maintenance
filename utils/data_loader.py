import streamlit as st
import pandas as pd
from snowflake.connector import connect
import os
from typing import Any, Optional

@st.cache_resource
def init_connection():
    """Initializes a connection to Snowflake, cached for all views."""
    return connect(**st.secrets["snowflake"], client_session_keep_alive=True)

conn = init_connection()

@st.cache_data(ttl=600)
def run_query(query: str, params: list = None) -> pd.DataFrame:
    """Executes a query and returns a Pandas DataFrame, with results cached."""
    with conn.cursor() as cur:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur.fetch_pandas_all()

# -----------------------------
# HTTP auth helpers for Snowflake REST endpoints (Cortex/Intelligence)
# -----------------------------

def get_base_url(account: str) -> str:
    """Build Snowflake base URL from account identifier (underscores become dashes)."""
    return f"https://{account.replace('_', '-')}.snowflakecomputing.com"


def get_verify_ssl(value: Any, default: bool = True) -> bool:
    """Normalize verify_ssl value from secrets or env into a boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(default)


def _read_token_file(path: str) -> Optional[str]:
    """Read a token from a file path if it exists and is readable."""
    try:
        if path and os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                token = f.read().strip()
                return token or None
    except Exception:
        # Intentionally silent: caller will try next source
        return None
    return None


def get_pat_token(connection_name: Optional[str] = None) -> str:
    """Resolve PAT using documented precedence.

    Precedence (first non-empty wins):
      1) SNOWFLAKE_TOKEN
      2) SNOWFLAKE_CONNECTIONS_<CONNECTION_NAME>_TOKEN (if connection_name provided)
      3) SNOWFLAKE_TOKEN_FILE_PATH (read file content)
      4) st.secrets["snowflake"]["personal_access_token"]
      5) st.secrets["snowflake"]["token_file_path"] (read file content)
    """
    # 1) Direct token env var
    token = os.getenv("SNOWFLAKE_TOKEN")
    if token:
        return token

    # 2) Connection-scoped token env var
    if connection_name:
        env_key = f"SNOWFLAKE_CONNECTIONS_{connection_name.upper()}_TOKEN"
        token = os.getenv(env_key)
        if token:
            return token

    # 3) Token file env var
    token_file_path = os.getenv("SNOWFLAKE_TOKEN_FILE_PATH")
    token = _read_token_file(token_file_path) if token_file_path else None
    if token:
        return token

    # 4) secrets: direct token
    try:
        secrets_token = st.secrets.get("snowflake", {}).get("personal_access_token")
        if secrets_token:
            return str(secrets_token)
    except Exception:
        pass

    # 5) secrets: token file path
    try:
        secrets_token_file = st.secrets.get("snowflake", {}).get("token_file_path")
        token = _read_token_file(secrets_token_file) if secrets_token_file else None
        if token:
            return token
    except Exception:
        pass

    raise RuntimeError(
        "Programmatic access token not found. Provide SNOWFLAKE_TOKEN, "
        "SNOWFLAKE_CONNECTIONS_<NAME>_TOKEN, SNOWFLAKE_TOKEN_FILE_PATH, or set "
        "personal_access_token/token_file_path in st.secrets['snowflake']."
    )


def build_snowflake_headers(token: str, accept: str = "application/json") -> dict:
    """Build standard headers for Snowflake REST APIs using PAT."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": accept,
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    }