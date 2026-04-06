import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def get_secret(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value not in (None, ""):
        return value

    try:
        import streamlit as st

        secret_value = st.secrets.get(name)
        if secret_value not in (None, ""):
            return str(secret_value)
    except Exception:
        pass

    return default

@dataclass(frozen=True)
class Settings:
    anthropic_api_key: str
    claude_model: str
    data_source: str
    excel_file_path: str
    excel_sheet_name: Optional[str]
    mysql_host: str
    mysql_port: int
    mysql_db: str
    mysql_user: str
    mysql_password: str
    allow_tables: list[str]

def get_settings() -> Settings:
    allow_tables_raw = get_secret("ALLOW_TABLES", "").strip()
    allow_tables = [t.strip() for t in allow_tables_raw.split(",") if t.strip()] if allow_tables_raw else []

    excel_file_path = get_secret("EXCEL_FILE_PATH", "").strip()
    excel_sheet_name_raw = get_secret("EXCEL_SHEET_NAME", "").strip()
    excel_sheet_name = excel_sheet_name_raw or None
    data_source = get_secret("NLQ_DATA_SOURCE", "excel" if excel_file_path else "sql").strip().lower()

    if excel_file_path:
        if not os.path.isfile(excel_file_path):
            raise ValueError(f"Excel file not found: {excel_file_path}")

    return Settings(
        anthropic_api_key=get_secret("ANTHROPIC_API_KEY", ""),
        claude_model=get_secret("CLAUDE_MODEL", "claude-sonnet-4.6"),
        data_source=data_source,
        excel_file_path=excel_file_path,
        excel_sheet_name=excel_sheet_name,
        mysql_host=get_secret("MYSQL_HOST", "localhost"),
        mysql_port=int(get_secret("MYSQL_PORT", "3306")),
        mysql_db=get_secret("MYSQL_DB", ""),
        mysql_user=get_secret("MYSQL_USER", ""),
        mysql_password=get_secret("MYSQL_PASSWORD", ""),
        allow_tables=allow_tables,
    )
