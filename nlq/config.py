import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

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
    allow_tables_raw = os.getenv("ALLOW_TABLES", "").strip()
    allow_tables = [t.strip() for t in allow_tables_raw.split(",") if t.strip()] if allow_tables_raw else []

    excel_file_path = os.getenv("EXCEL_FILE_PATH", "").strip()
    excel_sheet_name_raw = os.getenv("EXCEL_SHEET_NAME", "").strip()
    excel_sheet_name = excel_sheet_name_raw or None
    data_source = os.getenv("NLQ_DATA_SOURCE", "excel" if excel_file_path else "sql").strip().lower()

    if excel_file_path:
        if not os.path.isfile(excel_file_path):
            raise ValueError(f"Excel file not found: {excel_file_path}")

    return Settings(
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
        claude_model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4.6"),
        data_source=data_source,
        excel_file_path=excel_file_path,
        excel_sheet_name=excel_sheet_name,
        mysql_host=os.getenv("MYSQL_HOST", "localhost"),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        mysql_db=os.getenv("MYSQL_DB", ""),
        mysql_user=os.getenv("MYSQL_USER", ""),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        allow_tables=allow_tables,
    )
