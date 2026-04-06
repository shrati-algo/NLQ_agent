import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from nlq.config import Settings
from nlq.db import get_column_value_stats, get_schema


CATALOG_DIR = Path("metadata")
CATALOG_PATH = CATALOG_DIR / "catalog.json"
TEXT_TYPES = ("char", "text", "varchar")


def build_metadata_catalog(
    settings: Settings,
    allow_tables: Optional[List[str]] = None,
    distinct_limit: int = 100,
) -> Dict[str, Any]:
    schema = get_schema(settings, allow_tables=allow_tables)
    tables: Dict[str, Any] = {}

    for table_name, columns in schema["tables"].items():
        table_entry = {"columns": {}}
        for column_name, data_type in columns.items():
            column_entry: Dict[str, Any] = {
                "data_type": data_type,
                "is_text": any(token in data_type.lower() for token in TEXT_TYPES),
            }

            if column_entry["is_text"]:
                stats = get_column_value_stats(
                    settings=settings,
                    table=table_name,
                    column=column_name,
                    distinct_limit=distinct_limit,
                )
                column_entry.update(stats)
            else:
                column_entry.update({"distinct_count": None, "distinct_values": []})

            table_entry["columns"][column_name] = column_entry

        tables[table_name] = table_entry

    catalog = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database": schema["database"],
        "tables": tables,
    }
    return catalog


def save_metadata_catalog(catalog: Dict[str, Any], path: Path = CATALOG_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_metadata_catalog(path: Path = CATALOG_PATH) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_catalog(catalog: Dict[str, Any]) -> Dict[str, Any]:
    summary = {
        "generated_at": catalog.get("generated_at"),
        "database": catalog.get("database"),
        "table_count": len(catalog.get("tables", {})),
        "tables": {},
    }

    for table_name, table_info in catalog.get("tables", {}).items():
        columns = table_info.get("columns", {})
        summary["tables"][table_name] = {
            "column_count": len(columns),
            "text_column_count": sum(1 for col in columns.values() if col.get("is_text")),
            "low_cardinality_columns": [
                column_name
                for column_name, column_info in columns.items()
                if column_info.get("distinct_values")
            ],
        }

    return summary
