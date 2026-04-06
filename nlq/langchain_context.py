from typing import Any, Dict, List, Optional


def get_langchain_context(allow_tables: Optional[List[str]] = None) -> Dict[str, Any]:
    try:
        from langchain_community.utilities import SQLDatabase
    except Exception as exc:
        return {
            "available": False,
            "reason": f"langchain import failed: {exc}",
            "table_info": None,
        }

    from nlq.db_langchain import _build_mysql_uri_from_env

    uri = _build_mysql_uri_from_env()
    db = SQLDatabase.from_uri(uri, include_tables=allow_tables) if allow_tables else SQLDatabase.from_uri(uri)

    usable_tables = list(db.get_usable_table_names())
    table_info = db.get_table_info(usable_tables) if usable_tables else ""

    return {
        "available": True,
        "usable_tables": usable_tables,
        "table_info": table_info,
    }
