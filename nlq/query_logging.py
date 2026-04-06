import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


LOGS_DIR = Path("logs")


def derive_intent(question: str, schema_tables: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, Any]:
    q = question.strip()
    q_lower = q.lower()
    schema_tables = schema_tables or {}

    mentioned_tables = _find_schema_matches(q_lower, list(schema_tables.keys()))
    all_columns: List[str] = []
    for columns in schema_tables.values():
        all_columns.extend(columns.keys())
    mentioned_columns = _find_schema_matches(q_lower, all_columns)

    return {
        "question": q,
        "intent_type": _classify_intent_type(q_lower),
        "scope": _classify_scope(q_lower),
        "operation": _classify_operation(q_lower),
        "mentioned_tables": mentioned_tables,
        "mentioned_columns": mentioned_columns,
        "has_filters": _has_filters(q_lower),
        "has_grouping": any(token in q_lower for token in ["group by", "grouped", "each", "per "]),
        "has_sorting": any(token in q_lower for token in ["top", "highest", "lowest", "ascending", "descending", "order by", "sort"]),
        "is_aggregate": _classify_operation(q_lower) in {"count", "sum", "average", "minimum", "maximum"},
    }


def _find_schema_matches(question: str, names: List[str]) -> List[str]:
    matches: List[str] = []
    for name in names:
        candidate = name.lower()
        if candidate and candidate in question:
            matches.append(name)
            continue
        tokenized = candidate.replace("_", " ")
        if tokenized != candidate and tokenized in question:
            matches.append(name)
    return sorted(set(matches))


def _classify_scope(question: str) -> str:
    if any(token in question for token in ["schema", "column", "columns", "table", "tables", "database", "databases"]):
        return "metadata"
    return "data"


def _classify_intent_type(question: str) -> str:
    if any(token in question for token in ["schema", "column", "columns", "table", "tables", "database structure"]):
        return "schema_lookup"
    if any(token in question for token in ["how many", "count", "number of"]):
        return "count_query"
    if any(token in question for token in ["sum", "total", "add up"]):
        return "aggregation_query"
    if any(token in question for token in ["average", "avg", "mean"]):
        return "aggregation_query"
    if any(token in question for token in ["max", "maximum", "highest", "largest", "top"]):
        return "ranking_query"
    if any(token in question for token in ["min", "minimum", "lowest", "smallest", "bottom"]):
        return "ranking_query"
    if any(token in question for token in ["show", "list", "display", "give me", "find", "fetch"]):
        return "retrieval_query"
    return "general_query"


def _classify_operation(question: str) -> str:
    if any(token in question for token in ["how many", "count", "number of"]):
        return "count"
    if any(token in question for token in ["sum", "total", "add up"]):
        return "sum"
    if any(token in question for token in ["average", "avg", "mean"]):
        return "average"
    if any(token in question for token in ["max", "maximum", "highest", "largest", "top"]):
        return "maximum"
    if any(token in question for token in ["min", "minimum", "lowest", "smallest", "bottom"]):
        return "minimum"
    if any(token in question for token in ["show", "list", "display", "give me", "find", "fetch"]):
        return "select"
    return "unknown"


def _has_filters(question: str) -> bool:
    filter_tokens = ["where", "with", "for", "between", "after", "before", "on ", "in ", "equal to", "greater than", "less than"]
    return any(token in question for token in filter_tokens)


def _slugify(value: str, max_length: int = 50) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip()).strip("_").lower()
    return (text or "query")[:max_length]


class QueryLogger:
    def __init__(self, question: str, channel: str = "chat"):
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        slug = _slugify(question)
        self.log_path = LOGS_DIR / f"{timestamp}_{channel}_{slug}_{uuid.uuid4().hex[:8]}.log"

        self._write_entry(
            step="log_created",
            status="started",
            payload={"channel": channel, "question": question},
        )

    def _write_entry(self, step: str, status: str, payload: Optional[Dict[str, Any]] = None) -> None:
        entry = {
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "step": step,
            "status": status,
            "payload": payload or {},
        }
        with self.log_path.open("a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def info(self, step: str, **payload: Any) -> None:
        self._write_entry(step=step, status="info", payload=payload)

    def success(self, step: str, **payload: Any) -> None:
        self._write_entry(step=step, status="success", payload=payload)

    def error(self, step: str, **payload: Any) -> None:
        self._write_entry(step=step, status="error", payload=payload)
