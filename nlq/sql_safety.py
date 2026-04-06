import re


class SQLRejected(Exception):
    pass


def validate_select_only(sql: str) -> None:
    """
    Simple and stable SELECT-only validation.
    No sqlglot dependency (avoids version issues).
    """

    if not sql:
        raise SQLRejected("Empty SQL query.")

    sql_clean = sql.strip().lower()

    # Must start with SELECT
    if not sql_clean.startswith("select"):
        raise SQLRejected("Only SELECT queries are allowed.")

    # Block multiple statements
    if ";" in sql_clean[:-1]:
        raise SQLRejected("Multiple statements are not allowed.")

    # Forbidden keywords
    forbidden = [
        "insert",
        "update",
        "delete",
        "drop",
        "create",
        "alter",
        "truncate",
        "replace",
    ]

    for word in forbidden:
        if f" {word} " in f" {sql_clean} ":
            raise SQLRejected(f"Forbidden keyword detected: {word}")


def enforce_limit(sql: str, max_rows: int = 50) -> str:
    """
    Ensures a LIMIT clause exists.
    """
    sql_clean = sql.strip().rstrip(";")

    if not re.search(r"\blimit\s+\d+\b", sql_clean, flags=re.IGNORECASE):
        return f"{sql_clean} LIMIT {max_rows};"

    return sql_clean + ";"
