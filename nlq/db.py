import pymysql
from nlq.config import Settings


def _safe_identifier(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return f"`{name}`"


def mysql_conn(settings: Settings):
    return pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_db,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def get_schema(settings: Settings, allow_tables=None):
    sql = '''
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA=%s
    ORDER BY TABLE_NAME, ORDINAL_POSITION
    '''
    with mysql_conn(settings).cursor() as cur:
        cur.execute(sql, (settings.mysql_db,))
        rows = cur.fetchall()

    allowed = allow_tables if allow_tables else None

    schema = {}
    for r in rows:
        t = r["TABLE_NAME"]
        if allowed and t not in allowed:
            continue
        schema.setdefault(t, {})
        schema[t][r["COLUMN_NAME"]] = r["DATA_TYPE"]

    return {"database": settings.mysql_db, "tables": schema}

def run_query(settings: Settings, sql: str):
    with mysql_conn(settings).cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def get_distinct_column_values(
    settings: Settings,
    table: str,
    column: str,
    limit: int = 100,
):
    table_sql = _safe_identifier(table)
    column_sql = _safe_identifier(column)
    sql = f"""
    SELECT DISTINCT CAST({column_sql} AS CHAR) AS value
    FROM {table_sql}
    WHERE {column_sql} IS NOT NULL
      AND TRIM(CAST({column_sql} AS CHAR)) <> ''
    ORDER BY value
    LIMIT %s
    """

    with mysql_conn(settings).cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    return [row["value"] for row in rows if row.get("value")]


def get_column_value_stats(
    settings: Settings,
    table: str,
    column: str,
    distinct_limit: int = 100,
):
    table_sql = _safe_identifier(table)
    column_sql = _safe_identifier(column)

    count_sql = f"""
    SELECT COUNT(DISTINCT CAST({column_sql} AS CHAR)) AS distinct_count
    FROM {table_sql}
    WHERE {column_sql} IS NOT NULL
      AND TRIM(CAST({column_sql} AS CHAR)) <> ''
    """

    with mysql_conn(settings).cursor() as cur:
        cur.execute(count_sql)
        count_row = cur.fetchone() or {}

    distinct_count = int(count_row.get("distinct_count") or 0)
    values = []

    if distinct_count and distinct_count <= distinct_limit:
        values = get_distinct_column_values(settings, table, column, limit=distinct_limit)

    return {
        "distinct_count": distinct_count,
        "distinct_values": values,
    }
