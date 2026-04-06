import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional

from sqlalchemy import text, inspect
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate


# ==============================
# LOGGER SETUP
# ==============================
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("schema_logger")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    "logs/schema_intent.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=3
)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)


# ==============================
# LLM SETUP
# ==============================
llm = ChatOpenAI(temperature=0)


# ==============================
# DB CONNECTION
# ==============================
def _build_mysql_uri_from_env() -> str:
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT", "3306")
    dbname = os.getenv("MYSQL_DB")
    user = os.getenv("MYSQL_USER")
    pwd = os.getenv("MYSQL_PASSWORD")

    if not all([host, port, dbname, user, pwd]):
        logger.error("Missing required MySQL environment variables")
        raise ValueError("Missing MySQL env vars")

    return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{dbname}"


def _make_db(allow_tables: Optional[List[str]] = None) -> SQLDatabase:
    uri = _build_mysql_uri_from_env()

    if allow_tables:
        logger.info(f"DB restricted to tables: {allow_tables}")
        return SQLDatabase.from_uri(uri, include_tables=allow_tables)

    return SQLDatabase.from_uri(uri)


# ==============================
# 🔥 AUTO COLUMN DESCRIPTION (NEW)
# ==============================
def generate_column_description(col_name: str) -> str:
    col = col_name.lower()

    if col == "id":
        return "unique identifier"

    if col.endswith("_id"):
        return f"reference to {col.replace('_id', '')}"

    if any(k in col for k in ["amt", "amount", "price", "cost"]):
        return "monetary value"

    if any(k in col for k in ["date", "time", "created", "updated"]):
        return "timestamp or date"

    if any(k in col for k in ["name"]):
        return "name or label"

    if any(k in col for k in ["status", "type"]):
        return "categorical value"

    return "general field"


# ==============================
# 🔥 ENHANCED SCHEMA (UPDATED)
# ==============================
def get_enhanced_schema(db, tables: List[str]) -> str:
    engine = db._engine
    insp = inspect(engine)

    schema_text = ""

    for table in tables:
        cols = insp.get_columns(table)

        schema_text += f"\nTable: {table}\n"
        schema_text += "Columns:\n"

        for c in cols:
            col_name = c["name"]

            # Try DB comment first (if exists)
            description = c.get("comment")

            if not description:
                description = generate_column_description(col_name)

            schema_text += f"- {col_name}: {description}\n"

        # Sample rows
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table} LIMIT 3"))
                rows = result.mappings().all()

                if rows:
                    schema_text += "Sample Rows:\n"
                    for r in rows:
                        schema_text += f"{dict(r)}\n"

        except Exception:
            pass

    logger.info(f"Enhanced schema generated for: {tables}")
    return schema_text


# ==============================
# TABLE SELECTOR
# ==============================
table_selector_prompt = ChatPromptTemplate.from_template("""
Select relevant tables for the query.

Tables:
{tables}

Question:
{question}

Return ONLY comma-separated table names.
""")


def select_tables(question: str, all_tables: List[str]) -> List[str]:
    chain = table_selector_prompt | llm

    response = chain.invoke({
        "tables": ", ".join(all_tables),
        "question": question
    })

    selected = [t.strip() for t in response.content.split(",") if t.strip()]

    logger.info(f"Selected tables: {selected}")
    return selected


# ==============================
# SQL GENERATOR
# ==============================
sql_prompt = ChatPromptTemplate.from_template("""
Generate SQL using ONLY the schema.

Schema:
{schema}

Rules:
- Only use listed tables/columns
- Return ONLY SQL

Question:
{question}
""")


def generate_sql(question: str, schema: str) -> str:
    chain = sql_prompt | llm

    response = chain.invoke({
        "schema": schema,
        "question": question
    })

    sql = response.content.strip()
    logger.info(f"Generated SQL: {sql}")

    return sql


# ==============================
# RUN QUERY
# ==============================
def run_query(settings, sql: str, allow_tables=None, limit_rows=500):
    db = _make_db(allow_tables=allow_tables)

    if not sql.lower().startswith("select"):
        logger.warning(f"Blocked query: {sql}")
        raise ValueError("Only SELECT allowed")

    try:
        with db._engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = result.mappings().all()

            logger.info(f"Rows fetched: {len(rows)}")
            return [dict(r) for r in rows[:limit_rows]]

    except Exception as e:
        logger.error(f"Execution failed: {e}")
        raise


# ==============================
# RETRY LOOP
# ==============================
def execute_with_retry(question, db, tables, max_retries=2):
    schema = get_enhanced_schema(db, tables)

    for _ in range(max_retries):
        sql = generate_sql(question, schema)

        try:
            return run_query(None, sql, allow_tables=tables)

        except Exception as e:
            logger.warning(f"Retry due to error: {e}")

            fix_prompt = f"""
Fix SQL error.

Error: {e}
SQL: {sql}

Schema:
{schema}

Return ONLY corrected SQL.
"""

            sql = llm.invoke(fix_prompt).content.strip()

    raise Exception("Failed after retries")


# ==============================
# MAIN PIPELINE
# ==============================
def query_pipeline(question: str):
    logger.info(f"User Question: {question}")

    db = _make_db()

    all_tables = db.get_usable_table_names()

    selected_tables = select_tables(question, all_tables)

    return execute_with_retry(question, db, selected_tables)
