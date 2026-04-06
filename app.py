import os
import re
import uuid
import json
import streamlit as st
import pandas as pd
import plotly.express as px

from nlq.config import get_settings
from nlq.agent import ClaudeNLQAgent
from nlq.db import get_schema, run_query
from nlq.excel_parser import parse_excel
from nlq.langchain_context import get_langchain_context
from nlq.intent_resolution import resolve_query_context
from nlq.metadata_catalog import build_metadata_catalog, save_metadata_catalog, summarize_catalog
from nlq.query_logging import QueryLogger, derive_intent
from nlq.sql_safety import validate_select_only, enforce_limit, SQLRejected

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def extract_sql(text: str) -> str:
    if not text:
        return ""
    t = text.strip()
    t = re.sub(r"```sql|```", "", t, flags=re.IGNORECASE).strip()
    m = re.search(r"(SELECT\s.+)", t, flags=re.IGNORECASE | re.DOTALL)
    return (m.group(1).strip() if m else t).rstrip(";") + ";"


def appears_truncated_sql(sql: str) -> bool:
    sql_upper = sql.upper()
    return (
        sql_upper.count("CASE") > sql_upper.count("END")
        or sql_upper.rstrip().endswith("WHEN;")
        or sql_upper.rstrip().endswith("THEN;")
        or sql_upper.rstrip().endswith("CASE;")
    )


def wants_visualization(question: str) -> bool:
    q = (question or "").lower()
    visualization_terms = [
        "plot", "chart", "graph", "visual", "visualize", "visualise",
        "bar chart", "line chart", "scatter", "histogram", "pie chart",
        "pie graph", "pie plot", "donut chart", "doughnut chart",
        "trend", "distribution",
    ]
    return any(term in q for term in visualization_terms)


def normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def find_matching_column(df: pd.DataFrame, text: str, preferred_numeric: bool | None = None) -> str | None:
    text_norm = normalize_column_name(text)
    if not text_norm:
        return None

    numeric_cols = set(df.select_dtypes(include="number").columns)
    candidates: list[tuple[int, str]] = []

    for col in df.columns:
        col_norm = normalize_column_name(col)
        score = 0
        if col_norm == text_norm:
            score += 100
        if col_norm in text_norm or text_norm in col_norm:
            score += 60

        text_tokens = set(text_norm.split())
        col_tokens = set(col_norm.split())
        score += len(text_tokens & col_tokens) * 10

        if preferred_numeric is True and col in numeric_cols:
            score += 5
        if preferred_numeric is False and col not in numeric_cols:
            score += 5

        if score > 0:
            candidates.append((score, col))

    candidates.sort(reverse=True)
    return candidates[0][1] if candidates else None


def parse_visualization_request(df: pd.DataFrame, question: str) -> dict:
    q = (question or "").lower()
    numeric_cols = list(df.select_dtypes(include="number").columns)
    categorical_cols = [col for col in df.columns if col not in numeric_cols]

    chart_type = "bar"
    explicit_chart_type = None
    if any(term in q for term in ["pie chart", "pie graph", "pie plot", "donut chart", "doughnut chart"]):
        chart_type = "pie"
        explicit_chart_type = "pie"
    elif "scatter" in q:
        chart_type = "scatter"
        explicit_chart_type = "scatter"
    elif "histogram" in q or "distribution" in q:
        chart_type = "histogram"
        explicit_chart_type = "histogram"
    elif "line" in q or "trend" in q:
        chart_type = "line"
        explicit_chart_type = "line"
    elif "bar chart" in q or re.search(r"\bbar\b", q):
        chart_type = "bar"
        explicit_chart_type = "bar"

    top_n = None
    top_match = re.search(r"\btop\s+(\d+)\b", q)
    if top_match:
        top_n = int(top_match.group(1))

    x_col = None
    y_col = None

    x_axis_match = re.search(r"x[- ]axis\s+([a-z0-9_ ]+)", q)
    y_axis_match = re.search(r"y[- ]axis\s+([a-z0-9_ ]+)", q)
    by_match = re.search(r"\bby\s+([a-z0-9_ ]+)", q)

    if x_axis_match:
        x_col = find_matching_column(df, x_axis_match.group(1), preferred_numeric=False)
    if y_axis_match:
        y_col = find_matching_column(df, y_axis_match.group(1), preferred_numeric=True)
    if by_match and not x_col:
        x_col = find_matching_column(df, by_match.group(1), preferred_numeric=False)

    for col in df.columns:
        if y_col:
            break
        if normalize_column_name(col) in q and col in numeric_cols:
            y_col = col

    for col in df.columns:
        if x_col:
            break
        if normalize_column_name(col) in q and col in categorical_cols:
            x_col = col

    if chart_type == "scatter":
        x_col = x_col or (numeric_cols[0] if numeric_cols else None)
        if x_col in numeric_cols and len(numeric_cols) > 1:
            y_col = y_col or next((col for col in numeric_cols if col != x_col), None)
        else:
            y_col = y_col or (numeric_cols[0] if numeric_cols else None)
    elif chart_type in {"line", "bar", "pie"}:
        x_col = x_col or (categorical_cols[0] if categorical_cols else (df.columns[0] if len(df.columns) else None))
        y_col = y_col or (numeric_cols[0] if numeric_cols else None)
    elif chart_type == "histogram":
        x_col = y_col or x_col or (numeric_cols[0] if numeric_cols else None)

    return {
        "chart_type": chart_type,
        "explicit_chart_type": explicit_chart_type,
        "x_col": x_col,
        "y_col": y_col,
        "top_n": top_n,
    }


def build_visualization(df: pd.DataFrame, question: str):
    if df.empty:
        return None, "No rows available for visualization.", None

    numeric_cols = list(df.select_dtypes(include="number").columns)
    request = parse_visualization_request(df, question)
    chart_type = request["chart_type"]
    explicit_chart_type = request["explicit_chart_type"]
    x_col = request["x_col"]
    y_col = request["y_col"]
    top_n = request["top_n"]

    try:
        plot_df = df.copy()

        if chart_type == "scatter":
            if not x_col or not y_col:
                return None, "Need two numeric columns for a scatter plot.", request
            fig = px.scatter(plot_df, x=x_col, y=y_col, title=f"{y_col} vs {x_col}")
            return fig, None, request

        if chart_type == "histogram":
            if not x_col:
                return None, "Need a numeric column for a histogram.", request
            fig = px.histogram(plot_df, x=x_col, title=f"Distribution of {x_col}")
            return fig, None, request

        if chart_type == "pie":
            if not x_col:
                return None, "Need a category column for a pie chart.", request
            if y_col:
                pie_df = plot_df[[x_col, y_col]].dropna().groupby(x_col, as_index=False).sum()
                if top_n:
                    pie_df = pie_df.nlargest(top_n, y_col)
                fig = px.pie(pie_df, names=x_col, values=y_col, title=f"{y_col} by {x_col}")
            else:
                pie_df = plot_df[x_col].value_counts(dropna=False).reset_index()
                pie_df.columns = [x_col, "count"]
                if top_n:
                    pie_df = pie_df.head(top_n)
                fig = px.pie(pie_df, names=x_col, values="count", title=f"Count by {x_col}")
            return fig, None, request

        if chart_type in {"bar", "line"}:
            if x_col and y_col:
                grouped_df = plot_df[[x_col, y_col]].dropna()
                if x_col != y_col and y_col in numeric_cols:
                    grouped_df = grouped_df.groupby(x_col, as_index=False).sum()
                if top_n and y_col in grouped_df.columns:
                    grouped_df = grouped_df.nlargest(top_n, y_col)
                if chart_type == "line":
                    fig = px.line(grouped_df, x=x_col, y=y_col, markers=True, title=f"{y_col} by {x_col}")
                else:
                    fig = px.bar(grouped_df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
                return fig, None, request

        if explicit_chart_type:
            return None, f"Could not build the requested {explicit_chart_type} chart from the current result.", request

        if numeric_cols:
            fig = px.histogram(plot_df, x=numeric_cols[0], title=f"Distribution of {numeric_cols[0]}")
            return fig, None, request
    except Exception as exc:
        return None, str(exc), request

    return None, "No suitable columns found for visualization.", request


def render_visualization_if_requested(question: str, rows: list, query_logger: QueryLogger) -> None:
    if not wants_visualization(question):
        return

    query_logger.info("visualization_requested", question=question)
    df = pd.DataFrame(rows)
    fig, error, request = build_visualization(df, question)
    query_logger.info("visualization_plan", **(request or {}))
    if fig is not None:
        st.plotly_chart(fig, use_container_width=True)
        query_logger.success(
            "visualization_rendered",
            chart_type=fig.data[0].type if fig.data else "unknown",
            x_col=request.get("x_col") if request else None,
            y_col=request.get("y_col") if request else None,
            top_n=request.get("top_n") if request else None,
        )
    else:
        query_logger.error("visualization_failed", error=error or "Unknown visualization error")
        st.info(error or "Could not build a visualization for this result.")


def render_visualization_from_history(question: str, rows: list) -> None:
    if not wants_visualization(question):
        return
    fig, _, _ = build_visualization(pd.DataFrame(rows), question)
    if fig is not None:
        st.plotly_chart(fig, use_container_width=True)


def render_message_history(history: list[dict]) -> None:
    for message in history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            rows = message.get("rows") or []
            visualization_question = message.get("visualization_question", "")
            if message["role"] == "assistant" and rows:
                render_visualization_from_history(visualization_question, rows)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def build_excel_user_message(file_name: str, table_name: str, question: str) -> str:
    if question.strip():
        return f"Uploaded `{file_name}` for `{table_name}`. Result question: {question.strip()}"
    return f"Uploaded `{file_name}` for `{table_name}` and generated SQL from the Excel formulas."

st.set_page_config(page_title="NLQ Agent", layout="wide")
settings = get_settings()
agent = ClaudeNLQAgent(settings)


def get_cached_metadata_catalog():
    cache_key = "metadata_catalog"
    if cache_key not in st.session_state:
        catalog = build_metadata_catalog(settings, allow_tables=settings.allow_tables)
        save_metadata_catalog(catalog)
        st.session_state[cache_key] = catalog
    return st.session_state[cache_key]

st.title("NLQ Agent — OpenAI + MySQL + Excel")

tab_chat, tab_excel = st.tabs(["Chat NLQ (run SQL)", "Excel → SQL → Run"])

# =========================
# TAB 1: Chat NLQ (Run SQL + Human Response)
# =========================
with tab_chat:
    if "history" not in st.session_state:
        st.session_state.history = []

    chat_header_col, chat_action_col = st.columns([6, 1])
    with chat_header_col:
        st.caption("Ask questions about your database.")
    with chat_action_col:
        if st.button("Restart", key="reset_chat"):
            st.session_state.history = []
            st.rerun()

    render_message_history(st.session_state.history)

    user_q = st.chat_input("Ask your database...")

    if user_q:
        query_logger = QueryLogger(user_q, channel="chat")
        st.session_state.history.append({"role": "user", "content": user_q})
        with st.chat_message("user"):
            st.markdown(user_q)

        schema = get_schema(settings, allow_tables=settings.allow_tables)
        metadata_catalog = get_cached_metadata_catalog()
        langchain_context = get_langchain_context(allow_tables=settings.allow_tables)
        query_logger.info(
            "schema_context",
            database=schema["database"],
            allowed_tables=list(schema["tables"].keys()),
            schema=schema["tables"],
        )
        query_logger.info(
            "metadata_catalog",
            **summarize_catalog(metadata_catalog),
        )
        query_logger.info(
            "langchain_context",
            available=langchain_context["available"],
            usable_tables=langchain_context.get("usable_tables"),
            reason=langchain_context.get("reason"),
        )
        resolved_context = resolve_query_context(user_q, metadata_catalog)
        query_logger.info(
            "derived_intent",
            **resolved_context["intent"],
        )
        query_logger.info(
            "value_resolution",
            **resolved_context["value_resolution"],
        )

        prompt = f"""
Generate ONE MySQL SELECT query only.

Allowed tables: {list(schema["tables"].keys())}
Schema: {schema["tables"]}
Metadata catalog summary:
{json.dumps(summarize_catalog(metadata_catalog), ensure_ascii=False)}

Resolved intent:
{json.dumps(resolved_context["intent"], ensure_ascii=False)}

Resolved value mapping:
{json.dumps(resolved_context["value_resolution"], ensure_ascii=False)}

LangChain table info:
{langchain_context.get("table_info", "")}

User request:
{user_q}

Rules:
- Output only SQL (single SELECT).
- Use only allowed tables.
- If a matched_value is present in the resolved value mapping, use that exact full value.
- Prefer the matched_column when filtering by the matched_value.
- Do not shorten matched values or replace them with partial words.
- Keep query minimal.
- Do NOT explain.
"""

        raw = agent.generate_sql(prompt)
        sql = extract_sql(raw)
        query_logger.info("sql_generated", raw_model_output=raw, extracted_sql=sql)

        with st.chat_message("assistant"):
            human = "No response generated."
            rows = []
            try:
                validate_select_only(sql)
                sql2 = enforce_limit(sql, max_rows=10)
                query_logger.info("sql_validated", sql=sql2, limit_rows=10)
                rows = run_query(settings, sql2)
                query_logger.success(
                    "sql_executed",
                    sql=sql2,
                    row_count=len(rows),
                    rows_preview=rows[:5],
                )

                # 🔥 HUMAN RESPONSE STEP
                human = agent.human_answer(
                    question=user_q,
                    sql_executed=sql2,
                    rows=rows
                )
                query_logger.success("human_response_generated", response=human)

                st.markdown(human)
                render_visualization_if_requested(user_q, rows, query_logger)

                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                else:
                    st.info("No rows returned.")

            except SQLRejected as e:
                query_logger.error("sql_rejected", sql=sql, error=str(e))
                st.error(f"Query rejected: {e}")
            except Exception as e:
                query_logger.error(
                    "sql_execution_failed",
                    sql=locals().get("sql2", sql),
                    error=str(e),
                )
                st.error(f"Database error: {e}")

        st.session_state.history.append({
            "role": "assistant",
            "content": human,
            "rows": rows if "rows" in locals() else [],
            "visualization_question": user_q,
        })


# =========================
# TAB 2: Excel → SQL → Run (With Human Response)
# =========================
# with tab_excel:
#     st.subheader("Upload Excel (with formulas) → Generate SQL → Run on MySQL")

#     uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
#     sheet_name = st.text_input("Sheet name (optional)", value="")
#     excel_fields_raw = st.text_area("Optional output fields (comma-separated)", value="")
#     where_clause = st.text_input("Optional WHERE clause (without WHERE)", value="")

#     excel_fields = [x.strip() for x in excel_fields_raw.split(",") if x.strip()]

#     if not uploaded:
#         st.info("Upload an Excel file to start.")
#     else:
#         file_id = str(uuid.uuid4())[:8]
#         excel_path = os.path.join(UPLOAD_DIR, f"{file_id}_{uploaded.name}")
#         with open(excel_path, "wb") as f:
#             f.write(uploaded.getbuffer())

#         excel_info = parse_excel(excel_path, sheet_name=sheet_name.strip() or None)

#         st.markdown("### Excel Headers")
#         st.write(excel_info.get("headers", []))

#         formulas = excel_info.get("formulas", [])

#         schema = get_schema(settings, allow_tables=settings.allow_tables)
#         tables = list(schema["tables"].keys())

#         if not tables:
#             st.error("No allowed tables found.")
#         else:
#             selected_table = st.selectbox("Choose DB table", tables)

#             if st.button("Generate SQL + Run"):
#                 cols = schema["tables"][selected_table]

#                 prompt = f"""
# Generate ONE MySQL SELECT query only.

# Table: {selected_table}
# Columns: {cols}

# Excel headers:
# {excel_info["headers"]}

# Excel formulas:
# {formulas}

# User requested output fields:
# {excel_fields}

# Rules:
# - Output only SQL.
# - Translate Excel formulas into SQL.
# - Use LIMIT 5.
# """

#                 raw = agent.generate_sql(prompt)
#                 sql = extract_sql(raw)

#                 try:
#                     validate_select_only(sql)
#                     sql2 = enforce_limit(sql, max_rows=5)
#                     rows = run_query(settings, sql2)

#                     # 🔥 HUMAN RESPONSE STEP
#                     human = agent.human_answer(
#                         question="Excel-based query",
#                         sql_executed=sql2,
#                         rows=rows
#                     )

#                     st.markdown(human)

#                     if rows:
#                         st.dataframe(pd.DataFrame(rows))
#                     else:
#                         st.info("No rows returned.")

#                 except SQLRejected as e:
#                     st.error(f"SQL rejected: {e}")
#                 except Exception as e:
#                     st.error(f"DB error: {e}")
with tab_excel:
    if "excel_history" not in st.session_state:
        st.session_state.excel_history = []
    if "excel_last_rows" not in st.session_state:
        st.session_state.excel_last_rows = []
    if "excel_last_sql" not in st.session_state:
        st.session_state.excel_last_sql = ""
    if "excel_last_table" not in st.session_state:
        st.session_state.excel_last_table = ""

    excel_header_col, excel_action_col = st.columns([6, 1])
    with excel_header_col:
        st.caption("Upload an Excel template and generate SQL from its formulas.")
    with excel_action_col:
        if st.button("Restart", key="reset_excel"):
            st.session_state.excel_history = []
            st.session_state.excel_last_rows = []
            st.session_state.excel_last_sql = ""
            st.session_state.excel_last_table = ""
            st.rerun()

    st.subheader("Upload Excel (columns + formulas) → Generate SQL → Run on MySQL")

    uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
    sheet_name = st.text_input("Sheet name (optional)", value="")
    where_clause = st.text_input("Optional WHERE clause (without WHERE)", value="")
    limit_rows = st.number_input("Rows to return", min_value=1, max_value=200, value=10)

    if not uploaded:
        st.info("Upload an Excel file to start.")
    else:
        file_id = str(uuid.uuid4())[:8]
        excel_path = os.path.join(UPLOAD_DIR, f"{file_id}_{uploaded.name}")
        with open(excel_path, "wb") as f:
            f.write(uploaded.getbuffer())

        excel_info = parse_excel(excel_path, sheet_name=sheet_name.strip() or None)

        formulas = excel_info.get("formulas", [])
        with st.container(border=True):
            summary_col1, summary_col2, summary_col3 = st.columns(3)
            summary_col1.metric("Headers", len(excel_info.get("headers", [])))
            summary_col2.metric("Formula Cells", len(formulas))
            summary_col3.metric("Sheet", excel_info.get("sheet", ""))
            with st.expander("Template details"):
                st.write(excel_info.get("headers", []))
                st.json(formulas[:20] if formulas else [])

        schema = get_schema(settings, allow_tables=settings.allow_tables)
        tables = list(schema["tables"].keys())

        if not tables:
            st.error("No allowed tables found. Check MYSQL_DB and ALLOW_TABLES in .env.")
        else:
            selected_table = st.selectbox("Choose DB table to query", tables)
            table_cols = schema["tables"][selected_table]   # dict: col->type
            render_message_history(st.session_state.excel_history)

            if st.button("Build SQL from Excel + Run"):
                question_label = f"Excel template query for table {selected_table}"
                query_logger = QueryLogger(question_label, channel="excel")
                human = "No response generated."
                rows = []
                st.session_state.excel_history.append({
                    "role": "user",
                    "content": build_excel_user_message(uploaded.name, selected_table, ""),
                })
                query_logger.info(
                    "schema_context",
                    database=schema["database"],
                    selected_table=selected_table,
                    table_schema=table_cols,
                    excel_headers=excel_info["headers"],
                    excel_formulas=formulas,
                    where_clause=where_clause.strip(),
                    limit_rows=int(limit_rows),
                )
                query_logger.info(
                    "derived_intent",
                    **derive_intent(question_label, {selected_table: table_cols}),
                )

                # Prompt LLM to convert Excel formulas into SQL expressions
                prompt = f"""
You will convert an Excel template into a MySQL SELECT query.

DB table: {selected_table}
DB columns (name:type): {table_cols}

Excel headers: {excel_info["headers"]}
Excel header_to_col (header->Excel column letter): {excel_info["header_to_col"]}

Excel formulas (cell/header/formula):
{formulas}

Task:
- Produce ONE MySQL SELECT query that returns:
  1) all Excel headers that match DB columns, and
  2) computed columns for formula headers using SQL expressions.
- Map Excel header names to DB column names by exact match first, then closest match.
- Translate Excel formulas:
  - Replace references like B2, C2 using the header_to_col mapping:
      Example: if B is Ply_Width, then B2 -> Ply_Width
  - IF(x,y,z) -> CASE WHEN x THEN y ELSE z END
  - Division A/B -> A / NULLIF(B,0)
  - Use COALESCE if needed
- Do not include Excel row numbers in SQL; use DB column names.
- Add LIMIT {limit_rows}.
- Return ONLY SQL (no explanation).

Optional filter:
{("WHERE " + where_clause) if where_clause.strip() else "(none)"}
"""

                raw = agent.generate_sql(prompt, max_tokens=900)
                sql = extract_sql(raw)
                query_logger.info("sql_generated", raw_model_output=raw, extracted_sql=sql)

                if appears_truncated_sql(sql):
                    query_logger.error("sql_truncated", sql=sql)
                    repaired_sql = agent.repair_sql(
                        user_intent="Excel template query",
                        schema={selected_table: table_cols},
                        failed_sql=sql,
                        db_error="Generated SQL appears truncated or incomplete",
                        extra_context=json.dumps(
                            {
                                "excel_headers": excel_info["headers"],
                                "excel_formulas": formulas[:20],
                                "where_clause": where_clause.strip(),
                                "limit_rows": int(limit_rows),
                            },
                            ensure_ascii=False,
                        ),
                    )
                    sql = extract_sql(repaired_sql)
                    query_logger.success("sql_repaired", repaired_sql=sql)

                # Run SQL
                try:
                    validate_select_only(sql)
                    sql2 = enforce_limit(sql, max_rows=int(limit_rows))
                    query_logger.info("sql_validated", sql=sql2, limit_rows=int(limit_rows))

                    rows = run_query(settings, sql2)
                    df = pd.DataFrame(rows)
                    st.session_state.excel_last_rows = rows
                    st.session_state.excel_last_sql = sql2
                    st.session_state.excel_last_table = selected_table
                    query_logger.success(
                        "sql_executed",
                        sql=sql2,
                        row_count=len(rows),
                        rows_preview=rows[:5],
                    )

                    human = f"Generated `{len(rows)}` row(s) from the Excel template."
                    query_logger.success("human_response_generated", response=human)

                    st.markdown(human)

                    if not df.empty:
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No rows returned.")

                except SQLRejected as e:
                    query_logger.error("sql_rejected", sql=sql, error=str(e))
                    st.error(f"SQL rejected: {e}")
                except Exception as e:
                    failed_sql = locals().get("sql2", sql)
                    query_logger.error(
                        "sql_execution_failed",
                        sql=failed_sql,
                        error=str(e),
                    )
                    try:
                        repaired_sql = agent.repair_sql(
                            user_intent="Excel template query",
                            schema={selected_table: table_cols},
                            failed_sql=failed_sql,
                            db_error=str(e),
                            extra_context=json.dumps(
                                {
                                    "excel_headers": excel_info["headers"],
                                    "excel_formulas": formulas[:20],
                                    "where_clause": where_clause.strip(),
                                    "limit_rows": int(limit_rows),
                                },
                                ensure_ascii=False,
                            ),
                        )
                        sql_retry = enforce_limit(extract_sql(repaired_sql), max_rows=int(limit_rows))
                        query_logger.success("sql_repaired", repaired_sql=sql_retry)
                        rows = run_query(settings, sql_retry)
                        df = pd.DataFrame(rows)
                        st.session_state.excel_last_rows = rows
                        st.session_state.excel_last_sql = sql_retry
                        st.session_state.excel_last_table = selected_table
                        query_logger.success(
                            "sql_executed_after_repair",
                            sql=sql_retry,
                            row_count=len(rows),
                            rows_preview=rows[:5],
                        )

                        human = f"Generated `{len(rows)}` row(s) from the Excel template."
                        query_logger.success("human_response_generated", response=human)

                        st.markdown(human)
                        if not df.empty:
                            st.dataframe(df, use_container_width=True, hide_index=True)
                        else:
                            st.info("No rows returned.")
                    except Exception as repair_error:
                        query_logger.error(
                            "sql_repair_failed",
                            sql=failed_sql,
                            error=str(repair_error),
                        )
                        st.error(f"DB error: {e}")

                st.session_state.excel_history.append({
                    "role": "assistant",
                    "content": human if "human" in locals() else "No response generated.",
                    "rows": rows if "rows" in locals() else [],
                    "visualization_question": "",
                })

            if st.session_state.excel_last_rows:
                st.divider()
                followup_question = st.text_input(
                    "Ask a question about the generated result",
                    key="excel_followup_question",
                    placeholder="Example: Which materials are below safety stock? Show a plot by material type.",
                )

                if st.button("Ask About Generated Result"):
                    followup_question = followup_question.strip()
                    if followup_question:
                        query_logger = QueryLogger(followup_question, channel="excel")
                        st.session_state.excel_history.append({
                            "role": "user",
                            "content": followup_question,
                        })
                        query_logger.info(
                            "excel_followup_context",
                            selected_table=st.session_state.excel_last_table,
                            sql_executed=st.session_state.excel_last_sql,
                            row_count=len(st.session_state.excel_last_rows),
                        )

                        human = agent.human_answer(
                            question=followup_question,
                            sql_executed=st.session_state.excel_last_sql,
                            rows=st.session_state.excel_last_rows,
                            extra_notes="Answer only from the generated result rows and explain computed columns clearly if present.",
                        )
                        query_logger.success("human_response_generated", response=human)

                        st.session_state.excel_history.append({
                            "role": "assistant",
                            "content": human,
                            "rows": st.session_state.excel_last_rows,
                            "visualization_question": followup_question,
                        })
                        st.rerun()
