import os
import json
from openai import OpenAI
from nlq.config import Settings

# 1) Prompt to generate SQL only
SQL_SYSTEM_PROMPT = """
You are an NLQ agent that generates SAFE MySQL SELECT queries only.

Rules:
- Return ONLY a single SELECT query (no markdown, no explanation).
- No INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE.
- Use only the tables/columns provided in schema (if given).
- If the user asks for "first row", "second row", or "first and second row",
  you MUST use ORDER BY on a stable column (id/timestamp/createdAt if available).
- If unsure, return best possible SELECT with LIMIT 10.
"""

# 2) Prompt to convert DB rows into a human response
ANSWER_SYSTEM_PROMPT = """
You are a helpful data assistant.

You will be given:
- the user's question,
- the SQL that was executed,
- and the resulting rows (JSON).

Your job:
- Answer in a human, friendly way.
- If the question asks for a calculation (sum/addition/etc), show the values and the math clearly.
- Keep it short and clear.
- Do NOT invent data. Use only the provided rows.
- Do not mention tokens or internal system details.
"""

# 3) Prompt to repair SQL if DB execution fails
REPAIR_SYSTEM_PROMPT = """
You are a MySQL SQL repair assistant.

You will receive JSON with:
- user_intent (what user wanted)
- schema (allowed tables/columns)
- failed_sql (the SQL that failed)
- db_error (MySQL error message)
- extra_context (optional)

Return ONLY valid JSON:
{
  "sql": "SINGLE SELECT QUERY"
}

Rules:
- Fix the SQL so it runs on MySQL.
- Must be SELECT-only.
- Use only allowed tables/columns from schema.
- If a column name is wrong, pick the closest valid column from schema.
- If the query needs an ORDER BY for "first/second row", add it using a stable column.
- Keep it minimal and correct.
- No markdown. No explanations.
"""

class ClaudeNLQAgent:
    def __init__(self, settings: Settings):
        self.settings = settings

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in .env")

        self.client = OpenAI(api_key=api_key)

        # Read models from .env with defaults
        self.primary_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.fallback_model = os.getenv("OPENAI_FALLBACK_MODEL", "gpt-4o-mini")

        # Token safety limits
        self.max_tokens_sql = int(os.getenv("OPENAI_MAX_TOKENS_SQL", "220"))
        self.max_tokens_answer = int(os.getenv("OPENAI_MAX_TOKENS_ANSWER", "220"))
        self.max_tokens_repair = int(os.getenv("OPENAI_MAX_TOKENS_REPAIR", "300"))

    def _call_model(self, model_name: str, system_prompt: str, user_message: str, max_tokens: int):
        return self.client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=0,
            max_tokens=max_tokens,
            top_p=1,
            timeout=25,
        )

    # ---------- 1) SQL generator ----------
    def generate_sql(self, user_message: str, max_tokens: int | None = None) -> str:
        """Returns SQL only (SELECT)."""
        token_limit = max_tokens or self.max_tokens_sql
        try:
            resp = self._call_model(self.primary_model, SQL_SYSTEM_PROMPT, user_message, token_limit)
            return resp.choices[0].message.content.strip()
        except Exception:
            resp = self._call_model(self.fallback_model, SQL_SYSTEM_PROMPT, user_message, token_limit)
            return resp.choices[0].message.content.strip()

    # ---------- 2) Human response generator ----------
    def human_answer(self, question: str, sql_executed: str, rows: list, extra_notes: str = "") -> str:
        """Converts DB query output into a human answer (uses only first 10 rows)."""
        rows_small = rows[:10]

        payload = {
            "question": question,
            "sql_executed": sql_executed,
            "rows": rows_small,
            "notes": extra_notes
        }

        user_message = "Use the following JSON to answer:\n" + json.dumps(payload, ensure_ascii=False)

        try:
            resp = self._call_model(self.primary_model, ANSWER_SYSTEM_PROMPT, user_message, self.max_tokens_answer)
            return resp.choices[0].message.content.strip()
        except Exception as e:
            try:
                resp = self._call_model(self.fallback_model, ANSWER_SYSTEM_PROMPT, user_message, self.max_tokens_answer)
                return resp.choices[0].message.content.strip()
            except Exception as e2:
                return f"❌ OpenAI request failed.\nPrimary error: {e}\nFallback error: {e2}"

    # ---------- 3) SQL repair ----------
    def repair_sql(self, user_intent: str, schema: dict, failed_sql: str, db_error: str, extra_context: str = "") -> str:
        """
        Repairs failing SQL based on MySQL error.
        Returns SQL (string).
        """
        payload = {
            "user_intent": user_intent,
            "schema": schema,
            "failed_sql": failed_sql,
            "db_error": db_error,
            "extra_context": extra_context
        }
        user_message = json.dumps(payload, ensure_ascii=False)

        def _parse_sql_from_json(text: str) -> str:
            obj = json.loads(text)
            if not isinstance(obj, dict) or "sql" not in obj:
                raise ValueError("Repair response JSON missing 'sql'")
            return str(obj["sql"]).strip()

        try:
            resp = self._call_model(self.primary_model, REPAIR_SYSTEM_PROMPT, user_message, self.max_tokens_repair)
            return _parse_sql_from_json(resp.choices[0].message.content.strip())
        except Exception:
            resp = self._call_model(self.fallback_model, REPAIR_SYSTEM_PROMPT, user_message, self.max_tokens_repair)
            return _parse_sql_from_json(resp.choices[0].message.content.strip())

    # Backward compatible method name
    def chat(self, user_message: str) -> str:
        return self.generate_sql(user_message)
