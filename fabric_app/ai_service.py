from __future__ import annotations

import json
import logging
import os
import re
import textwrap
from typing import Dict, List, Optional

import yaml
from openai import AzureOpenAI

from config import Config
from db import (
    get_table_columns,
    get_primary_keys,
    list_tables_in_schema,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Azure OpenAI client
# ---------------------------------------------------------------------------

_client: Optional[AzureOpenAI] = None


def _get_client() -> AzureOpenAI:
    """Lazily initialize Azure OpenAI client to avoid import-time crashes."""
    global _client
    if _client is not None:
        return _client

    missing = []
    if not Config.AZURE_OPENAI_ENDPOINT:
        missing.append("AZURE_OPENAI_ENDPOINT")
    if not Config.AZURE_OPENAI_API_KEY:
        missing.append("AZURE_OPENAI_API_KEY")
    if not Config.AZURE_OPENAI_API_VERSION:
        missing.append("AZURE_OPENAI_API_VERSION")

    if missing:
        raise RuntimeError(
            "Azure OpenAI is not configured. Missing: " + ", ".join(missing)
        )

    _client = AzureOpenAI(
        azure_endpoint=Config.AZURE_OPENAI_ENDPOINT,
        api_key=Config.AZURE_OPENAI_API_KEY,
        api_version=Config.AZURE_OPENAI_API_VERSION,
    )
    return _client


# ---------------------------------------------------------------------------
# Schema prompt builder
# ---------------------------------------------------------------------------

def load_schema_from_yaml(yaml_path: str | None = None) -> str:
    """
    Build the SQL-generation system prompt from schema_model.yaml.
    Falls back to a minimal prompt if the file is missing.

    Resolved relative to THIS FILE's directory when a bare filename is
    given (same reasoning as config.py's _load_app_settings - the default
    Config.SCHEMA_METADATA_FILE is just "schema_model.yaml", which would
    otherwise only be found if the process's current working directory
    happened to be backend/fabric_app/).
    """
    path = yaml_path or Config.SCHEMA_METADATA_FILE
    if not os.path.isabs(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            schema_data = yaml.safe_load(fh)
    except FileNotFoundError:
        logger.warning(
            "Schema metadata file not found: %s. Using minimal prompt.", path)
        return (
            f"You are a SQL expert for Microsoft Fabric SQL (T-SQL).\n"
            f"DATABASE: {Config.FABRIC_READYTOBUILD_DATABASE}\nSCHEMA: {Config.SCHEMA}\n"
            "Generate T-SQL SELECT queries only. Use DATETRUNC, GETDATE, TOPN syntax."
        )

    prompt = textwrap.dedent(f"""
        You are a SQL expert for Microsoft Fabric SQL (T-SQL).

        DATABASE: {Config.FABRIC_READYTOBUILD_DATABASE}
        SCHEMA: {Config.SCHEMA}

    """)

    if "custom_instructions" in schema_data:
        prompt += schema_data["custom_instructions"] + "\n\n"

    prompt += "AVAILABLE TABLES:\n\n"

    for table in schema_data.get("tables", []):
        actual_schema = table.get("schema") or schema_data.get("schema", Config.SCHEMA)
        actual_table = table.get("table_name", "")

        # Real schema_model.yaml keys each table's columns under "columns"
        # (list of dicts), while "dimensions"/"measures" are just flat
        # lists of column-name strings that classify those columns.
        # Build a name -> column-detail lookup so we can enrich each one.
        columns_by_name = {
            col.get("name"): col
            for col in table.get("columns", [])
            if isinstance(col, dict) and col.get("name")
        }

        prompt += f"Table: {actual_schema}.{actual_table}\n"
        prompt += f"Friendly name: {table.get('display_name', '')} (DO NOT use in SQL)\n"
        prompt += f"Description: {table.get('description', '')}\n"
        prompt += "Columns:\n"

        for dim_name in table.get("dimensions", []):
            col = columns_by_name.get(dim_name, {})
            dtype = col.get("datatype", "varchar")
            desc = col.get("business_description", "")
            syns = col.get("synonyms", [])
            prompt += f"  - {dim_name} ({dtype}): {desc}\n"
            if syns:
                prompt += f"    Synonyms: {', '.join(syns)}\n"

        for measure_name in table.get("measures", []):
            col = columns_by_name.get(measure_name, {})
            dtype = col.get("datatype", "decimal")
            desc = col.get("business_description", "")
            prompt += f"  - {measure_name} ({dtype}): {desc}\n"

        prompt += "\n"

    if "verified_queries" in schema_data:
        prompt += "VERIFIED QUERIES - USE THESE EXACT QUERIES WHEN THE QUESTION MATCHES:\n\n"
        for vq in schema_data.get("verified_queries", []):
            q = vq.get("question", "")
            sql = vq.get("sql", "").strip()
            if q and sql:
                prompt += f"Question: {q}\nSQL:\n{sql}\n\n"

    prompt += textwrap.dedent("""
        CRITICAL SQL GENERATION RULES:
        1. Always use fully qualified table names: schema.table_name
        2. Never use friendly names in SQL - use actual table names
        3. Table names are case-sensitive
        4. Only generate SELECT queries
        5. Use T-SQL syntax: TOPN instead of LIMIT

        DATE FUNCTIONS (T-SQL):
          Month grouping : DATETRUNC(month, column)
          Current date   : GETDATE()
          6 months ago   : DATEADD(month, -6, GETDATE())
          Never use      : DATE_TRUNC, CURRENT_DATE()

        WINDOW FUNCTIONS for top-N per group:
          Use ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
          Never use bare TOP N for per-group rankings.

        OUTPUT FORMAT (ABSOLUTE RULE):
          - Return ONLY the raw SQL query.
          - No markdown, no code fences, no explanations, no bullet points.
          - Do NOT write "Descriptive:", "Prescriptive:", or any prose.
          - If the question is vague or conversational, return exactly:
            SELECT 'Please ask a specific question about work orders, BOM, parts, shortages, or suppliers.' AS MESSAGE
          - The very first character of your response must be S (for SELECT) or W (for WITH).
    """)

    return prompt.replace("{schema}", Config.SCHEMA)


# Preload system prompt at module import time
SYSTEM_PROMPT = load_schema_from_yaml()


# ---------------------------------------------------------------------------
# SQL generation (cache-first, req 8)
# ---------------------------------------------------------------------------

def _clean_sql(raw: str) -> str:
    """Remove markdown fences and common dialect issues from generated SQL."""
    sql = raw.replace("```sql", "").replace("```", "").strip()

    sql = sql.replace("CURRENT_DATE()", "GETDATE()").replace(
        "CURRENT_DATE", "GETDATE()")

    replacements = [
        (r"DATE_TRUNC\('month',\s*",   "DATETRUNC(month, "),
        (r"DATE_TRUNC\('quarter',\s*", "DATETRUNC(quarter, "),
        (r"DATE_TRUNC\('year',\s*",    "DATETRUNC(year, "),
        (r"DATEADD\('month',\s*",      "DATEADD(month, "),
        (r"DATEADD\('year',\s*",       "DATEADD(year, "),
        (r"DATEADD\('day',\s*",        "DATEADD(day, "),
    ]
    for pattern, repl in replacements:
        sql = re.sub(pattern, repl, sql, flags=re.IGNORECASE)

    # LIMIT N -> TOP N
    m = re.search(r"\bLIMIT\s+(\d+)", sql, re.IGNORECASE)
    if m:
        limit_n = m.group(1)
        sql = re.sub(r"\bLIMIT\s+\d+\b", "", sql, flags=re.IGNORECASE)
        sql = re.sub(
            r"\bSELECT\b", f"SELECT TOP{limit_n}", sql, count=1, flags=re.IGNORECASE)

    # Wrap ORDER BY CASE in MIN() when inside aggregate context
    if re.search(r"ORDER\s+BY\s+CASE\s+WHEN", sql, re.IGNORECASE):
        def _wrap_case(match: re.Match) -> str:
            prefix = match.group(1)
            expr = match.group(2)
            if not re.match(r"^\s*(MIN|MAX|AVG|SUM|COUNT)\s*\(", expr, re.IGNORECASE):
                return f"{prefix}MIN({expr})"
            return match.group(0)

        sql = re.sub(
            r"(ORDER\s+BY\s+)(CASE\s+WHEN.*?END)(?=\s*(?:ASC|DESC)?\s*(?:,|$))",
            _wrap_case,
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )

    sql = re.sub(r"\bCOUNT\s*\(\s*\)", "COUNT(*)", sql, flags=re.IGNORECASE)
    sql = _remove_cte_order_by(sql)
    return sql


def _remove_cte_order_by(sql: str) -> str:
    """Strip ORDER BY clauses that appear immediately before a closing CTE paren."""
    lines = sql.split("\n")
    result = []
    for idx, line in enumerate(lines):
        stripped = line.strip().upper()
        if stripped.startswith("ORDER BY"):
            is_cte = False
            for j in range(idx + 1, len(lines)):
                nxt = lines[j].strip()
                if nxt:
                    is_cte = nxt.startswith(")")
                    break
            if is_cte:
                continue
        result.append(line)
    return "\n".join(result)


def generate_sql(
    user_question: str,
    temperature: float | None = None,
    short_memory: str | None = None,
    long_memory: str | None = None,
) -> str:
    """
    Convert a natural language question into a T-SQL query.

    short_memory/long_memory (optional recent-conversation / known-user-context
    text from the frontend) are folded directly into the prompt sent to Azure
    OpenAI, the same way Snowflake's call_cortex_analyst folds them into
    analyst_input before calling Cortex Analyst - this is what lets a
    follow-up question like "what about last month?" resolve correctly
    using the prior turn's context. Previously this function ignored both
    params entirely, so every question was generated with zero awareness
    of the conversation so far, regardless of what the frontend sent.

    Flow:
      1. Call Azure OpenAI with the schema system prompt + question (+ memory).
      2. Clean/validate the returned SQL.

    NOTE: this used to check a Warehouse-table-backed cache (db.cache_get)
    first. That cache's write side (db.cache_set) is never called anywhere
    in this codebase, so every lookup was a guaranteed, permanent miss -
    pure wasted latency, plus a guaranteed-failing GENIE_CONTEXT_MEMORY
    logging call on every single question (see genie_middleware.py). Removed;
    copilot_service.copilot_chat now has its own real, working cache
    (mirroring Snowflake's in-process TTL cache) at the point that actually
    matters - the full formatted answer - so caching a bare SQL string here
    added no value even before accounting for the fact that it never worked.
    """
    temperature = temperature if temperature is not None else float(
        Config._settings.get("ai", {}).get("sql_generation_temperature", 0.1)
        if hasattr(Config, "_settings") else 0.1
    )

    user_content = user_question
    mem_parts: list[str] = []
    if short_memory:
        mem_parts.append(f"Short memory (recent conversation):\n{short_memory}")
    if long_memory:
        mem_parts.append(f"Long memory (important context):\n{long_memory}")
    if mem_parts:
        user_content = f"{user_question}\n\n" + "\n\n".join(mem_parts)

    try:
        response = _get_client().chat.completions.create(
            model=Config.AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_content},
            ],
            temperature=temperature,
            max_tokens=Config._settings.get("ai", {}).get("max_tokens", 4096)
            if hasattr(Config, "_settings") else 4096,
        )
        raw_sql = response.choices[0].message.content.strip()
        sql = _clean_sql(raw_sql)

        # Guard: if the model returned prose instead of SQL, return a safe fallback
        sql_upper = sql.lstrip(";").strip().upper()
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            logger.warning(
                "generate_sql returned non-SQL response (first 80 chars): %s",
                sql[:80]
            )
            # Try to extract any SELECT/WITH block from the prose
            import re as _re
            match = _re.search(r"(WITH\s+\w|SELECT\s+)", sql, _re.IGNORECASE)
            if match:
                sql = sql[match.start():].strip()
                logger.info(
                    "Extracted SQL from prose starting at position %d", match.start())
            else:
                sql = (
                    "SELECT 'Please ask a specific question about work orders, "
                    "BOM, parts, shortages, or suppliers.' AS MESSAGE"
                )

        return sql

    except Exception as exc:
        raise RuntimeError(f"SQL generation failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Generic AI completion
# ---------------------------------------------------------------------------

def cortex_complete(
    prompt: str,
    model: Optional[str] = None,
    temperature: float = 0.3,
) -> str:
    """
    General-purpose Azure OpenAI completion.
    Replaces SNOWFLAKE.CORTEX.COMPLETE.
    """
    try:
        response = _get_client().chat.completions.create(
            model=model or Config.PRESCRIPTIVE_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("AI completion failed: %s", exc)
        return f"AI completion unavailable: {exc}"


# ---------------------------------------------------------------------------
# Generic AI completion Model To return in JSON format
# ---------------------------------------------------------------------------


def cortex_complete_json(
    prompt: str,
    model: Optional[str] = None,
    temperature: float = 0.3,
) -> str:
    """
    General-purpose Azure OpenAI completion.
    """
    try:
        response = _get_client().chat.completions.create(
            model=model or Config.PRESCRIPTIVE_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("AI completion failed: %s", exc)
        return f"AI completion unavailable: {exc}"


# ---------------------------------------------------------------------------
# Predictive insights
# ---------------------------------------------------------------------------
def generate_prescriptive_insights(summary: str, question: str) -> str:
    """
    Generate short, actionable prescriptive recommendations from an
    already-computed summary (no SQL execution) using Azure OpenAI.
    """
    start_time = time.time()

    prompt = (
        "You are a manufacturing/procurement business analyst. "
        "Based on the following summary of analytics results, provide "
        "EXACTLY 3-5 specific, actionable prescriptive recommendations. "
        "Be concrete: cite numbers, names, and amounts from the summary where "
        "available. Format as a numbered list, each item on its own line. "
        "Do NOT use generic phrases like 'review the data'.\n\n"
        f"User question: {question}\n\n"
        f"Summary:\n{summary}"
    )

    try:
        result = cortex_complete(prompt, temperature=0.3)
        duration = round(time.time() - start_time, 3)

        if result and len(result.strip()) > 20:
            result_clean = result.strip()
            log_event("AI_PRESCRIPTIVE", {
                "summary": result_clean[:200],
                "full_answer": result_clean,
                "relevance": 0.95,
                "details": f"LLM response time: {duration}s",
            })
            return result_clean

        log_event("AI_EMPTY", {
            "summary": "LLM returned empty/weak prescriptive response",
            "details": f"Time: {duration}s",
            "relevance": 0.2,
        })

    except Exception as e:
        duration = round(time.time() - start_time, 3)
        log_event("AI_ERROR", {
            "summary": "LLM prescriptive insights failed",
            "details": f"{str(e)} | Time: {duration}s",
            "relevance": 0.0,
        })
        logger.error(f"Prescriptive insights failed: {e}")

    return ""


def _cortex_complete_forecast(content: list, run_df_func, question: str) -> str:
    """Generate predictive insights (forecasts & trends) using Azure OpenAI with middleware logging."""
    
    start_time = time.time()

    data_parts = []
    executed_sqls = []

    for block in content or []:
        if block.get("type") != "sql":
            continue

        sql = block.get("statement", "")
        if not sql.strip():
            continue

        try:
            df = run_df_func(sql)
            executed_sqls.append(sql)

            if df is None or df.empty:
                continue

            head = df.head(40)
            data_parts.append(head.to_string(index=False, max_colwidth=40))

        except Exception as e:
            logger.warning(f"Failed to execute SQL block: {e}")
            continue
    
    if not data_parts:
        return ""
    
    data_str = "\n\n---\n\n".join(data_parts)

    # Limit payload size
    if len(data_str) > 15000:
        data_str = data_str[:] + "\n(truncated)"
    
    prompt = (
    "You are a procurement business analyst with forecasting expertise. "
    "The user asked a question and received the following data from our analytics. "
    "Analyze this historical data and provide predictive insights. "
    "Your output must include: "
    "1. Vendor-specific forecasts with **numeric projections** (e.g., counts for Q4 2024, Q1 2025). "
    "2. **Scenario analysis**: best case, worst case, and most likely outcomes. "
    "3. **Confidence ranges** (e.g., 70–80% likelihood). "
    "4. Risks and opportunities tied directly to the metrics. "
    "Format as a numbered list, each item on a NEW LINE (1. ...\\n2. ...\\n3. ...). "
    "Bold ALL key predictions, numbers, and timeframes using ** (e.g., **increase 15%**, **Q2 2026**). "
    "Do NOT use HTML tags. "
    "Each numbered point must start on its own line.\n\n"
    f"User question: {question}\n\n"
    f"Historical Data:\n{data_str}"
    )

    
    try:
        result = cortex_complete(prompt, temperature=0.4)

        duration = round(time.time() - start_time, 3)

        if result and len(result.strip()) > 20:
            result_clean = result.strip()

            # 🔥 Middleware Logging (SUCCESS)
            log_event("AI_PREDICTIVE", {
                "summary": result_clean[:],
                "full_answer": result_clean,
                "sql": " | ".join(executed_sqls)[:],
                "relevance": 0.95,
                "details": f"LLM response time: {duration}s"
            })

            return result_clean

        # 🔹 Edge case: empty/weak response
        log_event("AI_EMPTY", {
            "summary": "LLM returned empty/weak predictive response",
            "details": f"Time: {duration}s",
            "relevance": 0.2
        })

    except Exception as e:
        duration = round(time.time() - start_time, 3)

        # 🔥 Middleware Logging (ERROR)
        log_event("AI_ERROR", {
            "summary": "LLM predictive insights failed",
            "details": f"{str(e)} | Time: {duration}s",
            "relevance": 0.0
        })

        logger.error(f"Predictive insights failed: {e}")
    
    return ""

def _cortex_complete_predictive(content: list, run_df_func, question: str) -> str:
    """Generate predictive insights using Azure OpenAI with middleware logging."""
    # NOTE: this is the version that actually runs at import time — the
    # earlier same-named function (now `_cortex_complete_forecast`) used to
    # be silently shadowed by this one until the rename above.
    
    start_time = time.time()
    
    data_parts = []
    executed_sqls = []

    for block in content or []:
        if block.get("type") != "sql":
            continue

        sql = block.get("statement", "")
        if not sql.strip():
            continue

        try:
            df = run_df_func(sql)
            executed_sqls.append(sql)

            if df is None or df.empty:
                continue

            head = df.head(40)
            data_parts.append(head.to_string(index=False, max_colwidth=40))

        except Exception as e:
            logger.warning(f"Failed to execute SQL block: {e}")
            continue
    
    if not data_parts:
        return ""
    
    data_str = "\n\n---\n\n".join(data_parts)

    # Limit payload size
    if len(data_str) > 15000:
        data_str = data_str[:15000] + "\n(truncated)"
    
    prompt = (
        "You are a sales & operations business analyst. The user asked a question and received the following data from our analytics. "
        "Provide predictive insights: forecasts and trends based on the data. "
        "Be concrete: cite numbers, dealer names, product names, amounts, and percentages from the data. "
        "Format as bullet points (use •). Do NOT use generic phrases like 'review the data'—give actionable recommendations.\n\n"
        f"User question: {question}\n\n"
        f"Data:\n{data_str}"
    )
    
    try:
        result = cortex_complete(prompt, temperature=0.3)

        duration = round(time.time() - start_time, 3)

        if result and len(result.strip()) > 20:
            result_clean = result.strip()

            # 🔥 Middleware Logging (SUCCESS)
            log_event("AI_INSIGHT", {
                "summary": result_clean[:200],
                "full_answer": result_clean,
                "sql": " | ".join(executed_sqls)[:1000],
                "relevance": 0.95,
                "details": f"LLM response time: {duration}s"
            })

            return result_clean

        # 🔹 Edge case: empty/weak response
        log_event("AI_EMPTY", {
            "summary": "LLM returned empty/weak response",
            "details": f"Time: {duration}s",
            "relevance": 0.2
        })

    except Exception as e:
        duration = round(time.time() - start_time, 3)

        # 🔥 Middleware Logging (ERROR)
        log_event("AI_ERROR", {
            "summary": "LLM failed",
            "details": f"{str(e)} | Time: {duration}s",
            "relevance": 0.0
        })

        logger.error(f"Predictive insights failed: {e}")
    
    return ""
# ---------------------------------------------------------------------------
#  YAML enrichment (req 4, 7)
# ---------------------------------------------------------------------------


def _infer_data_vault_objects(
    table_name: str,
    columns: List[Dict],
    primary_keys: List[str],
) -> Dict:
    """
    Use Azure OpenAI to infer Data Vault 2.0 Hub, Satellite, Link, and
    hierarchy definitions for a given table schema.

    Returns a dict suitable for merging into schema_metadata.yaml.
    """
    col_descriptions = "\n".join(
        f"  - {c['COLUMN_NAME']} ({c['DATA_TYPE']}, nullable={c['IS_NULLABLE']})"
        for c in columns
    )
    pk_list = ", ".join(primary_keys) if primary_keys else "unknown"

    prompt = textwrap.dedent(f"""
        You are a Data Vault 2.0 architect.

        Given the table schema below, produce JSON with the following structure:

        {{
          "data_vault": {{
            "hub": {{
              "name": "<{Config.DATA_VAULT_HUB_PREFIX}tablename>",
              "business_keys": ["<col1>", ...],
              "description": "<one sentence>"
            }},
            "satellite": {{
              "name": "<{Config.DATA_VAULT_SAT_PREFIX}tablename>",
              "descriptive_attributes": ["<col1>", ...],
              "description": "<one sentence>"
            }},
            "links": [
              {{
                "name": "<{Config.DATA_VAULT_LINK_PREFIX}tablename_othertable>",
                "hubs": ["<hub1>", "<hub2>"],
                "description": "<one sentence>"
              }}
            ],
            "hierarchy": {{
              "parent_column": "<col>",
              "child_column":  "<col>",
              "description": "<one sentence or null>"
            }}
          }},
          "sample_questions": [
            "<natural language question 1>",
            "<natural language question 2>",
            "<natural language question 3>"
          ]
        }}

        TABLE NAME: {table_name}
        PRIMARY KEYS: {pk_list}
        COLUMNS:
        {col_descriptions}

        Return only valid JSON, no markdown, no explanation.
    """).strip()

    try:
        raw = cortex_complete(
            prompt,
            temperature=float(
                Config._settings.get("ai", {}).get(
                    "yaml_generation_temperature", 0.2)
                if hasattr(Config, "_settings") else 0.2
            ),
        )
        # Strip accidental markdown fences
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Data Vault inference failed for %s: %s",
                       table_name, exc)
        return {}


def enrich_yaml_for_table(
    table_name: str,
    schema: str = "INFORMATION_MART",
    yaml_path: str | None = None,
) -> bool:
    """
    AI-driven YAML enrichment for a single table (req 4, 7).

    Steps:
      1. Fetch column metadata from the Fabric Lakehouse.
      2. Call Azure OpenAI to infer Data Vault objects and sample questions.
      3. Merge the result into schema_metadata.yaml under the matching table
         entry (or create a new entry if absent).

    Returns True on success.
    """
    if not Config.ENABLE_DATA_VAULT_AI:
        logger.info(
            "Data Vault AI enrichment is disabled in app_settings.yaml.")
        return False

    yaml_path = yaml_path or Config.SCHEMA_METADATA_FILE

    # Load existing YAML
    try:
        with open(yaml_path, "r", encoding="utf-8") as fh:
            schema_data = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        schema_data = {"tables": [], "verified_queries": []}

    tables = schema_data.get("tables", [])

    # Check if this table is already fully enriched
    existing = next(
        (t for t in tables if t.get("base_table", {}).get(
            "table", "").upper() == table_name.upper()),
        None,
    )
    if existing and existing.get("data_vault"):
        logger.info(
            "Table %s already has Data Vault metadata. Skipping.", table_name)
        return True

    # Fetch schema from Fabric
    cols_df = get_table_columns(table_name, schema)
    if cols_df.empty:
        logger.warning("No columns found for %s.%s", schema, table_name)
        return False

    pks = get_primary_keys(table_name, schema)
    columns = cols_df.to_dict("records")

    # AI inference
    enrichment = _infer_data_vault_objects(table_name, columns, pks)
    if not enrichment:
        return False

    # Merge into YAML
    if existing is None:
        # Build a new minimal table entry
        new_entry: Dict = {
            "name": table_name.lower(),
            "description": f"Auto-discovered table: {schema}.{table_name}",
            "base_table": {"database": Config.FABRIC_ORDERLENS_DATABASE, "schema": schema, "table": table_name},
        }
        if pks:
            new_entry["primary_key"] = {"columns": [k.lower() for k in pks]}
        # Add inferred columns as dimensions/measures
        dims, measures = [], []
        for col in columns:
            col_name = col["COLUMN_NAME"]
            col_type = col["DATA_TYPE"].lower()
            if any(t in col_type for t in ("int", "float", "decimal", "numeric", "money")):
                measures.append({"name": col_name.lower(), "expr": col_name, "data_type": "number",
                                 "default_aggregation": "sum"})
            else:
                dims.append({"name": col_name.lower(),
                            "expr": col_name, "data_type": col_type})
        if dims:
            new_entry["dimensions"] = dims
        if measures:
            new_entry["measures"] = measures
        tables.append(new_entry)
        existing = tables[-1]

    # Attach Data Vault metadata
    if "data_vault" in enrichment:
        existing["data_vault"] = enrichment["data_vault"]

    # Attach sample questions
    if "sample_questions" in enrichment:
        existing["sample_questions"] = enrichment["sample_questions"]

    schema_data["tables"] = tables

    # Write back
    try:
        with open(yaml_path, "w", encoding="utf-8") as fh:
            yaml.dump(schema_data, fh, default_flow_style=False,
                      sort_keys=False, allow_unicode=True)
        logger.info("YAML enriched for table: %s", table_name)
        return True
    except Exception as exc:
        logger.error("Failed to write enriched YAML: %s", exc)
        return False


def auto_discover_and_enrich_yaml(
    schema: str = "INFORMATION_MART",
    yaml_path: str | None = None,
) -> List[str]:
    """
    Scan the schema for tables not yet in the YAML and enrich each one.
    Returns a list of table names that were processed.

    This function is the entry point for req 4 and req 7:
    'If a new table is added, the YAML should be updated automatically.'
    """
    yaml_path = yaml_path or Config.SCHEMA_METADATA_FILE
    processed = []

    try:
        with open(yaml_path, "r", encoding="utf-8") as fh:
            schema_data = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        schema_data = {"tables": []}

    known_tables = {
        t.get("base_table", {}).get("table", "").upper()
        for t in schema_data.get("tables", [])
    }

    try:
        all_tables_df = list_tables_in_schema(schema)
    except Exception as exc:
        logger.error("Could not list tables in schema %s: %s", schema, exc)
        return processed

    for _, row in all_tables_df.iterrows():
        tname = str(row.get("TABLE_NAME", "")).strip().upper()
        if tname and tname not in known_tables:
            logger.info(
                "New table detected: %s. Running AI enrichment.", tname)
            ok = enrich_yaml_for_table(tname, schema, yaml_path)
            if ok:
                processed.append(tname)

    return processed


# ---------------------------------------------------------------------------
# Module self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Testing SQL generation...")
    q = "what parts are low in inventory"
    sql = generate_sql(q)
    print(f"Question : {q}")
    print(f"SQL      :\n{sql}")

    print("\nTesting prescriptive insights...")
    summary = "Top dealer: Dealer with highest UnitPrice spend"
    insights = generate_prescriptive_insights(
        summary, "Show top dealers by unit price")
    print(f"Insights:\n{insights}")