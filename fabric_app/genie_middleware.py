# genie_middleware.py
#
# NOTE: this module originally used Streamlit's `st.session_state` to stash
# per-user logging context (question/user/session_id) between calls. That
# does not exist in a stateless FastAPI backend - there is no single shared
# "session" object across requests, and multiple requests can be in flight
# concurrently on the same worker. It has been replaced with a
# `contextvars.ContextVar`, which is the asyncio/thread-safe equivalent:
# each request gets its own isolated context automatically, so calling
# `set_log_context(...)` once near the top of a request handler and then
# `log_event(...)` later in the same call stack behaves the same way the
# Streamlit version did, but safely under concurrent requests.
from __future__ import annotations
import contextvars
import hashlib
import logging
from config import Config
from db import run_warehouse_non_query, run_warehouse_df

WH = f"[{Config.FABRIC_READYTOBUILD_WAREHOUSE_DATABASE}].[{Config.DEFAULT_SCHEMA}]"
GENIE_CONTEXT_MEMORY_TABLE = Config.GENIE_CONTEXT_MEMORY_TABLE
logger = logging.getLogger(__name__)

# -------------------------------
# Context Management
# -------------------------------
_log_context: contextvars.ContextVar[dict] = contextvars.ContextVar(
    "genie_log_context", default={}
)


def set_log_context(**kwargs):
    current = dict(_log_context.get())
    current.update(kwargs)
    _log_context.set(current)


def get_log_context():
    return _log_context.get()


def clear_log_context():
    _log_context.set({})


# -------------------------------
# Utils
# -------------------------------
def _sql_escape(val):
    if val is None:
        return ""
    return str(val).replace("'", "''")


def _fit_col(val: str, max_len: int) -> str:
    """Trim text to the target VARCHAR length."""
    txt = "" if val is None else str(val)
    return txt[:max_len]


def generate_context_hash(question: str, user: str):
    raw = f"{user}:{question.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()

# -------------------------------
# Middleware Logger (Insert and Update Frequency)
# -------------------------------
def log_event(event_type: str, payload: dict):
    try:
        ctx = get_log_context()

        question = payload.get("question") or ctx.get("question", "")
        session_id = payload.get("session_id") or ctx.get("session_id", "unknown")
        user = payload.get("user") or ctx.get("user", "UNKNOWN")

        context_hash = generate_context_hash(question, user)

        sql_query = _sql_escape(payload.get("sql", ""))
        summary = _sql_escape(payload.get("summary", ""))
        full = _sql_escape(payload.get("full_answer", ""))
        tables = _sql_escape(payload.get("tables", ""))
        filters = _sql_escape(payload.get("filters", ""))
        details = _sql_escape(payload.get("details", ""))
        cache_key = _sql_escape(payload.get("cache_key", ""))

        relevance = payload.get("relevance", 0.0)

        username = _fit_col(user, 100)
        user_id = _fit_col(user, 64)
        user_esc = _sql_escape(username)
        user_id_esc = _sql_escape(user_id)
        question_esc = _sql_escape(question)

        # Frequency is per (question, user). Allow caller to pin it so
        # multiple events in the same request share one frequency value.
        payload_frequency = payload.get("frequency")
        if payload_frequency is not None:
            try:
                new_frequency = max(1, int(payload_frequency))
            except (TypeError, ValueError):
                existing_frequency = get_existing_question_frequency(
                    question_esc, user_esc
                )
                new_frequency = existing_frequency + 1
        else:
            existing_frequency = get_existing_question_frequency(question_esc, user_esc)
            new_frequency = existing_frequency + 1

        sql = f"""
        INSERT INTO {GENIE_CONTEXT_MEMORY_TABLE} (
            SessionId,
            Username,
            user_id,
            Question,
            AnswerSummary,
            FullAnswer,
            Context_Hash,
            Sql_Query,
            Tables_Used,
            Filters_Applied,
            Relevance_Score,
            Usage_Count,
            Last_Accessed_At,
            CacheKey,
            Frequency,
            Action_Type,
            Action_Details,
            ChatDate,
            CreatedAt,
            UpdatedAt
        )
        VALUES (
            '{session_id}',
            '{user_esc}',
            '{user_id_esc}',
            '{question_esc}',
            '{summary}',
            '{full}',
            '{context_hash}',
            '{sql_query}',
            '{tables}',
            '{filters}',
            {relevance},
            1,
            GETDATE(),
            '{cache_key}',
            {new_frequency},
            '{event_type}',
            '{details}',
            CAST(GETDATE() AS DATE),
            GETDATE(),
            GETDATE()
        );
        """

        run_warehouse_non_query(sql)

    except Exception as e:
        logger.warning(f"[Middleware] Logging failed: {e}")


def get_existing_question_frequency(question: str, user: str) -> int:
    try:
        sql = f"""
        SELECT ISNULL(MAX(Frequency), 0) AS maxFrequency
        FROM {GENIE_CONTEXT_MEMORY_TABLE}
        WHERE Question = '{question}'
          AND Username = '{user}'
        """

        result = run_warehouse_df(sql)
        print(f"Frequency query result:\n{result}")

        # ✅ Proper DataFrame emptiness check
        if result is None or result.empty:
            return 0

        # ✅ Safe value extraction
        max_freq = result.iloc[0]["maxFrequency"]
        print(f"Existing frequency result: {max_freq}")

        return int(max_freq) if max_freq is not None else 0

    except Exception as e:
        logger.warning(f"[Middleware] Fetch existing frequency failed: {e}")
        return 0


# -------------------------------
# Middleware Logger (MERGE)
# -------------------------------
def log_events_upsert(event_type: str, payload: dict):
    try:
        ctx = get_log_context()

        question = payload.get("question") or ctx.get("question", "")
        session_id = payload.get("session_id") or ctx.get("session_id", "unknown")
        user = payload.get("user") or ctx.get("user", "UNKNOWN")

        context_hash = generate_context_hash(question, user)

        sql_query = _sql_escape(payload.get("sql", ""))
        summary = _sql_escape(payload.get("summary", ""))
        full = _sql_escape(payload.get("full_answer", ""))
        tables = _sql_escape(payload.get("tables", ""))
        filters = _sql_escape(payload.get("filters", ""))
        details = _sql_escape(payload.get("details", ""))
        cache_key = _sql_escape(payload.get("cache_key", ""))

        relevance = payload.get("relevance", 0.0)

        username = _fit_col(user, 100)
        user_id = _fit_col(user, 64)
        user_esc = _sql_escape(username)
        user_id_esc = _sql_escape(user_id)
        question_esc = _sql_escape(question)

        sql = f"""
        MERGE {GENIE_CONTEXT_MEMORY_TABLE} AS target
        USING (
            SELECT
                '{session_id}' AS SessionId,
                '{user_esc}' AS Username,
                '{user_id_esc}' AS user_id,
                '{question_esc}' AS Question,
                '{context_hash}' AS Context_Hash
        ) AS source
        ON target.Context_Hash = source.Context_Hash
           AND target.Username = source.Username

        WHEN MATCHED THEN
            UPDATE SET
                Frequency = target.Frequency + 1,
                Last_Accessed_At = GETDATE(),
                UpdatedAt = GETDATE(),
                AnswerSummary = '{summary}',
                FullAnswer = '{full}',
                Sql_Query = '{sql_query}',
                Tables_Used = '{tables}',
                Filters_Applied = '{filters}',
                Relevance_Score = {relevance},
                CacheKey = '{cache_key}',
                Action_Type = '{event_type}',
                Action_Details = '{details}',
                Usage_Count = ISNULL(target.Usage_Count, 0) + 1

        WHEN NOT MATCHED THEN
            INSERT (
                SessionId, Username, user_id, Question,
                AnswerSummary, FullAnswer, Context_Hash,
                Sql_Query, Tables_Used, Filters_Applied,
                Relevance_Score, Usage_Count, Last_Accessed_At,
                CacheKey, Frequency, Action_Type, Action_Details,
                ChatDate, CreatedAt, UpdatedAt
            )
            VALUES (
                '{session_id}', '{user_esc}', '{user_id_esc}', '{question_esc}',
                '{summary}', '{full}', '{context_hash}',
                '{sql_query}', '{tables}', '{filters}',
                {relevance}, 1, GETDATE(),
                '{cache_key}', 1, '{event_type}', '{details}',
                CAST(GETDATE() AS DATE), GETDATE(), GETDATE()
            );
        """

        run_warehouse_non_query(sql)

    except Exception as e:
        logger.warning(f"[Middleware] Logging failed: {e}")