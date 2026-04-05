"""
app/search.py

Read-only DB connection and similarity search logic.
"""

import os
import duckdb
from typing import Optional

from app.config import CORPUS_ALLOWLIST

DB_PATH = os.environ.get("CORPUS_DB", "/home/alexs/Projects/DataSources/corpus.duckdb")
MODEL_NAME = "all-mpnet-base-v2"

_conn: Optional[duckdb.DuckDBPyConnection] = None


def get_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(DB_PATH, read_only=True)
    return _conn


def _allowlist_clause(alias: str = "c") -> str:
    """Returns a SQL fragment that restricts to the allowlist, or empty string."""
    if not CORPUS_ALLOWLIST:
        return ""
    return f"AND {alias}.name = ANY($allowlist)"


def _allowlist_param(params: dict) -> dict:
    if CORPUS_ALLOWLIST:
        params["allowlist"] = CORPUS_ALLOWLIST
    return params


_SIMILARITY_SQL = """
SELECT
    p.id          AS passage_id,
    c.name        AS corpus,
    ct.name       AS tradition,
    p.unit_label,
    p.text,
    list_dot_product(e.vector, $query_vec::FLOAT[768])
        / (
            sqrt(list_dot_product(e.vector, e.vector))
            * sqrt(list_dot_product($query_vec::FLOAT[768], $query_vec::FLOAT[768]))
          )         AS similarity
FROM embedding e
JOIN passage p  ON e.passage_id  = p.id
JOIN corpus c   ON p.corpus_id   = c.id
JOIN corpus_tradition ct ON c.tradition_id = ct.id
WHERE e.model_name = $model
{allowlist_clause}
{tradition_filter}
{corpus_filter}
ORDER BY similarity DESC
LIMIT $limit
OFFSET $offset
"""


def _run_similarity(
    vector: list[float],
    limit: int,
    offset: int = 0,
    exclude_tradition: Optional[str] = None,
    only_corpora: Optional[list[str]] = None,
) -> list[dict]:
    conn = get_conn()

    tradition_filter = ""
    corpus_filter = ""
    params: dict = _allowlist_param({
        "query_vec": vector,
        "model": MODEL_NAME,
        "limit": limit,
        "offset": offset,
    })

    if exclude_tradition:
        tradition_filter = "AND ct.name != $exclude_tradition"
        params["exclude_tradition"] = exclude_tradition

    if only_corpora:
        corpus_filter = "AND c.name = ANY($only_corpora)"
        params["only_corpora"] = only_corpora

    sql = _SIMILARITY_SQL.format(
        allowlist_clause=_allowlist_clause(),
        tradition_filter=tradition_filter,
        corpus_filter=corpus_filter,
    )
    rows = conn.execute(sql, params).fetchall()
    cols = ["passage_id", "corpus", "tradition", "unit_label", "text", "similarity"]
    return [dict(zip(cols, row)) for row in rows]


def search_by_vector(
    vector: list[float],
    limit: int = 10,
    offset: int = 0,
    exclude_tradition: Optional[str] = None,
    only_corpora: Optional[list[str]] = None,
) -> list[dict]:
    return _run_similarity(vector, limit, offset, exclude_tradition, only_corpora)


def search_by_verse(
    corpus_name: str,
    unit_label: str,
    limit: int = 10,
    offset: int = 0,
    exclude_tradition: Optional[str] = None,
    only_corpora: Optional[list[str]] = None,
) -> list[dict]:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT e.vector
        FROM embedding e
        JOIN passage p  ON e.passage_id = p.id
        JOIN corpus c   ON p.corpus_id  = c.id
        WHERE c.name       = $corpus
          AND p.unit_label = $unit_label
          AND e.model_name = $model
        LIMIT 1
        """,
        {"corpus": corpus_name, "unit_label": unit_label, "model": MODEL_NAME},
    ).fetchone()

    if not row:
        return []

    return _run_similarity(list(row[0]), limit, offset, exclude_tradition, only_corpora)


def get_refs(corpus_name: str, q: str, limit: int = 20) -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT p.unit_label
        FROM passage p
        JOIN corpus c ON p.corpus_id = c.id
        WHERE c.name = $corpus
          AND p.unit_label ILIKE $fuzzy
        ORDER BY
            -- exact-suffix match first: "3:16" → "John 3:16" before "Acts 13:16"
            CASE WHEN p.unit_label ILIKE $suffix THEN 0 ELSE 1 END,
            p.unit_number,
            p.unit_label
        LIMIT $n
        """,
        {
            "corpus": corpus_name,
            "fuzzy": f"%{q}%",
            "suffix": f"% {q}" if q else "%",
            "n": limit,
        },
    ).fetchall()
    return [r[0] for r in rows if r[0]]


def get_corpora() -> list[dict]:
    conn = get_conn()
    params = _allowlist_param({})
    sql = f"""
        SELECT c.name, ct.name AS tradition
        FROM corpus c
        JOIN corpus_tradition ct ON c.tradition_id = ct.id
        WHERE 1=1 {_allowlist_clause()}
        ORDER BY ct.name, c.name
    """
    rows = conn.execute(sql, params).fetchall()
    return [{"corpus": r[0], "tradition": r[1]} for r in rows]
