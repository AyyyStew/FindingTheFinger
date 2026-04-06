"""
app/search.py

Read-only DB access and similarity search against PostgreSQL + pgvector.
"""

import os
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.config import CORPUS_ALLOWLIST

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://ftf:ftf@localhost:5432/ftf")
MODEL_NAME   = "nomic-embed-text-v1.5"
QUERY_PREFIX = "search_query: "

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return _engine


# ---------------------------------------------------------------------------
# Allowlist helpers
# ---------------------------------------------------------------------------

def _allowlist_clause(alias: str = "c") -> str:
    if not CORPUS_ALLOWLIST:
        return ""
    return f"AND {alias}.name = ANY(:allowlist)"


def _allowlist_params(params: dict) -> dict:
    if CORPUS_ALLOWLIST:
        params["allowlist"] = CORPUS_ALLOWLIST
    return params


# ---------------------------------------------------------------------------
# Similarity search
# ---------------------------------------------------------------------------

_SIMILARITY_SQL = """
SELECT
    u.id                                        AS unit_id,
    c.name                                      AS corpus,
    t.name                                      AS tradition,
    u.label,
    u.text,
    1 - (e.vector <=> CAST(:query_vec AS vector)) AS similarity
FROM embedding e
JOIN unit    u ON e.unit_id      = u.id
JOIN corpus  c ON u.corpus_id    = c.id
JOIN tradition t ON c.tradition_id = t.id
WHERE e.model_name = :model
  AND u.height = 0
  {allowlist_clause}
  {tradition_filter}
  {corpus_filter}
ORDER BY e.vector <=> CAST(:query_vec AS vector)
LIMIT  :limit
OFFSET :offset
"""


def _vec_str(vector) -> str:
    """Format a vector as a pgvector literal regardless of input type.
    Handles: list[float], numpy array, or a bare pgvector string from psycopg2."""
    if isinstance(vector, str):
        return vector  # already formatted by psycopg2 registered type
    # list, numpy array, or anything iterable of numbers
    return "[" + ",".join(str(float(v)) for v in vector) + "]"


def _run_similarity(
    vector,
    limit: int,
    offset: int = 0,
    exclude_tradition: Optional[str] = None,
    only_corpora: Optional[list[str]] = None,
) -> list[dict]:
    tradition_filter = ""
    corpus_filter    = ""
    params = _allowlist_params({
        "query_vec": _vec_str(vector),
        "model":     MODEL_NAME,
        "limit":     limit,
        "offset":    offset,
    })

    if exclude_tradition:
        tradition_filter = "AND t.name != :exclude_tradition"
        params["exclude_tradition"] = exclude_tradition

    if only_corpora:
        corpus_filter = "AND c.name = ANY(:only_corpora)"
        params["only_corpora"] = only_corpora

    sql = text(_SIMILARITY_SQL.format(
        allowlist_clause=_allowlist_clause(),
        tradition_filter=tradition_filter,
        corpus_filter=corpus_filter,
    ))

    with Session(get_engine()) as session:
        rows = session.execute(sql, params).fetchall()

    cols = ["unit_id", "corpus", "tradition", "label", "text", "similarity"]
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
    with Session(get_engine()) as session:
        row = session.execute(text("""
            SELECT e.vector
            FROM embedding e
            JOIN unit   u ON e.unit_id    = u.id
            JOIN corpus c ON u.corpus_id  = c.id
            WHERE c.name       = :corpus
              AND u.label      = :label
              AND e.model_name = :model
            LIMIT 1
        """), {"corpus": corpus_name, "label": unit_label, "model": MODEL_NAME}).fetchone()

    if not row:
        return []

    return _run_similarity(row[0], limit, offset, exclude_tradition, only_corpora)


# ---------------------------------------------------------------------------
# Browse helpers
# ---------------------------------------------------------------------------

def get_refs(corpus_name: str, q: str, limit: int = 20) -> list[str]:
    with Session(get_engine()) as session:
        rows = session.execute(text("""
            SELECT u.label
            FROM unit   u
            JOIN corpus c ON u.corpus_id = c.id
            WHERE c.name    = :corpus
              AND u.height  = 0
              AND u.label ILIKE :fuzzy
            ORDER BY
                CASE WHEN u.label ILIKE :suffix THEN 0 ELSE 1 END,
                u.id
            LIMIT :n
        """), {
            "corpus": corpus_name,
            "fuzzy":  f"%{q}%",
            "suffix": f"% {q}" if q else "%",
            "n":      limit,
        }).fetchall()
    return [r[0] for r in rows if r[0]]


def get_corpora() -> list[dict]:
    params = _allowlist_params({})
    sql = text(f"""
        SELECT c.name, t.name AS tradition
        FROM corpus    c
        JOIN tradition t ON c.tradition_id = t.id
        WHERE 1=1 {_allowlist_clause()}
        ORDER BY t.name, c.name
    """)
    with Session(get_engine()) as session:
        rows = session.execute(sql, params).fetchall()
    return [{"corpus": r[0], "tradition": r[1]} for r in rows]


def get_passage(corpus_name: str, label: str) -> dict | None:
    with Session(get_engine()) as session:
        row = session.execute(text("""
            SELECT u.text, u.label, c.name, t.name
            FROM unit      u
            JOIN corpus    c ON u.corpus_id    = c.id
            JOIN tradition t ON c.tradition_id = t.id
            WHERE c.name  = :corpus
              AND u.label = :label
            LIMIT 1
        """), {"corpus": corpus_name, "label": label}).fetchone()
    if not row:
        return None
    return {"text": row[0], "label": row[1], "corpus": row[2], "tradition": row[3]}


def get_unit_by_id(unit_id: int) -> dict | None:
    with Session(get_engine()) as session:
        row = session.execute(text("""
            SELECT u.text, u.label, c.name, t.name
            FROM unit      u
            JOIN corpus    c ON u.corpus_id    = c.id
            JOIN tradition t ON c.tradition_id = t.id
            WHERE u.id = :uid
            LIMIT 1
        """), {"uid": unit_id}).fetchone()
    if not row:
        return None
    return {"text": row[0], "label": row[1], "corpus": row[2], "tradition": row[3]}


# ---------------------------------------------------------------------------
# Map / UMAP helpers
# ---------------------------------------------------------------------------

def get_map_versions() -> list[dict]:
    with Session(get_engine()) as session:
        rows = session.execute(text("""
            SELECT r.id,
                   r.created_at,
                   r.label,
                   r.model_name,
                   r.n_neighbors,
                   r.min_dist,
                   COUNT(p.unit_id) AS point_count
            FROM umap_run r
            LEFT JOIN umap_point p ON p.umap_run_id = r.id
            GROUP BY r.id
            ORDER BY r.id DESC
        """)).fetchall()
    return [
        {
            "id":          r[0],
            "created_at":  r[1].isoformat() if r[1] else None,
            "label":       r[2],
            "model_name":  r[3],
            "n_neighbors": r[4],
            "min_dist":    r[5],
            "point_count": r[6],
        }
        for r in rows
    ]


def get_map_data(version_id: int | None = None) -> dict | None:
    """
    Return a compact UMAP projection payload.

    Points use integer indices into `traditions` and `corpora` arrays to
    reduce JSON size. All heights are included so the client can toggle
    between leaf/parent layers without a second fetch.
    """
    with Session(get_engine()) as session:
        if version_id is None:
            run_row = session.execute(text("""
                SELECT id, created_at, label, model_name, n_neighbors, min_dist
                FROM   umap_run
                ORDER  BY id DESC
                LIMIT  1
            """)).fetchone()
        else:
            run_row = session.execute(text("""
                SELECT id, created_at, label, model_name, n_neighbors, min_dist
                FROM   umap_run
                WHERE  id = :id
            """), {"id": version_id}).fetchone()

        if not run_row:
            return None

        run_id = run_row[0]
        run = {
            "id":          run_row[0],
            "created_at":  run_row[1].isoformat() if run_row[1] else None,
            "label":       run_row[2],
            "model_name":  run_row[3],
            "n_neighbors": run_row[4],
            "min_dist":    run_row[5],
        }

        rows = session.execute(text("""
            SELECT up.unit_id,
                   up.x,
                   up.y,
                   up.corpus_seq,
                   u.height,
                   u.label,
                   c.name  AS corpus,
                   t.name  AS tradition
            FROM   umap_point  up
            JOIN   unit        u  ON u.id           = up.unit_id
            JOIN   corpus      c  ON c.id           = u.corpus_id
            JOIN   tradition   t  ON t.id           = c.tradition_id
            WHERE  up.umap_run_id = :run_id
            ORDER  BY up.unit_id
        """), {"run_id": run_id}).fetchall()

        if not rows:
            return None

        # Build compact index arrays to reduce JSON payload
        traditions = sorted({r[7] for r in rows})
        corpora    = sorted({r[6] for r in rows})
        trad_idx   = {t: i for i, t in enumerate(traditions)}
        corp_idx   = {c: i for i, c in enumerate(corpora)}

        # Map corpus -> tradition for the client
        corp_trad  = {}
        for r in rows:
            corp_trad[r[6]] = r[7]
        trad_of_corpus = [trad_idx[corp_trad[c]] for c in corpora]

        points = [
            {
                "id":    r[0],
                "x":     round(r[1], 4),
                "y":     round(r[2], 4),
                "s":     r[3],          # corpus_seq (None for h > 0)
                "h":     r[4],          # height (0 = leaf)
                "label": r[5],
                "ti":    trad_idx[r[7]],
                "ci":    corp_idx[r[6]],
            }
            for r in rows
        ]

        # corpus_levels[ci] = {height: level_name} e.g. {0: 'Verse', 1: 'Chapter', 2: 'Book'}
        level_rows = session.execute(text("""
            SELECT c.name, cl.height, cl.name AS level_name
            FROM   corpus_level cl
            JOIN   corpus       c ON c.id = cl.corpus_id
            WHERE  c.name = ANY(:corpora)
            ORDER  BY c.name, cl.height
        """), {"corpora": corpora}).fetchall()

        corpus_levels = [{} for _ in corpora]
        for corp_name, height, level_name in level_rows:
            ci = corp_idx.get(corp_name)
            if ci is not None:
                corpus_levels[ci][height] = level_name

    return {
        "run":            run,
        "traditions":     traditions,
        "corpora":        corpora,
        "trad_of_corpus": trad_of_corpus,
        "corpus_levels":  corpus_levels,
        "points":         points,
    }
