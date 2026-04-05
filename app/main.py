"""
app/main.py

FastAPI application entry point.
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles

from app.search import get_conn, get_corpora, get_refs, search_by_vector, search_by_verse

# ---------------------------------------------------------------------------
# Model — loaded once at startup
# ---------------------------------------------------------------------------

_model = None


def get_model():
    return _model


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _model
    from sentence_transformers import SentenceTransformer
    print("Loading sentence-transformer model…")
    _model = SentenceTransformer("all-mpnet-base-v2")
    print("Model ready.")
    yield
    _model = None


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Verse Similarity Search", lifespan=lifespan)


# ---------------------------------------------------------------------------
# API routes  (must be registered before StaticFiles catch-all)
# ---------------------------------------------------------------------------

@app.get("/corpora")
def corpora():
    return get_corpora()


@app.get("/passage")
def passage(
    corpus: str = Query(...),
    ref: str = Query(...),
):
    conn = get_conn()
    row = conn.execute(
        """
        SELECT p.text, p.unit_label, c.name, ct.name
        FROM passage p
        JOIN corpus c  ON p.corpus_id    = c.id
        JOIN corpus_tradition ct ON c.tradition_id = ct.id
        WHERE c.name = $corpus AND p.unit_label = $ref
        LIMIT 1
        """,
        {"corpus": corpus, "ref": ref},
    ).fetchone()
    if not row:
        raise HTTPException(404, f"Passage not found: '{ref}' in '{corpus}'")
    return {"text": row[0], "unit_label": row[1], "corpus": row[2], "tradition": row[3]}


@app.get("/refs")
def refs(
    corpus: str = Query(...),
    q: str = Query(""),
    limit: int = Query(20, ge=1, le=50),
):
    return get_refs(corpus, q, limit)


@app.get("/search")
def search(
    q: str = Query(..., description="Free-text query"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    exclude_tradition: Optional[str] = Query(None),
    corpora: list[str] = Query(default=[]),
):
    model = get_model()
    if model is None:
        raise HTTPException(503, "Model not ready")
    vector = model.encode(q, normalize_embeddings=False).tolist()
    results = search_by_vector(
        vector, limit=limit, offset=offset,
        exclude_tradition=exclude_tradition,
        only_corpora=corpora or None,
    )
    return results


@app.get("/verse")
def verse(
    corpus: str = Query(..., description="Corpus name"),
    ref: str = Query(..., description="unit_label, e.g. 'John 3:16'"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    exclude_tradition: Optional[str] = Query(None),
    corpora: list[str] = Query(default=[]),
):
    results = search_by_verse(
        corpus_name=corpus,
        unit_label=ref,
        limit=limit,
        offset=offset,
        exclude_tradition=exclude_tradition,
        only_corpora=corpora or None,
    )
    if not results:
        raise HTTPException(404, f"No embedding found for '{ref}' in corpus '{corpus}'")
    return results


# ---------------------------------------------------------------------------
# Static files — served last so API routes take precedence
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory="static", html=True), name="static")
