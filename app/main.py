"""
app/main.py

FastAPI application entry point.
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles

from app.search import get_corpora, get_refs, search_by_vector, search_by_verse

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
):
    model = get_model()
    if model is None:
        raise HTTPException(503, "Model not ready")
    vector = model.encode(q, normalize_embeddings=False).tolist()
    results = search_by_vector(vector, limit=limit, offset=offset, exclude_tradition=exclude_tradition)
    return results


@app.get("/verse")
def verse(
    corpus: str = Query(..., description="Corpus name"),
    ref: str = Query(..., description="unit_label, e.g. 'John 3:16'"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    exclude_tradition: Optional[str] = Query(None),
):
    results = search_by_verse(
        corpus_name=corpus,
        unit_label=ref,
        limit=limit,
        offset=offset,
        exclude_tradition=exclude_tradition,
    )
    if not results:
        raise HTTPException(404, f"No embedding found for '{ref}' in corpus '{corpus}'")
    return results


# ---------------------------------------------------------------------------
# Static files — served last so API routes take precedence
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory="static", html=True), name="static")
