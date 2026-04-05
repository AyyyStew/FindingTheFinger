"""
app/main.py

FastAPI application entry point.
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.search import (
    get_corpora, get_refs, get_passage,
    search_by_vector, search_by_verse,
    QUERY_PREFIX,
)

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
    print("Loading nomic-embed-text-v1.5…")
    _model = SentenceTransformer("nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True)
    print("Model ready.")
    yield
    _model = None


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Finding the Finger", lifespan=lifespan)

templates = Jinja2Templates(directory="templates")
api = APIRouter(prefix="/api/v1")


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

@app.get("/")
def index(request: Request):
    return templates.TemplateResponse(request, "index.html", {"page": "search"})


@app.get("/about")
def about(request: Request):
    return templates.TemplateResponse(request, "about.html", {"page": "about"})


@app.get("/map")
def map_page(request: Request):
    return templates.TemplateResponse(request, "map.html", {"page": "map"})


@app.get("/passage")
def passage_page(request: Request):
    return templates.TemplateResponse(request, "passage.html", {"page": "passage"})


# ---------------------------------------------------------------------------
# API routes — /api/v1/...
# ---------------------------------------------------------------------------

@api.get("/corpora")
def corpora():
    return get_corpora()


@api.get("/passage")
def passage(
    corpus: str = Query(...),
    ref: str = Query(...),
):
    row = get_passage(corpus, ref)
    if not row:
        raise HTTPException(404, f"Passage not found: '{ref}' in '{corpus}'")
    return row


@api.get("/refs")
def refs(
    corpus: str = Query(...),
    q: str = Query(""),
    limit: int = Query(20, ge=1, le=50),
):
    return get_refs(corpus, q, limit)


@api.get("/search")
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
    vector = model.encode(QUERY_PREFIX + q, normalize_embeddings=True).tolist()  # list[float]
    return search_by_vector(
        vector, limit=limit, offset=offset,
        exclude_tradition=exclude_tradition,
        only_corpora=corpora or None,
    )


@api.get("/verse")
def verse(
    corpus: str = Query(..., description="Corpus name"),
    ref: str = Query(..., description="Unit label, e.g. 'Yasna 28:3'"),
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


app.include_router(api)

# ---------------------------------------------------------------------------
# Static files — served last so API routes take precedence
# ---------------------------------------------------------------------------

app.mount("/static", StaticFiles(directory="static"), name="static")
