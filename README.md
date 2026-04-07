# Finding The Finger

> *The finger pointing at the moon is not the moon.* — Zen proverb

A semantic similarity search engine across sacred and philosophical texts. Find where different traditions point at the same things.

## What it does

- **Passage search** — select a verse, chapter, or book from one tradition and find semantically similar passages across all others
- **Free text search** — describe a concept or theme and find matching passages
- **UMAP map** — a 2D projection of all embeddings, coloured by tradition, with density clouds, layer toggles, and an in-map search

Traditions covered: Abrahamic, Buddhist, Confucian, Dharmic, Norse, Shinto, Sikh, Taoist, Zoroastrian.

## Tech stack

| Layer | Tech |
|---|---|
| Backend | FastAPI + Python 3.13 |
| Database | PostgreSQL 17 + pgvector |
| Embeddings | [nomic-embed-text-v1.5](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5) |
| Projection | UMAP (stratified fit + full transform) |
| Frontend | Alpine.js + deck.gl + d3 |
| Infra | Docker Compose |

## Getting started

### Prerequisites

- Docker + Docker Compose
- ~4 GB disk for the database and model cache

### 1. Clone and start services

```bash
git clone https://github.com/ayyystew/FindingTheFinger.git
cd FindingTheFinger
docker compose up -d
```

This starts PostgreSQL (with pgvector) and the app on `http://localhost:8000`.

### 2. Run migrations

```bash
docker compose exec app alembic upgrade head
```

### 3. Load corpus data

Each script in `scripts/` loads one text into the database:

```bash
docker compose exec app python -m scripts.add_kjv
docker compose exec app python -m scripts.add_quran
docker compose exec app python -m scripts.add_gita
# ... etc
```

### 4. Generate embeddings

```bash
docker compose exec app python -m scripts.embed_nomic
```

This embeds all leaf-level passages using `nomic-embed-text-v1.5`. Requires internet access on first run to download the model (~270 MB, cached after that).

Pre-compute aggregated embeddings for chapter/book-level search:

```bash
docker compose exec app python -m scripts.embed_aggregate
```

### 5. Compute UMAP projection

```bash
docker compose exec app python -m scripts.compute_umap
```

By default this fits UMAP on a balanced sample — up to 100 passages per top-level division (Book, Raag, Surah, etc.) across all corpora — then projects every passage into that space. This prevents large corpora from dominating the projection.

```bash
# Custom sample size
docker compose exec app python -m scripts.compute_umap --sample-per-division 50

# Disable sampling (fit on all ~116k passages)
docker compose exec app python -m scripts.compute_umap --no-sample
```

### 6. Open the app

Visit `http://localhost:8000`.

## Development

Run locally without Docker (requires a running PostgreSQL with pgvector):

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://ftf:ftf@localhost:5432/ftf
uvicorn app.main:app --reload
```

## Corpus allowlist

To restrict which corpora are searchable (e.g. for a focused deployment), set `CORPUS_ALLOWLIST` in `app/config.py`.

## Links

- Dataset: *(coming soon)*
- [GitHub](https://github.com/ayyystew/FindingTheFinger)
