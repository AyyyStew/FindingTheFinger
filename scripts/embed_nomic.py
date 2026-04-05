"""
scripts/embed_nomic.py

Embeds all leaf units (height=0) with nomic-embed-text-v1.5.

Uses the "search_document: " prefix required by nomic's asymmetric model.
Skips units that already have an embedding for this model (safe to re-run).

Usage:
    # All corpora
    python -m scripts.embed_nomic

    # Specific corpus IDs
    python -m scripts.embed_nomic --corpus 8 9 10

    # Dry run (count units, don't embed)
    python -m scripts.embed_nomic --dry-run
"""

import argparse
import sys

import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models import Unit, Embedding

MODEL_NAME   = "nomic-ai/nomic-embed-text-v1.5"
MODEL_KEY    = "nomic-embed-text-v1.5"   # stored in embedding.model_name
PREFIX       = "search_document: "
BATCH_SIZE   = 32
DATABASE_URL = "postgresql://ftf:ftf@localhost:5432/ftf"

# Reduce GPU memory fragmentation
import os
os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--corpus", nargs="*", type=int, default=None,
                   help="Corpus IDs to embed (default: all)")
    p.add_argument("--dry-run", action="store_true",
                   help="Count units without embedding")
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    return p.parse_args()


def load_pending(session: Session, corpus_ids: list[int] | None) -> list[tuple[int, str]]:
    """Return (unit_id, text) for all height=0 units not yet embedded."""
    q = text("""
        SELECT u.id, u.text
        FROM unit u
        WHERE u.height = 0
          AND u.text IS NOT NULL
          AND u.text != ''
          AND NOT EXISTS (
              SELECT 1 FROM embedding e
              WHERE e.unit_id = u.id AND e.model_name = :model
          )
          {corpus_filter}
        ORDER BY u.corpus_id, u.id
    """.format(
        corpus_filter="AND u.corpus_id = ANY(:cids)" if corpus_ids else ""
    ))

    params = {"model": MODEL_KEY}
    if corpus_ids:
        params["cids"] = corpus_ids

    rows = session.execute(q, params).fetchall()
    return [(r[0], r[1]) for r in rows]


def upsert_embeddings(session: Session, batch: list[tuple[int, list[float]]]) -> None:
    stmt = pg_insert(Embedding).values([
        {"unit_id": uid, "model_name": MODEL_KEY, "vector": vec}
        for uid, vec in batch
    ])
    stmt = stmt.on_conflict_do_update(
        index_elements=["unit_id", "model_name"],
        set_={"vector": stmt.excluded.vector},
    )
    session.execute(stmt)
    session.commit()


def main() -> None:
    args = parse_args()
    batch_size = args.batch_size

    print("Connecting to PostgreSQL...")
    engine = create_engine(DATABASE_URL)

    with Session(engine) as session:
        corpus_ids = args.corpus
        if corpus_ids:
            print(f"  Filtering to corpus IDs: {corpus_ids}")

        print("Loading pending units...")
        pending = load_pending(session, corpus_ids)
        print(f"  {len(pending):,} units to embed")

        if args.dry_run or not pending:
            if not pending:
                print("Nothing to do.")
            return

        print(f"\nLoading {MODEL_NAME}...")
        model = SentenceTransformer(MODEL_NAME, trust_remote_code=True)
        print("  Model loaded.")

        n_done = 0
        n_batches = (len(pending) + batch_size - 1) // batch_size

        for i in range(0, len(pending), batch_size):
            batch_rows = pending[i : i + batch_size]
            ids   = [r[0] for r in batch_rows]
            texts = [PREFIX + r[1] for r in batch_rows]

            try:
                vecs = model.encode(texts, batch_size=batch_size, show_progress_bar=False)
            except torch.cuda.OutOfMemoryError:
                # Fall back to one-at-a-time to find the culprit
                print(f"\n  OOM on batch {i // batch_size + 1} — falling back to single encoding")
                vecs_list = []
                for uid, t in zip(ids, texts):
                    try:
                        v = model.encode([t], batch_size=1, show_progress_bar=False)
                        vecs_list.append(v[0])
                    except torch.cuda.OutOfMemoryError:
                        char_count = len(t)
                        print(f"\n  OOM on unit_id={uid} ({char_count:,} chars) — skipping")
                        vecs_list.append(None)
                        torch.cuda.empty_cache()

                pairs = [(uid, v.tolist()) for uid, v in zip(ids, vecs_list) if v is not None]
                if pairs:
                    upsert_embeddings(session, pairs)
                n_done += len(batch_rows)
                batch_num = i // batch_size + 1
                pct = 100 * n_done / len(pending)
                print(f"  [{batch_num}/{n_batches}] {n_done:,}/{len(pending):,} ({pct:.1f}%)",
                      end="\r", flush=True)
                continue

            upsert_embeddings(session, list(zip(ids, vecs.tolist())))

            n_done += len(batch_rows)
            batch_num = i // batch_size + 1
            pct = 100 * n_done / len(pending)
            print(f"  [{batch_num}/{n_batches}] {n_done:,}/{len(pending):,} ({pct:.1f}%)",
                  end="\r", flush=True)

        print(f"\n\nDone. {n_done:,} embeddings written.")


if __name__ == "__main__":
    main()
