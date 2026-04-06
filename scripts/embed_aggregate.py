"""
scripts/embed_aggregate.py

Computes and stores aggregated embeddings for all h>0 units by taking the
uniform mean of their leaf-descendant embeddings and renormalizing to unit
length (correct centroid on the unit hypersphere for cosine similarity).

These are stored in the existing `embedding` table so the similarity search
endpoint can query any height level directly via pgvector.

Units that already have an embedding are skipped (idempotent).

Usage:
    # All heights > 0
    python -m scripts.embed_aggregate

    # Only a specific height
    python -m scripts.embed_aggregate --height 1

    # Dry run (count units, don't write)
    python -m scripts.embed_aggregate --dry-run
"""

import argparse

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

DATABASE_URL = "postgresql://ftf:ftf@localhost:5432/ftf"
MODEL_NAME   = "nomic-embed-text-v1.5"
BATCH_SIZE   = 500


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--height",   type=int, default=None, help="Only process this height (default: all h>0)")
    p.add_argument("--dry-run",  action="store_true")
    return p.parse_args()


def get_target_units(session: Session, height: int | None) -> list[tuple[int, int]]:
    """Return (unit_id, height) for all h>0 units that don't yet have an embedding."""
    height_clause = "AND u.height = :height" if height is not None else ""
    rows = session.execute(text(f"""
        SELECT u.id, u.height
        FROM   unit u
        WHERE  u.height > 0
          {height_clause}
          AND NOT EXISTS (
              SELECT 1 FROM embedding e
              WHERE  e.unit_id    = u.id
                AND  e.model_name = :model
          )
        ORDER  BY u.height, u.id
    """), {"model": MODEL_NAME, "height": height}).fetchall()
    return [(r[0], r[1]) for r in rows]


def aggregate_unit(session: Session, unit_id: int) -> np.ndarray | None:
    """
    Fetch all leaf-descendant embeddings for unit_id via recursive CTE,
    return normalized mean vector. Returns None if no leaves found.
    """
    rows = session.execute(text("""
        WITH RECURSIVE descendants AS (
            SELECT id FROM unit WHERE id = :uid
            UNION ALL
            SELECT u.id FROM unit u
            JOIN descendants d ON u.parent_id = d.id
        )
        SELECT e.vector
        FROM   embedding e
        JOIN   unit      u ON e.unit_id = u.id
        JOIN   descendants d ON d.id    = u.id
        WHERE  u.height     = 0
          AND  e.model_name = :model
    """), {"uid": unit_id, "model": MODEL_NAME}).fetchall()

    if not rows:
        return None

    vecs = []
    for (v,) in rows:
        if isinstance(v, str):
            v = [float(x) for x in v.strip("[]").split(",")]
        vecs.append(v)

    mat  = np.array(vecs, dtype=np.float32)
    mean = mat.mean(axis=0)
    norm = np.linalg.norm(mean)
    return (mean / norm).astype(np.float32) if norm > 0 else mean


def main():
    args = parse_args()
    engine = create_engine(DATABASE_URL)

    with Session(engine) as session:
        units = get_target_units(session, args.height)

    height_desc = f"h={args.height}" if args.height is not None else "all h>0"
    print(f"Found {len(units):,} units ({height_desc}) without aggregated embeddings.")

    if args.dry_run:
        print("Dry run — exiting.")
        return

    skipped = 0
    written = 0

    with Session(engine) as session:
        for i, (uid, h) in enumerate(units):
            vec = aggregate_unit(session, uid)
            if vec is None:
                skipped += 1
                continue

            vec_str = "[" + ",".join(f"{v:.8f}" for v in vec.tolist()) + "]"
            session.execute(text("""
                INSERT INTO embedding (unit_id, model_name, vector)
                VALUES (:uid, :model, CAST(:vec AS vector))
                ON CONFLICT (unit_id, model_name) DO NOTHING
            """), {"uid": uid, "model": MODEL_NAME, "vec": vec_str})
            written += 1

            if written % BATCH_SIZE == 0:
                session.commit()
                print(f"  {i+1:,}/{len(units):,} processed — {written:,} written, {skipped:,} skipped", end="\r", flush=True)

        session.commit()

    print(f"\nDone. {written:,} embeddings written, {skipped:,} units had no leaves.")


if __name__ == "__main__":
    main()
