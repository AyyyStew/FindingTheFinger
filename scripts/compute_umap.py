"""
scripts/compute_umap.py

Computes a 2D UMAP projection of all height=0 unit embeddings and stores
results in the umap_point table. Positions for parent units (height > 0)
are derived as the mean of their children's 2D coordinates.

Each run creates a new umap_run record so old projections are preserved
(visible at /map/<run_id>). The latest run is served at /map.

Usage:
    # Default parameters
    python -m scripts.compute_umap

    # With label and custom UMAP params
    python -m scripts.compute_umap --label "after SGGS" --n-neighbors 15 --min-dist 0.1

    # Dry run (count embeddings, don't compute)
    python -m scripts.compute_umap --dry-run
"""

import argparse
from collections import defaultdict

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models import UmapRun, UmapPoint

DATABASE_URL  = "postgresql://ftf:ftf@localhost:5432/ftf"
MODEL_NAME    = "nomic-embed-text-v1.5"

DEFAULT_N_NEIGHBORS = 15
DEFAULT_MIN_DIST    = 0.1
UMAP_METRIC         = "cosine"
UMAP_RANDOM_STATE   = 42
BATCH_INSERT        = 5_000


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--label",       default=None,              help="Human-readable label for this run")
    p.add_argument("--n-neighbors", type=int,   default=DEFAULT_N_NEIGHBORS)
    p.add_argument("--min-dist",    type=float, default=DEFAULT_MIN_DIST)
    p.add_argument("--dry-run",     action="store_true")
    p.add_argument("--no-gpu",      action="store_true", help="Force CPU UMAP even if cuML is available")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_embeddings(session: Session) -> tuple[list[int], np.ndarray]:
    """Load all height=0 unit embeddings. Returns (unit_ids, float32 matrix)."""
    print("Loading embeddings from DB...")
    rows = session.execute(text("""
        SELECT u.id, e.vector
        FROM   embedding e
        JOIN   unit      u ON e.unit_id = u.id
        WHERE  e.model_name = :model
          AND  u.height     = 0
        ORDER  BY u.corpus_id, u.id
    """), {"model": MODEL_NAME}).fetchall()

    if not rows:
        raise SystemExit("No embeddings found. Run scripts/embed_nomic.py first.")

    unit_ids = [r[0] for r in rows]
    vectors  = []
    for r in rows:
        v = r[1]
        if isinstance(v, str):
            v = [float(x) for x in v.strip("[]").split(",")]
        vectors.append(v)

    matrix = np.array(vectors, dtype=np.float32)
    print(f"  {len(unit_ids):,} units  |  shape: {matrix.shape}")
    return unit_ids, matrix


def load_unit_tree(session: Session) -> tuple[dict[int, int], dict[int, list[int]], dict[int, int]]:
    """
    Load the full unit tree from DB.

    Returns:
        parent_of:   child_id -> parent_id
        children_of: parent_id -> [child_id, ...]
        height_of:   unit_id -> height
    """
    print("Loading unit tree...")
    rows = session.execute(text("""
        SELECT id, parent_id, height FROM unit
        WHERE  parent_id IS NOT NULL
    """)).fetchall()

    parent_of:   dict[int, int]        = {}
    children_of: dict[int, list[int]]  = defaultdict(list)
    height_of:   dict[int, int]        = {}

    for uid, pid, h in rows:
        parent_of[uid]  = pid
        children_of[pid].append(uid)
        if h is not None:
            height_of[uid] = h

    # Also grab heights for root nodes (parent_id IS NULL)
    roots = session.execute(text("""
        SELECT id, height FROM unit WHERE parent_id IS NULL
    """)).fetchall()
    for uid, h in roots:
        if h is not None:
            height_of[uid] = h

    n_parents = len(children_of)
    print(f"  {len(parent_of):,} non-root units, {n_parents:,} parent nodes")
    return parent_of, children_of, height_of


# ---------------------------------------------------------------------------
# UMAP
# ---------------------------------------------------------------------------

def run_umap(matrix: np.ndarray, n_neighbors: int, min_dist: float, force_cpu: bool = False) -> np.ndarray:
    print(f"\nRunning UMAP  n_neighbors={n_neighbors}  min_dist={min_dist}  metric={UMAP_METRIC}...")

    if not force_cpu:
        try:
            from cuml.manifold import UMAP as cuUMAP
            print("  Backend: cuML (GPU)")
            reducer = cuUMAP(
                n_components=2,
                n_neighbors=n_neighbors,
                min_dist=min_dist,
                metric=UMAP_METRIC,
                random_state=UMAP_RANDOM_STATE,
                verbose=True,
            )
            coords = reducer.fit_transform(matrix)
            # cuML may return a cupy array — convert to numpy
            if hasattr(coords, "get"):
                coords = coords.get()
            coords = np.array(coords, dtype=np.float32)
            print(f"  Done (GPU).  x∈[{coords[:,0].min():.3f}, {coords[:,0].max():.3f}]"
                  f"  y∈[{coords[:,1].min():.3f}, {coords[:,1].max():.3f}]")
            return coords
        except ImportError:
            print("  cuML not found — falling back to CPU UMAP")

    import umap as umap_lib
    print("  Backend: umap-learn (CPU)")
    reducer = umap_lib.UMAP(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=UMAP_METRIC,
        random_state=UMAP_RANDOM_STATE,
        verbose=True,
    )
    coords = reducer.fit_transform(matrix)
    print(f"  Done (CPU).  x∈[{coords[:,0].min():.3f}, {coords[:,0].max():.3f}]"
          f"  y∈[{coords[:,1].min():.3f}, {coords[:,1].max():.3f}]")
    return coords.astype(np.float32)


# ---------------------------------------------------------------------------
# Parent aggregation
# ---------------------------------------------------------------------------

def aggregate_parents(
    leaf_ids: list[int],
    leaf_coords: np.ndarray,
    children_of: dict[int, list[int]],
    parent_of: dict[int, int],
    height_of: dict[int, int],
) -> dict[int, tuple[float, float]]:
    """Bottom-up mean aggregation for all ancestor units."""
    positions: dict[int, tuple[float, float]] = {
        uid: (float(x), float(y))
        for uid, (x, y) in zip(leaf_ids, leaf_coords)
    }

    # Walk upward level by level
    frontier = set(leaf_ids)
    visited  = set(leaf_ids)

    while frontier:
        next_frontier: set[int] = set()
        for uid in frontier:
            pid = parent_of.get(uid)
            if pid is not None and pid not in visited:
                next_frontier.add(pid)

        for pid in next_frontier:
            child_positions = [positions[c] for c in children_of[pid] if c in positions]
            if child_positions:
                arr = np.array(child_positions, dtype=np.float32)
                positions[pid] = (float(arr[:, 0].mean()), float(arr[:, 1].mean()))

        visited.update(next_frontier)
        frontier = next_frontier

    derived = len(positions) - len(leaf_ids)
    print(f"  {len(leaf_ids):,} leaf positions + {derived:,} derived parent positions"
          f" = {len(positions):,} total")
    return positions


# ---------------------------------------------------------------------------
# Corpus sequence (ordering within corpus for constellation lines)
# ---------------------------------------------------------------------------

def compute_corpus_seqs(session: Session, leaf_ids: list[int]) -> dict[int, int]:
    """Assign a sequential rank to each leaf unit within its corpus."""
    rows = session.execute(text("""
        SELECT u.id, u.corpus_id
        FROM   unit u
        WHERE  u.id = ANY(:ids)
        ORDER  BY u.corpus_id, u.id
    """), {"ids": leaf_ids}).fetchall()

    counter: dict[int, int] = defaultdict(int)
    seq: dict[int, int] = {}
    for uid, cid in rows:
        counter[cid] += 1
        seq[uid] = counter[cid]
    return seq


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def write_run(
    session: Session,
    label: str | None,
    n_neighbors: int,
    min_dist: float,
    positions: dict[int, tuple[float, float]],
    corpus_seqs: dict[int, int],
) -> int:
    print("\nWriting umap_run record...")
    run = UmapRun(
        label=label,
        model_name=MODEL_NAME,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
    )
    session.add(run)
    session.flush()
    run_id = run.id
    print(f"  umap_run.id = {run_id}")

    rows = [
        {
            "umap_run_id": run_id,
            "unit_id":     uid,
            "x":           x,
            "y":           y,
            "corpus_seq":  corpus_seqs.get(uid),
        }
        for uid, (x, y) in positions.items()
    ]

    print(f"Inserting {len(rows):,} umap_point rows (batch={BATCH_INSERT})...")
    for i in range(0, len(rows), BATCH_INSERT):
        batch = rows[i : i + BATCH_INSERT]
        session.execute(pg_insert(UmapPoint).values(batch).on_conflict_do_nothing())
        done = min(i + BATCH_INSERT, len(rows))
        print(f"  {done:,}/{len(rows):,}", end="\r", flush=True)

    session.commit()
    print(f"\nDone. Run ID={run_id}, {len(rows):,} points committed.")
    return run_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    engine = create_engine(DATABASE_URL)

    with Session(engine) as session:
        leaf_ids, matrix = load_embeddings(session)

        if args.dry_run:
            print(f"\nDry run: would project {len(leaf_ids):,} vectors to 2D.")
            return

        coords = run_umap(matrix, args.n_neighbors, args.min_dist, force_cpu=args.no_gpu)

        print("\nAggregating parent positions...")
        parent_of, children_of, height_of = load_unit_tree(session)
        positions = aggregate_parents(leaf_ids, coords, children_of, parent_of, height_of)

        print("Computing corpus sequences...")
        corpus_seqs = compute_corpus_seqs(session, leaf_ids)

        write_run(session, args.label, args.n_neighbors, args.min_dist, positions, corpus_seqs)


if __name__ == "__main__":
    main()
