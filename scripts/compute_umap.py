"""
scripts/compute_umap.py

Computes a 2D UMAP projection of all height=0 unit embeddings and stores
results in the umap_point table. Positions for parent units (height > 0)
are derived as the mean of their children's 2D coordinates.

By default, UMAP is *fit* on a balanced sample — up to --sample-per-division
leaves drawn from every depth=0 division (Book, Raag, Surah, etc.) across all
corpora. All leaves are then *transformed* into that space. This prevents large
corpora from dominating the projection.  Pass --no-sample to fit on everything.

Each run creates a new umap_run record so old projections are preserved
(visible at /map/<run_id>). The latest run is served at /map.

Usage:
    # Default parameters (balanced sample, 100 leaves per division)
    python -m scripts.compute_umap

    # Custom sample size
    python -m scripts.compute_umap --sample-per-division 50

    # Disable sampling (fit on all points)
    python -m scripts.compute_umap --no-sample

    # With label and custom UMAP params
    python -m scripts.compute_umap --label "v2" --n-neighbors 15 --min-dist 0.1

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

DEFAULT_N_NEIGHBORS       = 15
DEFAULT_MIN_DIST          = 0.1
DEFAULT_SAMPLE_PER_DIV    = 100
UMAP_METRIC               = "cosine"
UMAP_RANDOM_STATE         = 42
BATCH_INSERT              = 5_000


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--label",                default=None,   help="Human-readable label for this run")
    p.add_argument("--n-neighbors",          type=int,   default=DEFAULT_N_NEIGHBORS)
    p.add_argument("--min-dist",             type=float, default=DEFAULT_MIN_DIST)
    p.add_argument("--sample-per-division",  type=int,   default=DEFAULT_SAMPLE_PER_DIV,
                   help="Max leaves sampled per depth=0 division for UMAP fit (default: 100)")
    p.add_argument("--no-sample",            action="store_true",
                   help="Fit UMAP on all points instead of a balanced sample")
    p.add_argument("--dry-run",              action="store_true")
    p.add_argument("--no-gpu",               action="store_true", help="Force CPU UMAP even if cuML is available")
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


def sample_by_division(
    session: Session,
    unit_ids: list[int],
    n_per_division: int,
) -> np.ndarray:
    """
    For every depth=0 division (Book, Raag, Surah, …) across all corpora,
    sample up to n_per_division leaf indices. Returns a sorted int64 array
    of indices into unit_ids / the embedding matrix.
    """
    print(f"\nBuilding balanced sample ({n_per_division} leaves per depth=0 division)...")

    id_to_idx = {uid: i for i, uid in enumerate(unit_ids)}

    # Walk up at most 2 levels to find the depth=0 ancestor of each leaf.
    # Works for corpora with up to 3 levels (height 0/1/2).
    rows = session.execute(text("""
        SELECT l.id AS leaf_id,
               CASE
                   WHEN p1.depth = 0 THEN p1.id
                   WHEN p2.depth = 0 THEN p2.id
               END AS div_id
        FROM       unit l
        LEFT JOIN  unit p1 ON p1.id = l.parent_id
        LEFT JOIN  unit p2 ON p2.id = p1.parent_id
        WHERE l.height = 0
    """)).fetchall()

    div_to_indices: dict[int, list[int]] = defaultdict(list)
    unassigned = 0
    for leaf_id, div_id in rows:
        if div_id is None:
            unassigned += 1
            continue
        idx = id_to_idx.get(leaf_id)
        if idx is not None:
            div_to_indices[div_id].append(idx)

    if unassigned:
        print(f"  Warning: {unassigned:,} leaves had no depth=0 ancestor (skipped from sample)")

    rng = np.random.default_rng(UMAP_RANDOM_STATE)
    sampled: list[int] = []
    for div_id, indices in div_to_indices.items():
        k = min(n_per_division, len(indices))
        sampled.extend(rng.choice(indices, size=k, replace=False).tolist())

    sampled_arr = np.array(sorted(set(sampled)), dtype=np.int64)
    print(f"  {len(div_to_indices):,} divisions  →  {len(sampled_arr):,} sample leaves"
          f"  (out of {len(unit_ids):,} total)")
    return sampled_arr


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

def _make_reducer(n_neighbors: int, min_dist: float, backend: str):
    if backend == "gpu":
        from cuml.manifold import UMAP as cuUMAP
        return cuUMAP(
            n_components=2,
            n_neighbors=n_neighbors,
            min_dist=min_dist,
            metric=UMAP_METRIC,
            random_state=UMAP_RANDOM_STATE,
            verbose=True,
        )
    import umap as umap_lib
    return umap_lib.UMAP(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=UMAP_METRIC,
        random_state=UMAP_RANDOM_STATE,
        verbose=True,
    )


def _to_numpy(coords) -> np.ndarray:
    if hasattr(coords, "get"):   # cupy array
        coords = coords.get()
    return np.array(coords, dtype=np.float32)


def run_umap(
    matrix: np.ndarray,
    n_neighbors: int,
    min_dist: float,
    force_cpu: bool = False,
    sample_indices: np.ndarray | None = None,
) -> np.ndarray:
    """
    Fit UMAP on matrix[sample_indices] (or all of matrix if sample_indices is None),
    then transform the full matrix. Returns float32 coords for every row.
    """
    use_sample = sample_indices is not None
    fit_matrix = matrix[sample_indices] if use_sample else matrix
    label = f"sample n={len(fit_matrix):,}" if use_sample else f"all n={len(matrix):,}"
    print(f"\nRunning UMAP  n_neighbors={n_neighbors}  min_dist={min_dist}"
          f"  metric={UMAP_METRIC}  fit on {label}...")

    backend = "cpu"
    if not force_cpu:
        try:
            import cuml  # noqa: F401
            backend = "gpu"
        except ImportError:
            print("  cuML not found — falling back to CPU UMAP")

    print(f"  Backend: {'cuML (GPU)' if backend == 'gpu' else 'umap-learn (CPU)'}")
    reducer = _make_reducer(n_neighbors, min_dist, backend)

    reducer.fit(fit_matrix)
    print(f"  Fit done. Transforming all {len(matrix):,} points...")
    coords = _to_numpy(reducer.transform(matrix))

    print(f"  Done.  x∈[{coords[:,0].min():.3f}, {coords[:,0].max():.3f}]"
          f"  y∈[{coords[:,1].min():.3f}, {coords[:,1].max():.3f}]")
    return coords


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
            if not args.no_sample:
                sample_indices = sample_by_division(session, leaf_ids, args.sample_per_division)
                print(f"\nDry run: would fit UMAP on {len(sample_indices):,} sampled leaves,"
                      f" then transform all {len(leaf_ids):,}.")
            else:
                print(f"\nDry run: would fit+transform UMAP on all {len(leaf_ids):,} leaves.")
            return

        sample_indices = None if args.no_sample else sample_by_division(
            session, leaf_ids, args.sample_per_division
        )

        coords = run_umap(matrix, args.n_neighbors, args.min_dist,
                          force_cpu=args.no_gpu, sample_indices=sample_indices)

        print("\nAggregating parent positions...")
        parent_of, children_of, height_of = load_unit_tree(session)
        positions = aggregate_parents(leaf_ids, coords, children_of, parent_of, height_of)

        print("Computing corpus sequences...")
        corpus_seqs = compute_corpus_seqs(session, leaf_ids)

        write_run(session, args.label, args.n_neighbors, args.min_dist, positions, corpus_seqs)


if __name__ == "__main__":
    main()
