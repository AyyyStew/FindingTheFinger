"""
export_kaggle.py — export the FindingTheFinger database to Parquet files
suitable for uploading to Kaggle.

Usage:
    python -m scripts.export_kaggle [--out ./kaggle_export]

Output files:
    traditions.parquet      — tradition names
    corpora.parquet         — corpus names + tradition FK
    corpus_levels.parquet   — height → level name per corpus
    units.parquet           — all units (verses, chapters, books) with text
    embeddings.parquet      — nomic-embed-text-v1.5 vectors (768-dim float32)
    umap_points.parquet     — 2D UMAP projection coordinates
"""

import argparse
import os
import struct
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

EMBED_CHUNK = 5_000  # rows per chunk for embedding export (memory management)


def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def _pgvector_to_numpy(raw) -> np.ndarray:
    """
    Convert a pgvector value (returned by psycopg2 as a string like '[0.1,0.2,...]')
    into a float32 numpy array.
    """
    if isinstance(raw, str):
        return np.array(raw.strip("[]").split(","), dtype=np.float32)
    # Some driver versions return bytes
    if isinstance(raw, (bytes, bytearray, memoryview)):
        b = bytes(raw)
        # pgvector binary format: 4-byte uint16 dim, 4-byte uint16 unused, then float32s
        dim = struct.unpack_from("<H", b, 0)[0]
        return np.frombuffer(b, dtype="<f4", count=dim, offset=4)
    raise TypeError(f"Unexpected vector type: {type(raw)}")


def export_simple(conn, out_dir: Path, table: str, query: str, schema_desc: str):
    print(f"  Exporting {table}… ", end="", flush=True)
    rows = conn.execute(text(query)).fetchall()
    keys = conn.execute(text(query)).keys()
    data = {k: [r[i] for r in rows] for i, k in enumerate(keys)}
    table_pa = pa.table(data)
    path = out_dir / f"{table}.parquet"
    pq.write_table(table_pa, path, compression="snappy")
    print(f"{len(rows):,} rows → {path.name} ({path.stat().st_size / 1024:.0f} KB)")


def export_embeddings(conn, out_dir: Path):
    print("  Exporting embeddings… ", end="", flush=True)
    count = conn.execute(text("SELECT COUNT(*) FROM embedding")).scalar()
    print(f"{count:,} rows, chunking by {EMBED_CHUNK}…")

    writers = {}
    offset = 0
    total = 0

    while offset < count:
        rows = conn.execute(text("""
            SELECT unit_id, model_name, vector::text
            FROM embedding
            ORDER BY unit_id
            LIMIT :lim OFFSET :off
        """), {"lim": EMBED_CHUNK, "off": offset}).fetchall()

        if not rows:
            break

        unit_ids   = pa.array([r[0] for r in rows], type=pa.int64())
        model_names = pa.array([r[1] for r in rows], type=pa.string())
        vectors    = pa.array(
            [_pgvector_to_numpy(r[2]).tolist() for r in rows],
            type=pa.list_(pa.float32()),
        )

        batch = pa.table({
            "unit_id":    unit_ids,
            "model_name": model_names,
            "vector":     vectors,
        })

        path = out_dir / "embeddings.parquet"
        if offset == 0:
            writers["emb"] = pq.ParquetWriter(path, batch.schema, compression="snappy")
        writers["emb"].write_table(batch)

        offset += len(rows)
        total  += len(rows)
        print(f"    {total:,} / {count:,}", end="\r", flush=True)

    if "emb" in writers:
        writers["emb"].close()

    path = out_dir / "embeddings.parquet"
    print(f"\n    → {path.name} ({path.stat().st_size / 1024 / 1024:.1f} MB)")


def main():
    parser = argparse.ArgumentParser(description="Export FTF database to Parquet")
    parser.add_argument("--out", default="./kaggle_export", help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Exporting to {out_dir.resolve()}\n")

    engine = get_engine()
    with engine.connect() as conn:

        # ── Small reference tables ──────────────────────────────────────────

        export_simple(conn, out_dir, "traditions",
            "SELECT id, name FROM tradition ORDER BY id",
            "tradition id + name")

        export_simple(conn, out_dir, "corpora",
            """SELECT c.id, c.name, t.name AS tradition
               FROM corpus c JOIN tradition t ON c.tradition_id = t.id
               ORDER BY c.id""",
            "corpus id, name, tradition name")

        export_simple(conn, out_dir, "corpus_levels",
            """SELECT c.name AS corpus, cl.height, cl.name AS level_name
               FROM corpus_level cl JOIN corpus c ON cl.corpus_id = c.id
               ORDER BY c.id, cl.height""",
            "corpus + height → level name")

        # ── Units (the main text corpus) ────────────────────────────────────

        print("  Exporting units… ", end="", flush=True)
        unit_count = conn.execute(text("SELECT COUNT(*) FROM unit")).scalar()
        print(f"{unit_count:,} rows, chunking…")

        unit_path = out_dir / "units.parquet"
        unit_writer = None
        offset = 0

        while offset < unit_count:
            rows = conn.execute(text("""
                SELECT u.id, c.name AS corpus, t.name AS tradition,
                       u.parent_id, u.height, u.depth, u.label, u.text
                FROM unit u
                JOIN corpus c ON u.corpus_id = c.id
                JOIN tradition t ON c.tradition_id = t.id
                ORDER BY u.id
                LIMIT 10000 OFFSET :off
            """), {"off": offset}).fetchall()

            if not rows:
                break

            batch = pa.table({
                "id":        pa.array([r[0] for r in rows], type=pa.int64()),
                "corpus":    pa.array([r[1] for r in rows], type=pa.string()),
                "tradition": pa.array([r[2] for r in rows], type=pa.string()),
                "parent_id": pa.array([r[3] for r in rows], type=pa.int64()),
                "height":    pa.array([r[4] for r in rows], type=pa.int32()),
                "depth":     pa.array([r[5] for r in rows], type=pa.int32()),
                "label":     pa.array([r[6] for r in rows], type=pa.string()),
                "text":      pa.array([r[7] for r in rows], type=pa.string()),
            })

            if unit_writer is None:
                unit_writer = pq.ParquetWriter(unit_path, batch.schema, compression="snappy")
            unit_writer.write_table(batch)
            offset += len(rows)
            print(f"    {offset:,} / {unit_count:,}", end="\r", flush=True)

        if unit_writer:
            unit_writer.close()
        print(f"\n    → {unit_path.name} ({unit_path.stat().st_size / 1024:.0f} KB)")

        # ── Embeddings ──────────────────────────────────────────────────────

        export_embeddings(conn, out_dir)

        # ── UMAP points ─────────────────────────────────────────────────────

        export_simple(conn, out_dir, "umap_points",
            """SELECT up.unit_id, u.label, c.name AS corpus, t.name AS tradition,
                      up.x, up.y
               FROM umap_point up
               JOIN unit u ON up.unit_id = u.id
               JOIN corpus c ON u.corpus_id = c.id
               JOIN tradition t ON c.tradition_id = t.id
               ORDER BY up.unit_id""",
            "UMAP 2D coordinates per unit")

    print("\nDone. Files written:")
    for f in sorted(out_dir.glob("*.parquet")):
        print(f"  {f.name:35s} {f.stat().st_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()
