"""
db/schema_v2.py

Schema for corpus_v2.duckdb — unit-based hierarchy with flexible levels.

Tables:
    corpus_tradition  — unchanged from v1
    corpus            — unchanged from v1
    corpus_level      — defines level names per corpus (Surah, Ayah, Book, Verse, etc.)
    unit              — all content nodes at any level, self-referential via parent_id
    embedding         — vectors for any unit at any level
"""

import duckdb
import os

DB_PATH = os.environ.get("CORPUS_V2_DB", "/home/alexs/Projects/DataSources/corpus_v2.duckdb")


def get_conn(path: str = DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = duckdb.connect(path, read_only=read_only)
    if not read_only:
        _create_tables(conn)
    return conn


def _create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS tradition_id_seq START 1;
        CREATE SEQUENCE IF NOT EXISTS corpus_id_seq START 1;
        CREATE SEQUENCE IF NOT EXISTS unit_id_seq START 1;
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS corpus_tradition (
            id    INTEGER PRIMARY KEY DEFAULT nextval('tradition_id_seq'),
            name  TEXT NOT NULL UNIQUE
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS corpus (
            id           INTEGER PRIMARY KEY DEFAULT nextval('corpus_id_seq'),
            tradition_id INTEGER REFERENCES corpus_tradition(id),
            name         TEXT NOT NULL UNIQUE,
            type         TEXT,
            language     TEXT,
            era          TEXT,
            metadata     JSON
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS corpus_level (
            corpus_id  INTEGER REFERENCES corpus(id),
            height     INTEGER NOT NULL,
            name       TEXT NOT NULL,   -- natural name: 'Book', 'Surah', 'Verse', 'Ayah'...
            PRIMARY KEY (corpus_id, height)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS unit (
            id         INTEGER PRIMARY KEY DEFAULT nextval('unit_id_seq'),
            corpus_id  INTEGER NOT NULL REFERENCES corpus(id),
            parent_id  INTEGER,            -- parent unit id (no FK — DuckDB self-ref limitation)
            depth      INTEGER NOT NULL,   -- populated during build (root=0)
            height     INTEGER,            -- populated in second pass (leaf=0)
            label      TEXT,              -- human-readable: "Genesis", "Genesis 1", "Genesis 1:1"
            text       TEXT,              -- NULL for structural nodes (books)
            metadata   JSON
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS embedding (
            unit_id     INTEGER NOT NULL REFERENCES unit(id),
            model_name  TEXT NOT NULL,
            vector      FLOAT[],
            PRIMARY KEY (unit_id, model_name)
        )
    """)

    # Useful indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_unit_corpus  ON unit(corpus_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_unit_parent  ON unit(parent_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_unit_height  ON unit(height)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_emb_model    ON embedding(model_name)")
