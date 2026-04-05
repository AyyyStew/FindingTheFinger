"""
db/schema.py

Creates (or connects to) the DuckDB database and sets up all tables.
Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.

Usage:
    from db.schema import get_conn
    conn = get_conn()
"""

import duckdb
import os

DB_PATH = os.environ.get("CORPUS_DB", "/home/alexs/Projects/DataSources/corpus.duckdb")


def get_conn(path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = duckdb.connect(path)
    _create_tables(conn)
    return conn


def _create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS tradition_id_seq START 1;
        CREATE SEQUENCE IF NOT EXISTS corpus_id_seq START 1;
        CREATE SEQUENCE IF NOT EXISTS passage_id_seq START 1;
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS corpus_tradition (
            id      INTEGER PRIMARY KEY DEFAULT nextval('tradition_id_seq'),
            name    TEXT NOT NULL UNIQUE
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS corpus (
            id           INTEGER PRIMARY KEY DEFAULT nextval('corpus_id_seq'),
            tradition_id INTEGER REFERENCES corpus_tradition(id),
            name         TEXT NOT NULL UNIQUE,
            type         TEXT,       -- scripture | legal | news | music | literature
            language     TEXT,       -- ISO 639-1: en, zh, sa, ar ...
            era          TEXT,       -- ancient | medieval | modern
            metadata     JSON
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS passage (
            id          INTEGER PRIMARY KEY DEFAULT nextval('passage_id_seq'),
            corpus_id   INTEGER NOT NULL REFERENCES corpus(id),
            book        TEXT,
            section     TEXT,
            unit_number INTEGER,
            unit_label  TEXT,       -- human-readable ref: "1:1", "Ch.3 v.5"
            text        TEXT NOT NULL,
            metadata    JSON
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS embedding (
            passage_id  INTEGER NOT NULL REFERENCES passage(id),
            model_name  TEXT    NOT NULL,
            vector      FLOAT[],
            PRIMARY KEY (passage_id, model_name)
        )
    """)


if __name__ == "__main__":
    conn = get_conn()
    print(f"Database ready at: {DB_PATH}")
    tables = conn.execute("SHOW TABLES").fetchall()
    for t in tables:
        print(f"  {t[0]}")
    conn.close()
