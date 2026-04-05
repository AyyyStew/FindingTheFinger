"""
scripts/add_quran.py

Migrates the Quran from corpus.duckdb into postgres.

Hierarchy:
    height=1  Surah  — "The Opener", "The Cow", ...  (114)
    height=0  Ayah   — "The Opener 1:1", ...          (6236)

Run from project root:
    python -m scripts.add_quran
"""

import os
import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from db.models import Tradition, Corpus, CorpusLevel, Unit, Embedding

OLD_DB       = "/home/alexs/Projects/DataSources/corpus.duckdb"
CORPUS_NAME  = "Quran (Clear Quran Translation)"
MODEL        = "all-mpnet-base-v2"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://ftf:ftf@localhost:5432/ftf")


def compute_heights(session: Session, corpus_id: int) -> None:
    print("  Computing heights...")
    session.execute(text("""
        UPDATE unit SET height = 0
        WHERE corpus_id = :cid
          AND id NOT IN (SELECT DISTINCT parent_id FROM unit WHERE parent_id IS NOT NULL)
    """), {"cid": corpus_id})

    while True:
        result = session.execute(text("""
            UPDATE unit SET height = sub.h
            FROM (
                SELECT parent_id AS id, MAX(height) + 1 AS h
                FROM unit
                WHERE corpus_id = :cid AND height IS NOT NULL
                GROUP BY parent_id
            ) sub
            WHERE unit.id = sub.id
              AND unit.corpus_id = :cid
              AND unit.height IS NULL
        """), {"cid": corpus_id})
        if result.rowcount == 0:
            break

    remaining = session.execute(
        text("SELECT COUNT(*) FROM unit WHERE corpus_id = :cid AND height IS NULL"),
        {"cid": corpus_id}
    ).scalar()
    print("  Heights OK" if not remaining else f"  WARNING: {remaining} units with NULL height")


def main():
    print("Connecting...")
    engine = create_engine(DATABASE_URL)
    old    = duckdb.connect(OLD_DB, read_only=True)

    with Session(engine) as session:

        # ------------------------------------------------------------------ #
        # 1. Tradition + corpus
        # ------------------------------------------------------------------ #
        print("Migrating tradition and corpus...")

        row = old.execute("""
            SELECT ct.name, c.type, c.language, c.era, c.metadata
            FROM corpus c
            JOIN corpus_tradition ct ON c.tradition_id = ct.id
            WHERE c.name = ?
        """, [CORPUS_NAME]).fetchone()
        if not row:
            raise RuntimeError(f"Not found in old DB: {CORPUS_NAME}")
        trad_name, c_type, c_lang, c_era, c_meta = row

        tradition = session.query(Tradition).filter_by(name=trad_name).first()
        if not tradition:
            tradition = Tradition(name=trad_name)
            session.add(tradition)
            session.flush()

        corpus = session.query(Corpus).filter_by(name=CORPUS_NAME).first()
        if corpus:
            print(f"  Already exists (id={corpus.id}), skipping.")
        else:
            corpus = Corpus(
                tradition_id=tradition.id,
                name=CORPUS_NAME,
                type=c_type, language=c_lang, era=c_era, meta=c_meta,
            )
            session.add(corpus)
            session.flush()
        print(f"  corpus_id = {corpus.id}")

        # ------------------------------------------------------------------ #
        # 2. corpus_level
        # ------------------------------------------------------------------ #
        for height, name in [(1, "Surah"), (0, "Ayah")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        # ------------------------------------------------------------------ #
        # 3. Units
        # ------------------------------------------------------------------ #
        print("Fetching passages...")
        passages = old.execute("""
            SELECT p.id, p.book, p.unit_number, p.unit_label, p.text
            FROM passage p
            JOIN corpus c ON p.corpus_id = c.id
            WHERE c.name = ?
            ORDER BY CAST(p.section AS INTEGER), p.unit_number
        """, [CORPUS_NAME]).fetchall()
        print(f"  {len(passages)} ayahs")

        surah_units: dict[str, int] = {}  # surah name -> unit.id
        old_to_new:  dict[int, int] = {}

        print("Inserting units...")
        for old_id, book, unit_number, unit_label, text_ in passages:

            # Surah (depth=0)
            if book not in surah_units:
                u = Unit(corpus_id=corpus.id, parent_id=None, depth=0, label=book)
                session.add(u)
                session.flush()
                surah_units[book] = u.id

            # Ayah (depth=1)
            u = Unit(
                corpus_id=corpus.id,
                parent_id=surah_units[book],
                depth=1,
                label=unit_label,
                text=text_,
            )
            session.add(u)
            session.flush()
            old_to_new[old_id] = u.id

        print(f"  {len(surah_units)} surahs, {len(old_to_new)} ayahs")

        # ------------------------------------------------------------------ #
        # 4. Heights
        # ------------------------------------------------------------------ #
        compute_heights(session, corpus.id)

        # ------------------------------------------------------------------ #
        # 5. Embeddings
        # ------------------------------------------------------------------ #
        print("Migrating embeddings...")
        old_ids  = list(old_to_new.keys())
        BATCH    = 2000
        migrated = 0

        for i in range(0, len(old_ids), BATCH):
            batch        = old_ids[i:i + BATCH]
            placeholders = ", ".join("?" * len(batch))
            rows = old.execute(
                f"SELECT passage_id, vector FROM embedding WHERE model_name = ? AND passage_id IN ({placeholders})",
                [MODEL] + batch,
            ).fetchall()
            for old_pid, vector in rows:
                session.add(Embedding(unit_id=old_to_new[old_pid], model_name=MODEL, vector=vector))
            migrated += len(rows)

        session.flush()
        print(f"  {migrated} embeddings migrated")

        # ------------------------------------------------------------------ #
        # 6. Sanity check
        # ------------------------------------------------------------------ #
        print("\nSanity check:")
        for height, name in [(1, "Surah"), (0, "Ayah")]:
            count = session.execute(
                text("SELECT COUNT(*) FROM unit WHERE corpus_id = :cid AND height = :h"),
                {"cid": corpus.id, "h": height},
            ).scalar()
            print(f"  height={height} ({name}): {count}")

        emb = session.execute(text("""
            SELECT COUNT(*) FROM embedding e
            JOIN unit u ON e.unit_id = u.id WHERE u.corpus_id = :cid
        """), {"cid": corpus.id}).scalar()
        print(f"  embeddings: {emb}")

        session.commit()

    old.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
