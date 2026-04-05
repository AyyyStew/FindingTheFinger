"""
scripts/add_bhagavatam.py

Migrates the Srimad Bhagavatam from corpus.duckdb into postgres.

Hierarchy:
    height=2  Skandha  — "Skandha 1" ... "Skandha 12"           (12)
    height=1  Adhyaya  — "Skandha 1 Adhyaya 1"                  (335)
    height=0  Shloka   — "SB 1.1.1"                             (13004)

Adhyaya metadata stores the traditional chapter name (translator title).

Run from project root:
    python -m scripts.add_bhagavatam
"""

import os
import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from db.models import Tradition, Corpus, CorpusLevel, Unit, Embedding

OLD_DB       = "/home/alexs/Projects/DataSources/corpus.duckdb"
CORPUS_NAME  = "Srimad Bhagavatam"
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
        # Tradition + corpus
        # ------------------------------------------------------------------ #
        print("Migrating tradition and corpus...")
        row = old.execute("""
            SELECT ct.name, c.type, c.language, c.era, c.metadata
            FROM corpus c JOIN corpus_tradition ct ON c.tradition_id = ct.id
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
        # corpus_level
        # ------------------------------------------------------------------ #
        for height, name in [(2, "Skandha"), (1, "Adhyaya"), (0, "Shloka")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        # ------------------------------------------------------------------ #
        # Units
        # ------------------------------------------------------------------ #
        print("Fetching passages...")
        passages = old.execute("""
            SELECT p.id, p.book, p.unit_label, p.text,
                   CAST(p.metadata->>'$.canto'   AS INTEGER) AS canto,
                   CAST(p.metadata->>'$.chapter' AS INTEGER) AS chapter
            FROM passage p JOIN corpus c ON p.corpus_id = c.id
            WHERE c.name = ?
            ORDER BY canto, chapter, p.unit_number
        """, [CORPUS_NAME]).fetchall()
        print(f"  {len(passages)} shlokas")

        skandha_units: dict[int, int]         = {}  # canto_num  -> unit.id
        adhyaya_units: dict[tuple, int]       = {}  # (canto, chapter) -> unit.id
        old_to_new:    dict[int, int]         = {}

        print("Inserting units...")
        for old_id, chapter_name, unit_label, text_, canto, chapter in passages:

            # Skandha (depth=0)
            if canto not in skandha_units:
                u = Unit(
                    corpus_id=corpus.id,
                    parent_id=None,
                    depth=0,
                    label=f"Skandha {canto}",
                    meta={"skandha": canto},
                )
                session.add(u)
                session.flush()
                skandha_units[canto] = u.id

            # Adhyaya (depth=1) — label is numeric, name stored in metadata
            adhyaya_key = (canto, chapter)
            if adhyaya_key not in adhyaya_units:
                u = Unit(
                    corpus_id=corpus.id,
                    parent_id=skandha_units[canto],
                    depth=1,
                    label=f"Skandha {canto} Adhyaya {chapter}",
                    meta={"skandha": canto, "adhyaya": chapter, "name": chapter_name},
                )
                session.add(u)
                session.flush()
                adhyaya_units[adhyaya_key] = u.id

            # Shloka (depth=2) — label uses standard SB citation format
            shloka_num = unit_label.rsplit(".", 1)[-1]  # extract verse number
            u = Unit(
                corpus_id=corpus.id,
                parent_id=adhyaya_units[adhyaya_key],
                depth=2,
                label=f"SB {canto}.{chapter}.{shloka_num}",
                text=text_,
            )
            session.add(u)
            session.flush()
            old_to_new[old_id] = u.id

        print(f"  {len(skandha_units)} skandhas, {len(adhyaya_units)} adhyayas, {len(old_to_new)} shlokas")

        compute_heights(session, corpus.id)

        # ------------------------------------------------------------------ #
        # Embeddings
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
            if (i // BATCH + 1) % 5 == 0:
                session.flush()
                print(f"  {migrated}/{len(old_ids)}...")

        session.flush()
        print(f"  {migrated} embeddings migrated")

        # ------------------------------------------------------------------ #
        # Sanity check
        # ------------------------------------------------------------------ #
        print("\nSanity check:")
        for height, name in [(2, "Skandha"), (1, "Adhyaya"), (0, "Shloka")]:
            count = session.execute(
                text("SELECT COUNT(*) FROM unit WHERE corpus_id = :cid AND height = :h"),
                {"cid": corpus.id, "h": height},
            ).scalar()
            print(f"  height={height} ({name}): {count}")

        print("\nSample Adhyayas:")
        rows = session.execute(text("""
            SELECT label, metadata->>'name' as chapter_name
            FROM unit WHERE corpus_id = :cid AND height = 1
            ORDER BY id LIMIT 5
        """), {"cid": corpus.id}).fetchall()
        for r in rows:
            print(f"  {r[0]}  —  {r[1]}")

        session.commit()

    old.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
