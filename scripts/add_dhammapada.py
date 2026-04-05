"""
scripts/add_dhammapada.py

Migrates the Dhammapada (Müller) from corpus.duckdb into postgres.

Hierarchy:
    height=1  Vagga  — "The Twin-Verses", "On Earnestness", ...  (26)
    height=0  Verse  — "Dhp 1.1", "Dhp 1.2", ...                (404)

Old DB labels used "VaggaName 1.N" (section always 1) — rebuilt as "Dhp chapter.verse".

Run from project root:
    python -m scripts.add_dhammapada
"""

import os
import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from db.models import Tradition, Corpus, CorpusLevel, Unit, Embedding

OLD_DB       = "/home/alexs/Projects/DataSources/corpus.duckdb"
CORPUS_NAME  = "Dhammapada (Müller)"
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

        # Tradition + corpus
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

        # Levels
        for height, name in [(1, "Vagga"), (0, "Verse")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        # Units
        print("Fetching passages...")
        passages = old.execute("""
            SELECT p.id, p.book, p.unit_number, p.text
            FROM passage p JOIN corpus c ON p.corpus_id = c.id
            WHERE c.name = ?
            ORDER BY CAST(p.section AS INTEGER), p.unit_number
        """, [CORPUS_NAME]).fetchall()
        print(f"  {len(passages)} verses")

        vagga_units:  dict[str, int] = {}  # book name -> unit.id
        vagga_nums:   dict[str, int] = {}  # book name -> chapter number (1-26)
        old_to_new:   dict[int, int] = {}

        print("Inserting units...")
        for old_id, book, unit_number, text_ in passages:

            # Vagga (depth=0) — first time we see this book
            if book not in vagga_units:
                chapter_num = len(vagga_units) + 1
                vagga_nums[book] = chapter_num
                u = Unit(
                    corpus_id=corpus.id,
                    parent_id=None,
                    depth=0,
                    label=book,
                    meta={"vagga": chapter_num},
                )
                session.add(u)
                session.flush()
                vagga_units[book] = u.id

            # Verse (depth=1)
            chapter_num = vagga_nums[book]
            u = Unit(
                corpus_id=corpus.id,
                parent_id=vagga_units[book],
                depth=1,
                label=f"Dhp {chapter_num}.{unit_number}",
                text=text_,
            )
            session.add(u)
            session.flush()
            old_to_new[old_id] = u.id

        print(f"  {len(vagga_units)} vaggas, {len(old_to_new)} verses")

        compute_heights(session, corpus.id)

        # Embeddings
        print("Migrating embeddings...")
        old_ids      = list(old_to_new.keys())
        placeholders = ", ".join("?" * len(old_ids))
        rows = old.execute(
            f"SELECT passage_id, vector FROM embedding WHERE model_name = ? AND passage_id IN ({placeholders})",
            [MODEL] + old_ids,
        ).fetchall()
        for old_pid, vector in rows:
            session.add(Embedding(unit_id=old_to_new[old_pid], model_name=MODEL, vector=vector))
        session.flush()
        print(f"  {len(rows)} embeddings migrated")

        # Sanity check
        print("\nSanity check:")
        for height, name in [(1, "Vagga"), (0, "Verse")]:
            count = session.execute(
                text("SELECT COUNT(*) FROM unit WHERE corpus_id = :cid AND height = :h"),
                {"cid": corpus.id, "h": height},
            ).scalar()
            print(f"  height={height} ({name}): {count}")

        print("\nSample labels:")
        rows = session.execute(text("""
            SELECT height, label FROM unit
            WHERE corpus_id = :cid
            ORDER BY height DESC, id
            LIMIT 8
        """), {"cid": corpus.id}).fetchall()
        for r in rows:
            print(f"  h={r[0]}  {r[1]}")

        session.commit()

    old.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
