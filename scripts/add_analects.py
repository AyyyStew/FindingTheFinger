"""
scripts/add_analects.py

Migrates the Analects of Confucius (Legge) from corpus.duckdb into postgres.

Hierarchy:
    height=1  Book     — "Book I", "Book II", ...  (20)
    height=0  Chapter  — "Analects 1.1", ...        (326)

Old DB labels used "Book N 1.N" (section always 1) — rebuilt as "Analects N.N".

Run from project root:
    python -m scripts.add_analects
"""

import os
import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from db.models import Tradition, Corpus, CorpusLevel, Unit, Embedding

OLD_DB       = "/home/alexs/Projects/DataSources/corpus.duckdb"
CORPUS_NAME  = "Analects of Confucius (Legge)"
MODEL        = "all-mpnet-base-v2"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://ftf:ftf@localhost:5432/ftf")

BOOK_NUMS = {
    "Book I": 1, "Book II": 2, "Book III": 3, "Book IV": 4, "Book V": 5,
    "Book VI": 6, "Book VII": 7, "Book VIII": 8, "Book IX": 9, "Book X": 10,
    "Book XI": 11, "Book XII": 12, "Book XIII": 13, "Book XIV": 14, "Book XV": 15,
    "Book XVI": 16, "Book XVII": 17, "Book XVIII": 18, "Book XIX": 19, "Book XX": 20,
}


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
        for height, name in [(1, "Book"), (0, "Chapter")]:
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
        print(f"  {len(passages)} chapters")

        book_units: dict[str, int] = {}
        old_to_new: dict[int, int] = {}

        print("Inserting units...")
        for old_id, book, unit_number, text_ in passages:
            book_num = BOOK_NUMS[book]

            if book not in book_units:
                u = Unit(
                    corpus_id=corpus.id,
                    parent_id=None,
                    depth=0,
                    label=book,
                    meta={"book": book_num},
                )
                session.add(u)
                session.flush()
                book_units[book] = u.id

            u = Unit(
                corpus_id=corpus.id,
                parent_id=book_units[book],
                depth=1,
                label=f"Analects {book_num}.{unit_number}",
                text=text_,
            )
            session.add(u)
            session.flush()
            old_to_new[old_id] = u.id

        print(f"  {len(book_units)} books, {len(old_to_new)} chapters")

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
        for height, name in [(1, "Book"), (0, "Chapter")]:
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
