"""
scripts/add_vendidad.py

Ingests the Vendidad (Darmesteter translation) from the legacy DuckDB.

Source: /home/alexs/Projects/DataSources/corpus.duckdb (corpus_id=42)

Hierarchy:
    height=1  Fargard  — "Vendidad 1" … "Vendidad 22"   (22)
    height=0  Verse    — "Vendidad 1:1", "Vendidad 4:3"  (1232)

Fargard titles stored in metadata.

Run from project root:
    python -m scripts.add_vendidad
"""

import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from db.models import Tradition, Corpus, CorpusLevel, Unit

DUCK_PATH    = "/home/alexs/Projects/DataSources/corpus.duckdb"
DUCK_CID     = 42
CORPUS_NAME  = "Vendidad (Darmesteter)"
TRADITION    = "Zoroastrian"
DATABASE_URL = "postgresql://ftf:ftf@localhost:5432/ftf"


def load_data() -> dict[int, dict]:
    """Returns {fargard_num: {title, verses: [(unit_num, text)]}}"""
    con = duckdb.connect(DUCK_PATH, read_only=True)
    rows = con.execute("""
        SELECT
            unit_number,
            text,
            CAST(json_extract(metadata, '$.fargard_num')   AS INTEGER) AS fn,
            json_extract(metadata, '$.fargard_title')                   AS title
        FROM passage
        WHERE corpus_id = ?
        ORDER BY CAST(json_extract(metadata, '$.fargard_num') AS INTEGER), unit_number
    """, [DUCK_CID]).fetchall()
    con.close()

    fargards: dict[int, dict] = {}
    for unit_num, text_val, fn, title_raw in rows:
        if fn not in fargards:
            title = title_raw.strip('"') if title_raw and title_raw != 'null' else None
            fargards[fn] = {"title": title, "verses": []}
        fargards[fn]["verses"].append((unit_num, text_val))

    return fargards


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
        {"cid": corpus_id},
    ).scalar()
    print("  Heights OK" if not remaining else f"  WARNING: {remaining} units with NULL height")


def main() -> None:
    print("Loading from DuckDB...")
    fargards = load_data()
    total_verses = sum(len(f["verses"]) for f in fargards.values())
    print(f"  {len(fargards)} fargards, {total_verses} verses")

    print("\nConnecting to PostgreSQL...")
    engine = create_engine(DATABASE_URL)

    with Session(engine) as session:

        tradition = session.query(Tradition).filter_by(name=TRADITION).first()
        if not tradition:
            tradition = Tradition(name=TRADITION)
            session.add(tradition)
            session.flush()

        corpus = session.query(Corpus).filter_by(name=CORPUS_NAME).first()
        if corpus:
            print(f"  Already exists (id={corpus.id}), skipping.")
            return
        corpus = Corpus(
            tradition_id=tradition.id,
            name=CORPUS_NAME,
            type="scripture",
            language="English",
            era="6th–4th century BCE",
        )
        session.add(corpus)
        session.flush()
        print(f"  corpus_id = {corpus.id}")

        for height, name in [(1, "Fargard"), (0, "Verse")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        print("Inserting units...")
        n_verses = 0
        for fn in sorted(fargards.keys()):
            fargard = fargards[fn]
            fg_unit = Unit(
                corpus_id=corpus.id,
                parent_id=None,
                depth=0,
                label=f"Vendidad {fn}",
                meta={"fargard": fn, "title": fargard["title"]},
            )
            session.add(fg_unit)
            session.flush()

            for unit_num, text_val in fargard["verses"]:
                session.add(Unit(
                    corpus_id=corpus.id,
                    parent_id=fg_unit.id,
                    depth=1,
                    label=f"Vendidad {fn}:{unit_num}",
                    text=text_val,
                    meta={"fargard": fn, "verse": unit_num},
                ))
                n_verses += 1

        session.flush()
        print(f"  {len(fargards)} fargards, {n_verses} verses inserted")

        compute_heights(session, corpus.id)

        print("\nSanity check:")
        for height, name in [(1, "Fargard"), (0, "Verse")]:
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
            LIMIT 10
        """), {"cid": corpus.id}).fetchall()
        for r in rows:
            print(f"  h={r[0]}  {r[1]}")

        session.commit()

    print("\nDone.")


if __name__ == "__main__":
    main()
