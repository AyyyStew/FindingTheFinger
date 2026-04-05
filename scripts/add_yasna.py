"""
scripts/add_yasna.py

Ingests the Yasna (Mills translation) from the legacy DuckDB.

Source: /home/alexs/Projects/DataSources/corpus.duckdb (corpus_id=41)

Hierarchy:
    height=1  Chapter  — "Yasna 1" … "Yasna 72"     (72)
    height=0  Verse    — "Yasna 1:1", "Yasna 28:3"   (696)

Notes:
    - One DuckDB entry has chapter_num=NULL (translator's intro note) — skipped.
    - is_gatha flag (chapters 28–34, 43–51, 53) stored in metadata.
    - Chapter titles stored in metadata where available.

Run from project root:
    python -m scripts.add_yasna
"""

import json

import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from db.models import Tradition, Corpus, CorpusLevel, Unit

DUCK_PATH    = "/home/alexs/Projects/DataSources/corpus.duckdb"
DUCK_CID     = 41
CORPUS_NAME  = "Yasna (Mills)"
TRADITION    = "Zoroastrian"
DATABASE_URL = "postgresql://ftf:ftf@localhost:5432/ftf"


def load_data() -> dict[int, dict]:
    """Returns {chapter_num: {title, is_gatha, verses: [(unit_num, text)]}}"""
    con = duckdb.connect(DUCK_PATH, read_only=True)
    rows = con.execute("""
        SELECT
            unit_number,
            text,
            CAST(json_extract(metadata, '$.chapter_num')   AS INTEGER) AS ch,
            json_extract(metadata, '$.chapter_title')                   AS title,
            CAST(json_extract(metadata, '$.is_gatha')      AS BOOLEAN) AS is_gatha
        FROM passage
        WHERE corpus_id = ?
          AND json_extract(metadata, '$.chapter_num') IS NOT NULL
        ORDER BY CAST(json_extract(metadata, '$.chapter_num') AS INTEGER), unit_number
    """, [DUCK_CID]).fetchall()
    con.close()

    chapters: dict[int, dict] = {}
    for unit_num, text_val, ch, title_raw, is_gatha in rows:
        if ch is None:
            continue
        if ch not in chapters:
            title = title_raw.strip('"') if title_raw and title_raw != 'null' else None
            chapters[ch] = {"title": title, "is_gatha": bool(is_gatha), "verses": []}
        chapters[ch]["verses"].append((unit_num, text_val))

    return chapters


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
    chapters = load_data()
    total_verses = sum(len(c["verses"]) for c in chapters.values())
    gathas = [ch for ch, c in chapters.items() if c["is_gatha"]]
    print(f"  {len(chapters)} chapters, {total_verses} verses")
    print(f"  Gatha chapters: {sorted(gathas)}")

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

        for height, name in [(1, "Chapter"), (0, "Verse")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        print("Inserting units...")
        n_verses = 0
        for ch_num in sorted(chapters.keys()):
            ch = chapters[ch_num]
            ch_unit = Unit(
                corpus_id=corpus.id,
                parent_id=None,
                depth=0,
                label=f"Yasna {ch_num}",
                meta={
                    "chapter": ch_num,
                    "title":    ch["title"],
                    "is_gatha": ch["is_gatha"],
                },
            )
            session.add(ch_unit)
            session.flush()

            for unit_num, text_val in ch["verses"]:
                session.add(Unit(
                    corpus_id=corpus.id,
                    parent_id=ch_unit.id,
                    depth=1,
                    label=f"Yasna {ch_num}:{unit_num}",
                    text=text_val,
                    meta={"chapter": ch_num, "verse": unit_num, "is_gatha": ch["is_gatha"]},
                ))
                n_verses += 1

        session.flush()
        print(f"  {len(chapters)} chapters, {n_verses} verses inserted")

        compute_heights(session, corpus.id)

        print("\nSanity check:")
        for height, name in [(1, "Chapter"), (0, "Verse")]:
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
