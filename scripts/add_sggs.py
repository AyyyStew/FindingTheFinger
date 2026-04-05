"""
scripts/add_sggs.py

Ingests the Sri Guru Granth Sahib from the legacy DuckDB.

Source: /home/alexs/Projects/DataSources/corpus.duckdb (corpus_id=40)

Hierarchy:
    height=2  Raag    — book field, e.g. "Aasaa", "Jap Ji Sahib..."  (50)
    height=1  Shabad  — one hymn/composition, like a Psalm           (3,620)
    height=0  Tuk     — one verse line, cited as "SGGS p.347:2"      (60,403)

Labels:
    Raag   — book name as-is
    Shabad — "SGGS p.{first_page}" (disambiguated if multiple shabads share a page)
    Tuk    — "SGGS p.{page}:{page_line}"

No embeddings — all corpora will be re-embedded with nomic-embed-text-v1.5.

Run from project root:
    python -m scripts.add_sggs
"""

import json
from collections import OrderedDict

import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from db.models import Tradition, Corpus, CorpusLevel, Unit

DUCK_PATH    = "/home/alexs/Projects/DataSources/corpus.duckdb"
DUCK_CID     = 40
CORPUS_NAME  = "Sri Guru Granth Sahib"
TRADITION    = "Sikh"
DATABASE_URL = "postgresql://ftf:ftf@localhost:5432/ftf"


def load_data() -> tuple[list[dict], dict[int, list[dict]]]:
    """
    Returns:
        raags  — list of {book, min_hymn} ordered by first appearance
        hymns  — {hymn_id: [{book, hymn_id, section, unit_number, text, page, page_line, author, gurmukhi}]}
    """
    con = duckdb.connect(DUCK_PATH, read_only=True)
    rows = con.execute("""
        SELECT
            book,
            section,
            unit_number,
            text,
            metadata,
            CAST(json_extract(metadata, '$.hymn')      AS INTEGER) AS hymn_id,
            CAST(json_extract(metadata, '$.page')      AS INTEGER) AS page,
            CAST(json_extract(metadata, '$.page_line') AS INTEGER) AS page_line,
            json_extract(metadata, '$.author')                     AS author,
            json_extract(metadata, '$.gurmukhi')                   AS gurmukhi
        FROM passage
        WHERE corpus_id = ?
        ORDER BY hymn_id, unit_number
    """, [DUCK_CID]).fetchall()
    con.close()

    # Preserve book order by first-seen hymn_id
    book_order: OrderedDict[str, int] = OrderedDict()   # book → min hymn_id
    hymns: dict[int, list[dict]] = {}

    for (book, section, unit_num, text_val, meta_raw,
         hymn_id, page, page_line, author, gurmukhi) in rows:

        if book not in book_order:
            book_order[book] = hymn_id

        if hymn_id not in hymns:
            hymns[hymn_id] = {"book": book, "section": section, "tuks": []}

        hymns[hymn_id]["tuks"].append({
            "unit_number": unit_num,
            "text":        text_val,
            "page":        page,
            "page_line":   page_line,
            "author":      author.strip('"') if author else None,
            "gurmukhi":    gurmukhi.strip('"') if gurmukhi else None,
        })

    raags = [{"book": book, "min_hymn": min_h}
             for book, min_h in sorted(book_order.items(), key=lambda x: x[1])]

    return raags, hymns


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
    raags, hymns = load_data()

    total_hymns = len(hymns)
    total_tuks  = sum(len(h["tuks"]) for h in hymns.values())
    print(f"  {len(raags)} raags, {total_hymns} shabads, {total_tuks} tuks")

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
            era="15th–17th century CE",
        )
        session.add(corpus)
        session.flush()
        print(f"  corpus_id = {corpus.id}")

        for height, name in [(2, "Raag"), (1, "Shabad"), (0, "Tuk")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        print("Inserting raags...")
        raag_unit_ids: dict[str, int] = {}
        for raag in raags:
            u = Unit(
                corpus_id=corpus.id,
                parent_id=None,
                depth=0,
                label=raag["book"],
                meta={"raag": raag["book"]},
            )
            session.add(u)
            session.flush()
            raag_unit_ids[raag["book"]] = u.id
        print(f"  {len(raag_unit_ids)} raags inserted")

        print("Inserting shabads and tuks...")
        # Track page → count to disambiguate shabad labels
        page_seen: dict[int, int] = {}

        batch_size = 500
        tuk_batch  = []
        n_shabads  = 0
        n_tuks     = 0

        def flush_tuks():
            if tuk_batch:
                session.bulk_save_objects(tuk_batch)
                tuk_batch.clear()

        for hymn_id, hymn in sorted(hymns.items()):
            book    = hymn["book"]
            tuks    = hymn["tuks"]
            section = hymn.get("section")

            first_page = min(t["page"] for t in tuks if t["page"])
            author     = tuks[0]["author"] if tuks else None

            # Disambiguate shabad label when multiple shabads share a page
            page_seen[first_page] = page_seen.get(first_page, 0) + 1
            count = page_seen[first_page]
            shabad_label = f"SGGS p.{first_page}" if count == 1 else f"SGGS p.{first_page}:{count}"

            shabad_unit = Unit(
                corpus_id=corpus.id,
                parent_id=raag_unit_ids[book],
                depth=1,
                label=shabad_label,
                meta={
                    "hymn_id":    hymn_id,
                    "page_start": first_page,
                    "author":     author,
                    "section":    section,
                    "raag":       book,
                },
            )
            session.add(shabad_unit)
            session.flush()
            n_shabads += 1

            for tuk in tuks:
                page      = tuk["page"]
                page_line = tuk["page_line"]
                tuk_label = f"SGGS p.{page}:{page_line}" if page and page_line else shabad_label

                tuk_batch.append(Unit(
                    corpus_id=corpus.id,
                    parent_id=shabad_unit.id,
                    depth=2,
                    label=tuk_label,
                    text=tuk["text"],
                    meta={
                        "page":      page,
                        "page_line": page_line,
                        "author":    tuk["author"],
                        "hymn_id":   hymn_id,
                    },
                ))
                n_tuks += 1

                if len(tuk_batch) >= batch_size:
                    flush_tuks()

            if n_shabads % 500 == 0:
                print(f"  ... {n_shabads} shabads, {n_tuks} tuks")

        flush_tuks()
        print(f"  {n_shabads} shabads, {n_tuks} tuks inserted")

        compute_heights(session, corpus.id)

        print("\nSanity check:")
        for height, name in [(2, "Raag"), (1, "Shabad"), (0, "Tuk")]:
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
            LIMIT 12
        """), {"cid": corpus.id}).fetchall()
        for r in rows:
            print(f"  h={r[0]}  {r[1]}")

        session.commit()

    print("\nDone.")


if __name__ == "__main__":
    main()
