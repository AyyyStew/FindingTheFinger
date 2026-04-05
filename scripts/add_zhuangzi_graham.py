"""
scripts/add_zhuangzi_graham.py

Ingests the Zhuangzi (Graham) — A.C. Graham's translation of the Inner
Chapters (chapters 1-7) — from the epub directly into postgres.

Source: Chuang-tzŭ: The Inner Chapters (Hackett, 2001), EPUB edition.

Hierarchy:
    height=1  Chapter  — Graham's titles, e.g. "Going rambling..."  (7)
    height=0  Episode  — "Zhuangzi 1.1", "Zhuangzi 1.2", ...       (~135)

Parsing rules:
    - First <p class="nonindent"> per chapter = Graham's intro → skip
    - <p class="nonindent|extract|extract1|center"> = new episode start
    - <p class="indent"> in vignette mode = continuation of current episode
    - <p class="extracta"> = Graham's note → skip (+ following indents)
    - Episodes with < 20 chars = section headers ("1 FIRST SERIES") → skip

Run from project root:
    python -m scripts.add_zhuangzi_graham
"""

import os
import re
import zipfile
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from db.models import Tradition, Corpus, CorpusLevel, Unit, Embedding

EPUB = (
    "/home/alexs/Projects/WebProjects/FindingTheFinger/scripts/data/"
    "Zhuangzi._Chuang-Tzu, Graham, A. C - Chuang-tzŭ_ the inner chapters "
    "(2001, Hackett Publishing Company, Inc.) - libgen.li.epub"
)

CORPUS_NAME  = "Zhuangzi (Graham)"
TRADITION    = "Taoist"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://ftf:ftf@localhost:5432/ftf")

CHAPTER_FILES = [
    ("OEBPS/c09.htm", 1, "Going rambling without a destination"),
    ("OEBPS/c10.htm", 2, "The sorting which evens things out"),
    ("OEBPS/c11.htm", 3, "What matters in the nurture of life"),
    ("OEBPS/c12.htm", 4, "Worldly business among men"),
    ("OEBPS/c13.htm", 5, "The signs of fullness of Power"),
    ("OEBPS/c14.htm", 6, "The teacher who is the ultimate ancestor"),
    ("OEBPS/c15.htm", 7, "Responding to the Emperors and Kings"),
]

NOTE_CLASSES     = {"extracta"}
VIGNETTE_CLASSES = {"nonindent", "extract", "extract1", "center"}


def parse_episodes(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    episodes: list[str] = []
    current: list[str] = []
    in_note       = False
    skipped_intro = False

    for tag in soup.find_all(["h1", "p"]):
        cls  = tag.get("class", [""])[0]
        text = tag.get_text(" ", strip=True)
        if not text or cls == "chapter":
            continue

        if cls in NOTE_CLASSES:
            if current:
                episodes.append(" ".join(current))
                current = []
            in_note = True
            continue

        if cls == "indent":
            if not in_note and current:
                current.append(text)
            continue

        # vignette-class tag
        in_note = False
        if cls == "nonindent" and not skipped_intro:
            skipped_intro = True
            continue  # Graham's chapter introduction

        if current:
            episodes.append(" ".join(current))
        current = [text]

    if current:
        episodes.append(" ".join(current))

    # Drop section headers ("1 FIRST SERIES", "2 SECOND SERIES", etc.)
    episodes = [ep for ep in episodes if len(ep) >= 20]

    # Merge any episode that starts with a lowercase letter into the previous —
    # these are mid-sentence continuations caused by HTML paragraph boundaries.
    merged: list[str] = []
    for ep in episodes:
        first_alpha = next((c for c in ep if c.isalpha()), "")
        if merged and first_alpha and first_alpha.islower():
            merged[-1] = merged[-1] + " " + ep
        else:
            merged.append(ep)
    return merged


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

    with Session(engine) as session:

        # Tradition + corpus
        print("Migrating tradition and corpus...")
        tradition = session.query(Tradition).filter_by(name=TRADITION).first()
        if not tradition:
            tradition = Tradition(name=TRADITION)
            session.add(tradition)
            session.flush()

        corpus = session.query(Corpus).filter_by(name=CORPUS_NAME).first()
        if corpus:
            print(f"  Already exists (id={corpus.id}), skipping.")
        else:
            corpus = Corpus(
                tradition_id=tradition.id,
                name=CORPUS_NAME,
                type="scripture",
                language="English",
                era="4th–3rd century BCE",
            )
            session.add(corpus)
            session.flush()
        print(f"  corpus_id = {corpus.id}")

        # Levels
        for height, name in [(1, "Chapter"), (0, "Episode")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        # Parse epub and insert units
        print("Parsing epub...")
        total_episodes = 0

        with zipfile.ZipFile(EPUB) as z:
            for fn, ch_num, ch_title in CHAPTER_FILES:
                html     = z.read(fn).decode("utf-8")
                episodes = parse_episodes(html)

                # Chapter unit
                ch_unit = Unit(
                    corpus_id=corpus.id,
                    parent_id=None,
                    depth=0,
                    label=ch_title,
                    meta={"chapter": ch_num},
                )
                session.add(ch_unit)
                session.flush()

                for ep_num, ep_text in enumerate(episodes, 1):
                    session.add(Unit(
                        corpus_id=corpus.id,
                        parent_id=ch_unit.id,
                        depth=1,
                        label=f"Zhuangzi {ch_num}.{ep_num}",
                        text=ep_text,
                    ))
                total_episodes += len(episodes)
                print(f"  Ch{ch_num} '{ch_title[:40]}': {len(episodes)} episodes")

        session.flush()
        print(f"  Total: {len(CHAPTER_FILES)} chapters, {total_episodes} episodes")

        compute_heights(session, corpus.id)

        # Sanity check
        print("\nSanity check:")
        for height, name in [(1, "Chapter"), (0, "Episode")]:
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
