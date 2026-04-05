"""
scripts/add_chuang_tzu_giles.py

Ingests the Chuang Tzu (Giles) from the Project Gutenberg plaintext,
at chapter level (the DB paragraph splits are broken mid-sentence).

Source: scripts/data/chuang_tzu.txt (Gutenberg eBook #59709)

Hierarchy:
    height=1  Chapter  — "I — TRANSCENDENTAL BLISS", etc.  (33)

Note: no height=0 leaf — chapters ARE the leaf units here. Chapter-level
embeddings are deliberately coarse; vignette-level chunking can be added
later from the raw text.

Run from project root:
    python -m scripts.add_chuang_tzu_giles
"""

import os
import re
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from db.models import Tradition, Corpus, CorpusLevel, Unit, Embedding

TXT_PATH     = os.path.join(os.path.dirname(__file__), "data", "chuang_tzu.txt")
CORPUS_NAME  = "Chuang Tzu (Giles)"
TRADITION    = "Taoist"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://ftf:ftf@localhost:5432/ftf")

# Roman numeral → chapter number mapping for ordering
ROMAN = {
    "I": 1, "II": 2, "III": 3, "IV": 4, "V": 5,
    "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10,
    "XI": 11, "XII": 12, "XIII": 13, "XIV": 14, "XV": 15,
    "XVI": 16, "XVII": 17, "XVIII": 18, "XIX": 19, "XX": 20,
    "XXI": 21, "XXII": 22, "XXIII": 23, "XXIV": 24, "XXV": 25,
    "XXVI": 26, "XXVII": 27, "XXVIII": 28, "XXIX": 29, "XXX": 30,
    "XXXI": 31, "XXXII": 32, "XXXIII": 33,
}

CHAPTER_RE = re.compile(r"^CHAPTER\s+(I{1,3}|IV|V?I{0,3}|X{0,3}(?:IX|IV|V?I{0,3})|X+)\.$")


def parse_chapters(path: str) -> list[tuple[int, str, str]]:
    """
    Returns list of (chapter_num, label, text) tuples.
    label = "I — TRANSCENDENTAL BLISS"
    text  = chapter body with footnotes stripped
    """
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    chapters = []
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i].rstrip()
        m = CHAPTER_RE.match(line)
        if m:
            roman = m.group(1)
            ch_num = ROMAN.get(roman)
            if ch_num is None:
                i += 1
                continue

            # Next non-blank line is the chapter title
            j = i + 1
            while j < n and not lines[j].strip():
                j += 1
            title_line = lines[j].strip() if j < n else ""

            # Remove trailing period from title if present
            title = title_line.rstrip(".")
            label = f"{roman} — {title}"

            # Collect body until next CHAPTER heading or end of main text
            body_lines = []
            k = j + 1
            in_argument = False
            while k < n:
                l = lines[k].rstrip()
                if CHAPTER_RE.match(l):
                    break
                if l.startswith("*** END OF THE PROJECT GUTENBERG"):
                    break

                stripped = l.lstrip()

                # Skip _Argument_:-- block (ends at first blank line after it starts)
                if stripped.startswith("_Argument_"):
                    in_argument = True
                    k += 1
                    continue
                if in_argument:
                    if not stripped:
                        in_argument = False  # blank line ends the argument block
                    k += 1
                    continue

                # Skip indented commentator/translator notes (4+ spaces indent)
                if stripped and l.startswith("    "):
                    k += 1
                    continue

                body_lines.append(l)
                k += 1

            # Clean up body: collapse multiple blank lines, strip leading/trailing
            body = "\n".join(body_lines).strip()
            body = re.sub(r"\n{3,}", "\n\n", body)
            # Remove inline footnote markers like [1], [2] etc.
            body = re.sub(r"\[\d+\]", "", body)
            # Remove _italic_ markers from Gutenberg markup
            body = re.sub(r"_([^_]+)_", r"\1", body)
            body = body.strip()

            if ch_num and body:
                chapters.append((ch_num, label, body))
            i = k
        else:
            i += 1

    return sorted(chapters, key=lambda x: x[0])


def compute_heights(session: Session, corpus_id: int) -> None:
    print("  Computing heights...")
    # All chapters are root nodes — set height=0 for all
    session.execute(text("""
        UPDATE unit SET height = 0
        WHERE corpus_id = :cid AND parent_id IS NULL
    """), {"cid": corpus_id})
    remaining = session.execute(
        text("SELECT COUNT(*) FROM unit WHERE corpus_id = :cid AND height IS NULL"),
        {"cid": corpus_id}
    ).scalar()
    print("  Heights OK" if not remaining else f"  WARNING: {remaining} units with NULL height")


def main():
    print("Parsing text file...")
    chapters = parse_chapters(TXT_PATH)
    print(f"  Found {len(chapters)} chapters")
    for ch_num, label, body in chapters[:3]:
        print(f"  Ch{ch_num}: {label} ({len(body)} chars)")

    print("\nConnecting...")
    engine = create_engine(DATABASE_URL)

    with Session(engine) as session:

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

        # Single level — chapters are the leaf units
        if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=0).first():
            session.add(CorpusLevel(corpus_id=corpus.id, height=0, name="Chapter"))
        session.flush()

        print("Inserting chapter units...")
        for ch_num, label, body in chapters:
            session.add(Unit(
                corpus_id=corpus.id,
                parent_id=None,
                depth=0,
                label=label,
                text=body,
                meta={"chapter": ch_num},
            ))
        session.flush()

        compute_heights(session, corpus.id)

        print("\nSanity check:")
        count = session.execute(
            text("SELECT COUNT(*) FROM unit WHERE corpus_id = :cid AND height = 0"),
            {"cid": corpus.id},
        ).scalar()
        print(f"  height=0 (Chapter): {count}")

        print("\nSample labels:")
        rows = session.execute(text("""
            SELECT label FROM unit WHERE corpus_id = :cid ORDER BY id LIMIT 5
        """), {"cid": corpus.id}).fetchall()
        for r in rows:
            print(f"  {r[0]}")

        session.commit()

    print("\nDone.")


if __name__ == "__main__":
    main()
