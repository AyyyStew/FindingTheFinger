"""
scripts/add_poetic_edda.py

Ingests the Poetic Edda (Bellows translation) from the Gutenberg plaintext.
Parses stanzas directly from the source — the DuckDB version mixed stanzas
with Bellows' extensive prose commentary, making it unusable.

Source: scripts/data/poetic_eda.txt (Gutenberg eBook #73533)

Hierarchy:
    height=1  Poem    — Norse names: "Völuspá", "Hávamál", ...  (30)
    height=0  Stanza  — "Völuspá 1", "Hávamál 1", ...           (1556)

Stanza detection: numbered blocks (N.) whose first line contains '|'
(the Bellows caesura marker). The '|' separators are replaced with
spaces and stanza lines are joined with newlines.

Run from project root:
    python -m scripts.add_poetic_edda
"""

import os
import re
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from db.models import Tradition, Corpus, CorpusLevel, Unit, Embedding

TXT_PATH     = os.path.join(os.path.dirname(__file__), "data", "poetic_eda.txt")
CORPUS_NAME  = "Poetic Edda (Bellows)"
TRADITION    = "Norse"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://ftf:ftf@localhost:5432/ftf")

# Maps exact heading in file → (Norse name, English subtitle)
POEMS = {
    "VOLUSPO":                                     ("Völuspá",                    "The Wise-Woman's Prophecy"),
    "HOVAMOL":                                     ("Hávamál",                    "Sayings of the High One"),
    "VAFTHRUTHNISMOL":                             ("Vafþrúðnismál",              "The Ballad of Vafthruthnir"),
    "GRIMNISMOL":                                  ("Grímnismál",                 "The Ballad of Grimnir"),
    "SKIRNISMOL":                                  ("Skírnismál",                 "The Ballad of Skirnir"),
    "HARBARTHSLJOTH":                              ("Hárbarðsljóð",               "The Poem of Harbarth"),
    "HYMISKVITHA":                                 ("Hymiskviða",                 "The Lay of Hymir"),
    "LOKASENNA":                                   ("Lokasenna",                  "Loki's Wrangling"),
    "THRYMSKVITHA":                                ("Þrymskviða",                 "The Lay of Thrym"),
    "ALVISSMOL":                                   ("Álvíssmál",                  "The Ballad of Alvis"),
    "RIGSTHULA":                                   ("Rígsþula",                   "The Song of Rig"),
    "HYNDLULJOTH":                                 ("Hyndluljóð",                 "The Poem of Hyndla"),
    "FRAGMENT OF \u201cTHE SHORT VOLUSPO\u201d":  ("Grógaldr",                   "The Spell of Groa"),
    "II. FJOLSVINNSMOL":                           ("Fjölsvinnsmál",              "The Lay of Fjolsvith"),
    "VÖLUNDARKVITHA":                              ("Völundarkviða",              "The Lay of Völund"),
    "HELGAKVITHA HJORVARTHSSONAR":                ("Helgakviða Hjörvarðssonar",  "The Lay of Helgi Hjorvarthsson"),
    "HELGAKVITHA HUNDINGSBANA I":                 ("Helgakviða Hundingsbana I",  "The First Lay of Helgi Hundingsbane"),
    "HELGAKVITHA HUNDINGSBANA II":                ("Helgakviða Hundingsbana II", "The Second Lay of Helgi Hundingsbane"),
    "REGINSMOL":                                   ("Reginsmál",                  "The Ballad of Regin"),
    "FAFNISMOL":                                   ("Fáfnismál",                  "The Ballad of Fafnir"),
    "SIGRDRIFUMOL":                                ("Sigrdrífumál",               "The Ballad of the Victory-Bringer"),
    "BROT AF SIGURTHARKVITHU":                     ("Brot af Sigurðarkviðu",      "Fragment of a Sigurd Lay"),
    "GUTHRUNARKVITHA I":                           ("Guðrúnarkviða I",            "The First Lay of Guthrun"),
    "SIGURTHARKVITHA EN SKAMMA":                   ("Sigurðarkviða in skamma",    "The Short Lay of Sigurd"),
    "GUTHRUNARKVITHA II, EN FORNA":                ("Guðrúnarkviða II",           "The Second Lay of Guthrun"),
    "GUTHRUNARKVITHA III":                         ("Guðrúnarkviða III",          "The Third Lay of Guthrun"),
    "ODDRUNARGRATR":                               ("Oddrúnargrátr",              "The Lament of Oddrun"),
    "ATLAKVITHA EN GRÖNLENZKA":                    ("Atlakviða",                  "The Greenland Lay of Atli"),
    "ATLAMOL EN GRÖNLENZKU":                       ("Atlamál",                    "The Greenland Ballad of Atli"),
    "HAMTHESMOL":                                  ("Hamðismál",                  "The Ballad of Hamther"),
}

STANZA_START = re.compile(r"^(\d+)\.\s+\S")
CAESURA      = re.compile(r"\s+\|\s+")


def clean_line(line: str) -> str:
    """Remove caesura marker and normalise whitespace."""
    return CAESURA.sub(" ", line).strip()


def parse_stanzas(lines_slice: list[str]) -> list[tuple[int, str]]:
    """
    Extract verse stanzas from a poem's line range.
    A block is a stanza iff its first line contains '|'.
    """
    stanzas: list[tuple[int, str]] = []
    current_num: int | None = None
    current_lines: list[str] = []

    def flush() -> None:
        if current_num is not None and current_lines and "|" in current_lines[0]:
            body = "\n".join(clean_line(l) for l in current_lines if l.strip())
            stanzas.append((current_num, body))

    for line in lines_slice:
        m = STANZA_START.match(line)
        if m:
            flush()
            current_num = int(m.group(1))
            current_lines = [re.sub(r"^\d+\.\s+", "", line).rstrip()]
        elif current_num is not None:
            stripped = line.strip()
            if stripped:
                current_lines.append(stripped)
            else:
                flush()
                current_num = None
                current_lines = []

    flush()
    return stanzas


def load_all_poems(path: str) -> list[tuple[str, str, str, list[tuple[int, str]]]]:
    """Returns list of (key, norse_name, english_title, stanzas)."""
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    poem_starts: list[tuple[int, str]] = []
    for i, line in enumerate(lines):
        if line.strip() in POEMS:
            poem_starts.append((i, line.strip()))

    result = []
    for idx, (start_i, key) in enumerate(poem_starts):
        end_i = poem_starts[idx + 1][0] if idx + 1 < len(poem_starts) else len(lines)
        norse, english = POEMS[key]
        stanzas = parse_stanzas(lines[start_i:end_i])
        result.append((key, norse, english, stanzas))
    return result


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
    print("Parsing text file...")
    poems = load_all_poems(TXT_PATH)
    total_stanzas = sum(len(s) for _, _, _, s in poems)
    print(f"  {len(poems)} poems, {total_stanzas} stanzas")

    print("\nConnecting...")
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
        else:
            corpus = Corpus(
                tradition_id=tradition.id,
                name=CORPUS_NAME,
                type="scripture",
                language="English",
                era="9th–13th century CE",
            )
            session.add(corpus)
            session.flush()
        print(f"  corpus_id = {corpus.id}")

        for height, name in [(1, "Poem"), (0, "Stanza")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        print("Inserting units...")
        for key, norse, english, stanzas in poems:
            poem_unit = Unit(
                corpus_id=corpus.id,
                parent_id=None,
                depth=0,
                label=norse,
                meta={"english_title": english, "bellows_key": key},
            )
            session.add(poem_unit)
            session.flush()

            for stanza_num, body in stanzas:
                session.add(Unit(
                    corpus_id=corpus.id,
                    parent_id=poem_unit.id,
                    depth=1,
                    label=f"{norse} {stanza_num}",
                    text=body,
                    meta={"stanza": stanza_num},
                ))
            print(f"  {norse}: {len(stanzas)} stanzas")

        session.flush()
        compute_heights(session, corpus.id)

        print("\nSanity check:")
        for height, name in [(1, "Poem"), (0, "Stanza")]:
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

    print("\nDone.")


if __name__ == "__main__":
    main()
