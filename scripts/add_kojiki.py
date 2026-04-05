"""
scripts/add_kojiki.py

Ingests the Kojiki (Chamberlain translation) from local HTML files
(scraped from sacred-texts.com).

Source: scripts/data/shinto/kojiki/kj008.htm – kj187.htm
        (kj008 = Section 1, kj009 = Section 2, ..., kj187 = Section 180)
        File number is authoritative — heading Roman numerals contain typos.

Hierarchy:
    height=1  Volume   — "Volume I — Age of the Gods" etc.     (3)
    height=0  Section  — "Kojiki 1" … "Kojiki 180"             (180)

Cleaning:
    - Drop paragraphs that are pure page markers ("p. 24")
    - Strip "[paragraph continues]" prefix
    - Strip inline footnote refs "[N]" and footnote anchors
    - Drop short paragraphs < 20 chars (section sub-headers, stray labels)
    - Collapse whitespace

Run from project root:
    python -m scripts.add_kojiki
"""

import re
from pathlib import Path

from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from db.models import Tradition, Corpus, CorpusLevel, Unit

KOJIKI_DIR   = Path("scripts/data/shinto/kojiki")
CORPUS_NAME  = "Kojiki (Chamberlain)"
TRADITION    = "Shinto"
DATABASE_URL = "postgresql://ftf:ftf@localhost:5432/ftf"

# kj008 = section 1, so section_num = file_num - 7
FIRST_FILE = 8    # kj008.htm
LAST_FILE  = 187  # kj187.htm

PAGE_ONLY  = re.compile(r"^(p\.\s*\d+\s*)+$")
FOOTNOTE_N = re.compile(r"\[\d+\]")
NAV_TEXT   = re.compile(r"^(Next:|Sacred Texts\s*\|)", re.I)

VOLUME_LABELS = {
    "I":   "Volume I — Age of the Gods",
    "II":  "Volume II — Age of the Early Emperors",
    "III": "Volume III — Age of the Later Emperors",
}
VOLUME_ORDER = ["I", "II", "III"]


def extract_volume(title_text: str) -> str | None:
    m = re.search(r"Volume\s+(I{1,3}V?|IV|VI{0,3})", title_text, re.I)
    return m.group(1).upper() if m else None


def extract_section_title(heading_text: str) -> str:
    """
    '[SECT. LXI.—EMPEROR KŌ-GEN.]' → 'Emperor Kō-gen'
    Strips leading SECT./SECTION and Roman numeral, normalises case.
    """
    s = heading_text.strip().strip("[]").strip()
    # Remove leading SECT. / SECTION. token + Roman numeral
    s = re.sub(r"^SECT(?:ION)?\.?\s+[IVXLCDM]+\.?\s*[—–-]+\s*", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).rstrip(".]").strip()
    # Title-case (Chamberlain writes in ALL-CAPS)
    return s.title() if s.isupper() else s


def clean_para(raw: str) -> str | None:
    if PAGE_ONLY.match(raw.strip()):
        return None
    if NAV_TEXT.match(raw.strip()):
        return None
    text = re.sub(r"^\[paragraph continues\]\s*", "", raw)
    text = FOOTNOTE_N.sub("", text)
    text = re.sub(r"[\xa0\s]+", " ", text).strip()
    if len(text) < 20:
        return None
    return text


def parse_file(path: Path) -> dict | None:
    """
    Returns {volume_rom, title, text} or None if not a content page.
    """
    soup = BeautifulSoup(path.read_text(encoding="utf-8"), "html.parser")

    title_tag = soup.find("title")
    volume_rom = extract_volume(title_tag.get_text() if title_tag else "")
    if not volume_rom:
        return None  # appendix or non-content

    # Section heading: first h2/h3/h4 that looks like SECT.
    sect_tag = None
    for tag in soup.find_all(["h2", "h3", "h4"]):
        if re.search(r"SECT(?:ION)?\.?\s+[IVXLCDM]+", tag.get_text(), re.I):
            sect_tag = tag
            break
    if not sect_tag:
        return None

    # Strip footnote superscript links from heading before extracting title
    for a in sect_tag.find_all("a"):
        a.decompose()
    title = extract_section_title(sect_tag.get_text())

    # Collect paragraphs after heading, stop at Footnotes header
    frags = []
    in_content = False
    for tag in soup.find_all(["h2", "h3", "h4", "p"]):
        if tag is sect_tag:
            in_content = True
            continue
        if not in_content:
            continue
        if tag.name in ("h2", "h3", "h4"):
            if "footnote" in tag.get_text().lower():
                break
            continue
        # Strip footnote anchor links before extracting text
        for a in tag.find_all("a", href=lambda h: h and "#fn_" in h):
            a.decompose()
        raw = re.sub(r"[\xa0\s]+", " ", tag.get_text()).strip()
        cleaned = clean_para(raw)
        if cleaned:
            frags.append(cleaned)

    body = " ".join(frags)
    if not body:
        return None

    return {"volume_rom": volume_rom, "title": title, "text": body}


def load_sections() -> list[dict]:
    sections = []
    for file_num in range(FIRST_FILE, LAST_FILE + 1):
        path = KOJIKI_DIR / f"kj{file_num:03d}.htm"
        section_num = file_num - 7
        result = parse_file(path)
        if result:
            result["section_num"] = section_num
            sections.append(result)
        else:
            print(f"  WARNING: kj{file_num:03d}.htm (section {section_num}) — no content extracted")
    return sections


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
    print("Parsing HTML files...")
    sections = load_sections()
    print(f"  {len(sections)} sections loaded")
    for vol in VOLUME_ORDER:
        n = sum(1 for s in sections if s["volume_rom"] == vol)
        print(f"  {VOLUME_LABELS[vol]}: {n} sections")

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
            era="8th century CE",
        )
        session.add(corpus)
        session.flush()
        print(f"  corpus_id = {corpus.id}")

        for height, name in [(1, "Volume"), (0, "Section")]:
            if not session.query(CorpusLevel).filter_by(corpus_id=corpus.id, height=height).first():
                session.add(CorpusLevel(corpus_id=corpus.id, height=height, name=name))
        session.flush()

        print("Inserting units...")
        volume_units: dict[str, int] = {}

        for vol_rom in VOLUME_ORDER:
            vol_unit = Unit(
                corpus_id=corpus.id,
                parent_id=None,
                depth=0,
                label=VOLUME_LABELS[vol_rom],
                meta={"volume": vol_rom},
            )
            session.add(vol_unit)
            session.flush()
            volume_units[vol_rom] = vol_unit.id

        for s in sections:
            session.add(Unit(
                corpus_id=corpus.id,
                parent_id=volume_units[s["volume_rom"]],
                depth=1,
                label=f"Kojiki {s['section_num']}",
                text=s["text"],
                meta={
                    "section_num": s["section_num"],
                    "volume":      s["volume_rom"],
                    "title":       s["title"],
                },
            ))

        session.flush()
        print(f"  Inserted 3 volumes, {len(sections)} sections")

        compute_heights(session, corpus.id)

        print("\nSanity check:")
        for height, name in [(1, "Volume"), (0, "Section")]:
            count = session.execute(
                text("SELECT COUNT(*) FROM unit WHERE corpus_id = :cid AND height = :h"),
                {"cid": corpus.id, "h": height},
            ).scalar()
            print(f"  height={height} ({name}): {count}")

        print("\nSample labels:")
        rows = session.execute(text("""
            SELECT height, label, metadata->>'title' AS title
            FROM unit WHERE corpus_id = :cid
            ORDER BY height DESC, id
            LIMIT 8
        """), {"cid": corpus.id}).fetchall()
        for r in rows:
            extra = f"  — {r[2]}" if r[2] else ""
            print(f"  h={r[0]}  {r[1]}{extra}")

        session.commit()

    print("\nDone.")


if __name__ == "__main__":
    main()
