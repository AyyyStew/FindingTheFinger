#!/usr/bin/env python
"""
migrate_labels.py — fix unit_label (and unit_number where needed) for specific corpora.

Idempotent: checks current state before applying each fix.

Fixes applied:
  1. Bible — KJV        : "3:16"  → "John 3:16"
  2. Dao De Jing        : labels stuck at decade boundaries → sequential 1–81
  3. Bhagavad Gita      : "1.1"   → "Arjun Viṣhād Yog 1.1"
  4. Dhammapada         : "1.1"   → "The Twin-Verses 1.1"
  5. Quran              : "2:255" → "The Cow 2:255"
  6. Analects           : "1.1"   → "Book I 1.1"
  7. Poetic Edda        : "1.1"   → "THE Wise-Woman'S Prophecy 1.1"

Run:
    python migrate_labels.py [--dry-run]
"""

import sys
import duckdb
from rich.console import Console
from rich.table import Table

DB_PATH = "/home/alexs/Projects/DataSources/corpus.duckdb"
DRY_RUN = "--dry-run" in sys.argv

console = Console()


def get_corpus_id(conn, name: str) -> int:
    row = conn.execute("SELECT id FROM corpus WHERE name = ?", [name]).fetchone()
    if not row:
        raise ValueError(f"Corpus not found: {name!r}")
    return row[0]


# ---------------------------------------------------------------------------
# Generic: prepend book name to unit_label
# Idempotency check: first row's label already starts with its book name
# ---------------------------------------------------------------------------

def fix_book_prefix(conn, corpus_name: str):
    cid = get_corpus_id(conn, corpus_name)

    # Idempotency: check if any row's label does NOT yet start with its book
    needs_update = conn.execute(
        """
        SELECT count(*) FROM passage
        WHERE corpus_id = ?
          AND unit_label NOT LIKE (book || ' %')
        """,
        [cid],
    ).fetchone()[0]

    if needs_update == 0:
        console.print(f"[dim]{corpus_name}: already migrated, skipping.[/dim]")
        return

    total = conn.execute(
        "SELECT count(*) FROM passage WHERE corpus_id = ?", [cid]
    ).fetchone()[0]
    console.print(f"{corpus_name}: {needs_update}/{total} labels need book prepended")

    samples = conn.execute(
        """
        SELECT book, unit_label FROM passage
        WHERE corpus_id = ? AND unit_label NOT LIKE (book || ' %')
        LIMIT 5
        """,
        [cid],
    ).fetchall()
    t = Table("book", "unit_label", "→ new label")
    for book, label in samples:
        t.add_row(book, label, f"{book} {label}")
    console.print(t)

    if DRY_RUN:
        console.print("[yellow]dry-run: skipping write[/yellow]")
        return

    conn.execute(
        """
        UPDATE passage
        SET unit_label = book || ' ' || unit_label
        WHERE corpus_id = ?
          AND unit_label NOT LIKE (book || ' %')
        """,
        [cid],
    )
    console.print(f"[green]{corpus_name}: updated {needs_update} rows[/green]")


# ---------------------------------------------------------------------------
# Dao De Jing — reassign unit_number and unit_label from row order
# ---------------------------------------------------------------------------

def fix_daodejing(conn):
    corpus_name = "Dao De Jing (Linnell)"
    cid = get_corpus_id(conn, corpus_name)

    total = conn.execute(
        "SELECT count(*) FROM passage WHERE corpus_id = ?", [cid]
    ).fetchone()[0]
    distinct = conn.execute(
        "SELECT count(distinct unit_label) FROM passage WHERE corpus_id = ?", [cid]
    ).fetchone()[0]

    if total == distinct:
        console.print(f"[dim]{corpus_name}: labels already unique, skipping.[/dim]")
        return

    console.print(f"{corpus_name}: {total} passages, {distinct} distinct labels (should be {total})")

    rows = conn.execute(
        "SELECT id FROM passage WHERE corpus_id = ? ORDER BY id", [cid]
    ).fetchall()

    t = Table("passage id", "new unit_number / label")
    for i, (pid,) in enumerate(rows[:5]):
        t.add_row(str(pid), str(i + 1))
    console.print(t)

    if DRY_RUN:
        console.print("[yellow]dry-run: skipping write[/yellow]")
        return

    for i, (pid,) in enumerate(rows):
        chapter = i + 1
        conn.execute(
            "UPDATE passage SET unit_number = ?, unit_label = ? WHERE id = ?",
            [chapter, str(chapter), pid],
        )
    console.print(f"[green]{corpus_name}: reassigned {total} labels (1–{total})[/green]")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

BOOK_PREFIX_CORPORA = [
    "Bible — KJV (King James Version)",
    "Bhagavad Gita",
    "Dhammapada (Müller)",
    "Quran (Clear Quran Translation)",
    "Analects of Confucius (Legge)",
    "Poetic Edda (Bellows)",
    "Srimad Bhagavatam",
    "Upanishads (Paramananda)",
]

if __name__ == "__main__":
    if DRY_RUN:
        console.print("[yellow]--- DRY RUN ---[/yellow]\n")

    conn = duckdb.connect(DB_PATH, read_only=DRY_RUN)

    for name in BOOK_PREFIX_CORPORA:
        fix_book_prefix(conn, name)
        console.print()

    fix_daodejing(conn)

    if not DRY_RUN:
        conn.commit()
        console.print("\n[bold green]Done.[/bold green]")

    conn.close()
