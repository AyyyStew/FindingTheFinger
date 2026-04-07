#!/usr/bin/env python
"""
inspect_data.py — poke around the corpus DB to understand structure & metadata.

Usage:
    python inspect_data.py                      # overview of all corpora
    python inspect_data.py <corpus name>        # drill into one corpus
    python inspect_data.py <corpus> --sample N  # show N sample passages (default 5)
"""

import sys
import json
import duckdb
from collections import Counter
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from rich.panel import Panel
from rich.text import Text

DB_PATH = "/home/alexs/Projects/DataSources/corpus.duckdb"

console = Console()


def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)


# ---------------------------------------------------------------------------
# Overview: one row per corpus
# ---------------------------------------------------------------------------

def overview(conn):
    rows = conn.execute("""
        SELECT
            ct.name                                      AS tradition,
            c.name                                       AS corpus,
            count(*)                                     AS passages,
            count(distinct p.book)                       AS books,
            count(distinct p.unit_label)                 AS distinct_labels,
            count(*) > count(distinct p.unit_label)      AS labels_not_unique,
            min(p.unit_label)                            AS label_min,
            max(p.unit_label)                            AS label_max,
            -- check whether metadata is ever non-empty
            sum(CASE WHEN p.metadata != '{}' THEN 1 ELSE 0 END) AS has_metadata
        FROM passage p
        JOIN corpus c ON p.corpus_id = c.id
        JOIN corpus_tradition ct ON c.tradition_id = ct.id
        GROUP BY ct.name, c.name
        ORDER BY ct.name, c.name
    """).fetchall()

    t = Table(title="Corpus overview", show_lines=True)
    t.add_column("Tradition", style="dim")
    t.add_column("Corpus")
    t.add_column("Passages", justify="right")
    t.add_column("Books", justify="right")
    t.add_column("Distinct labels", justify="right")
    t.add_column("Labels unique?", justify="center")
    t.add_column("Label range")
    t.add_column("Has metadata?", justify="center")

    for r in rows:
        tradition, corpus, passages, books, distinct, not_unique, lmin, lmax, meta = r
        unique_marker = "[red]NO[/red]" if not_unique else "[green]yes[/green]"
        meta_marker   = "[green]yes[/green]" if meta else "[dim]no[/dim]"
        t.add_row(
            tradition, corpus,
            str(passages), str(books), str(distinct),
            unique_marker,
            f"{lmin} … {lmax}",
            meta_marker,
        )

    console.print(t)
    console.print()
    console.print("[dim]Run with a corpus name to drill in, e.g.:[/dim]")
    console.print('  python inspect_data.py "Bible — KJV (King James Version)"')


# ---------------------------------------------------------------------------
# Drill-down: one corpus
# ---------------------------------------------------------------------------

def drill(conn, corpus_name: str, sample_n: int = 5):
    # Confirm corpus exists
    row = conn.execute(
        "SELECT c.id, c.name, ct.name FROM corpus c JOIN corpus_tradition ct ON c.tradition_id = ct.id WHERE c.name ILIKE ?",
        [f"%{corpus_name}%"],
    ).fetchone()
    if not row:
        console.print(f"[red]No corpus matching '{corpus_name}'[/red]")
        sys.exit(1)
    corpus_id, corpus_full, tradition = row
    console.print(Panel(f"[bold]{corpus_full}[/bold]  [dim]({tradition})[/dim]"))

    # --- Books ---
    books = conn.execute("""
        SELECT book, count(*) as n, min(unit_label), max(unit_label)
        FROM passage WHERE corpus_id = ?
        GROUP BY book ORDER BY min(unit_number)
    """, [corpus_id]).fetchall()

    bt = Table(title="Books / sections", show_lines=False)
    bt.add_column("Book")
    bt.add_column("Passages", justify="right")
    bt.add_column("Label range")
    for book, n, lmin, lmax in books:
        bt.add_row(str(book), str(n), f"{lmin} … {lmax}")
    console.print(bt)
    console.print()

    # --- Label uniqueness ---
    dup = conn.execute("""
        SELECT unit_label, count(*) as c
        FROM passage WHERE corpus_id = ?
        GROUP BY unit_label HAVING count(*) > 1
        ORDER BY c DESC LIMIT 5
    """, [corpus_id]).fetchall()
    if dup:
        console.print(f"[yellow]unit_label is NOT unique — {len(dup)} duplicated labels (top 5):[/yellow]")
        for label, cnt in dup:
            console.print(f"  [bold]{label!r}[/bold] appears {cnt}×")
    else:
        console.print("[green]unit_label is unique within this corpus.[/green]")
    console.print()

    # --- Metadata keys ---
    meta_rows = conn.execute("""
        SELECT metadata FROM passage WHERE corpus_id = ? AND metadata != '{}'
        LIMIT 200
    """, [corpus_id]).fetchall()

    if meta_rows:
        key_counter = Counter()
        sample_vals: dict[str, list] = {}
        for (m,) in meta_rows:
            d = m if isinstance(m, dict) else json.loads(m)
            for k, v in d.items():
                key_counter[k] += 1
                if k not in sample_vals:
                    sample_vals[k] = []
                if len(sample_vals[k]) < 3:
                    sample_vals[k].append(v)

        mt = Table(title="Metadata keys", show_lines=False)
        mt.add_column("Key")
        mt.add_column("Rows with key", justify="right")
        mt.add_column("Sample values")
        for k, cnt in key_counter.most_common():
            mt.add_row(k, str(cnt), ", ".join(repr(v) for v in sample_vals[k]))
        console.print(mt)
    else:
        console.print("[dim]No metadata on passages in this corpus.[/dim]")
    console.print()

    # --- Sample passages ---
    samples = conn.execute("""
        SELECT book, unit_label, text, metadata
        FROM passage WHERE corpus_id = ?
        ORDER BY unit_number LIMIT ?
    """, [corpus_id, sample_n]).fetchall()

    console.print(f"[bold]Sample passages (first {sample_n})[/bold]")
    for book, label, text, meta in samples:
        meta_str = ""
        if meta and meta != "{}":
            d = meta if isinstance(meta, dict) else json.loads(meta)
            meta_str = "  " + " | ".join(f"{k}={v!r}" for k, v in d.items())
        header = Text()
        header.append(f"{book}  ", style="dim")
        header.append(label, style="bold cyan")
        header.append(meta_str, style="dim yellow")
        console.print(header)
        # Wrap text
        snippet = text[:300].replace("\n", " ")
        if len(text) > 300:
            snippet += "…"
        console.print(f"  {snippet}")
        console.print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    sample_n = 5
    for i, a in enumerate(sys.argv[1:]):
        if a == "--sample" and i + 2 < len(sys.argv):
            sample_n = int(sys.argv[i + 2])

    conn = get_conn()

    if not args:
        overview(conn)
    else:
        drill(conn, args[0], sample_n)

    conn.close()
