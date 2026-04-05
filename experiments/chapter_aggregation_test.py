"""
experiments/chapter_aggregation_test.py

Tests two approaches to chapter-level embeddings:
  A) Average of individually-encoded verse embeddings
  B) Re-embed the concatenated chapter text

For a sample of N multi-verse chapters, computes cosine similarity
between A and B to see how interchangeable they are.

Both approaches always use the same model freshly — stored DB embeddings
are not used, so the comparison is fair across any model.

Run from project root:
    python -m experiments.chapter_aggregation_test
    python -m experiments.chapter_aggregation_test --model nomic
    python -m experiments.chapter_aggregation_test --model nomic --sample 50
"""

import argparse
from collections import defaultdict

import duckdb
import numpy as np
from sentence_transformers import SentenceTransformer

DB_PATH = "/home/alexs/Projects/DataSources/corpus.duckdb"

# Corpora to sample from
CORPORA = [
    "Bible — KJV (King James Version)",
    "Quran (Clear Quran Translation)",
    "Bhagavad Gita",
    "Srimad Bhagavatam",
    "Upanishads (Paramananda)",
    "Yoga Sutras of Patanjali (Johnston)",
    "Dhammapada (Müller)",
    "Diamond Sutra (Gemmell)",
    "Analects of Confucius (Legge)",
    "Dao De Jing (Linnell)",
    "Chuang Tzu (Giles)",
    "Poetic Edda (Bellows)",
    "Kojiki",
    "Sri Guru Granth Sahib",
    "Avesta: Vendidad",
    "Avesta: Yasna",
]

# Model aliases → config
MODELS = {
    "mpnet": {
        "model_id": "all-mpnet-base-v2",
        "trust_remote_code": False,
        "prompt_prefix": "",          # no prefix needed
    },
    "nomic": {
        "model_id": "nomic-ai/nomic-embed-text-v1.5",
        "trust_remote_code": True,
        "prompt_prefix": "search_document: ",
    },
}


def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    a = a / (np.linalg.norm(a) + 1e-10)
    b = b / (np.linalg.norm(b) + 1e-10)
    return float(np.dot(a, b))


def encode(model: SentenceTransformer, texts: list[str], prefix: str) -> np.ndarray:
    prefixed = [prefix + t for t in texts]
    return model.encode(prefixed, normalize_embeddings=False, show_progress_bar=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=list(MODELS), default="mpnet",
                        help="Which model to use (default: mpnet)")
    parser.add_argument("--sample", type=int, default=100,
                        help="Number of chapters to sample (default: 100)")
    parser.add_argument("--min-verses", type=int, default=3,
                        help="Minimum verses per chapter (default: 3)")
    args = parser.parse_args()

    cfg = MODELS[args.model]
    print(f"\nModel     : {cfg['model_id']}")
    print(f"Sample    : {args.sample} chapters  (>= {args.min_verses} verses)\n")

    print("Loading model...")
    model = SentenceTransformer(cfg["model_id"], trust_remote_code=cfg["trust_remote_code"])
    max_tokens = model.get_max_seq_length()
    print(f"  Max sequence length: {max_tokens} tokens\n")

    print("Connecting to DB (read-only)...")
    conn = duckdb.connect(DB_PATH, read_only=True)

    placeholders = ", ".join(f"$corpus_{i}" for i in range(len(CORPORA)))
    params = {f"corpus_{i}": name for i, name in enumerate(CORPORA)}

    print(f"Sampling chapters...")
    chapters = conn.execute(f"""
        SELECT c.name, c.id, p.book, p.section, COUNT(*) AS verse_count
        FROM passage p
        JOIN corpus c ON p.corpus_id = c.id
        WHERE c.name IN ({placeholders})
          AND p.section IS NOT NULL
          AND p.book IS NOT NULL
        GROUP BY c.name, c.id, p.book, p.section
        HAVING COUNT(*) >= {args.min_verses}
        ORDER BY RANDOM()
        LIMIT {args.sample}
    """, params).fetchall()

    print(f"  Got {len(chapters)} chapters\n")

    results = []
    truncated_count = 0

    for i, (corpus_name, corpus_id, book, section, verse_count) in enumerate(chapters):
        rows = conn.execute("""
            SELECT p.text
            FROM passage p
            WHERE p.corpus_id = $cid AND p.book = $book AND p.section = $sec
            ORDER BY p.unit_number
        """, {"cid": corpus_id, "book": book, "sec": section}).fetchall()

        texts = [r[0] for r in rows]

        # Approach A: encode each verse, then average
        verse_vecs = encode(model, texts, cfg["prompt_prefix"])
        avg_vec = verse_vecs.mean(axis=0)

        # Approach B: encode concatenated chapter text
        chapter_text = " ".join(texts)
        token_count = len(model.tokenizer.encode(chapter_text))
        if token_count > max_tokens:
            truncated_count += 1

        reembed_vec = encode(model, [chapter_text], cfg["prompt_prefix"])[0]

        sim = cosine_sim(avg_vec, reembed_vec)
        results.append({
            "corpus": corpus_name,
            "book": book,
            "section": section,
            "verse_count": verse_count,
            "token_count": token_count,
            "sim": sim,
        })

        if (i + 1) % 10 == 0:
            running_mean = np.mean([r["sim"] for r in results])
            print(f"  {i+1}/{len(chapters)}  running mean sim: {running_mean:.4f}")

    conn.close()

    # ---- Report ----
    sims = np.array([r["sim"] for r in results])

    print("\n" + "=" * 62)
    print(f"RESULTS [{cfg['model_id']}]")
    print("cosine similarity: avg-of-verses  vs  re-embed-chapter")
    print("=" * 62)
    print(f"  n                     : {len(sims)}")
    print(f"  mean                  : {sims.mean():.4f}")
    print(f"  median                : {np.median(sims):.4f}")
    print(f"  std                   : {sims.std():.4f}")
    print(f"  min                   : {sims.min():.4f}")
    print(f"  max                   : {sims.max():.4f}")
    print(f"  > 0.95                : {(sims > 0.95).sum()}")
    print(f"  > 0.90                : {(sims > 0.90).sum()}")
    print(f"  < 0.80                : {(sims < 0.80).sum()}")
    print(f"  truncated (>{max_tokens} tok) : {truncated_count}")

    print("\n--- 10 most divergent chapters ---")
    for r in sorted(results, key=lambda r: r["sim"])[:10]:
        short = r["corpus"].split("(")[0].strip()[:28]
        print(f"  {r['sim']:.4f}  {short:<28}  {r['book']} §{r['section']}"
              f"  ({r['verse_count']}v, {r['token_count']}tok)")

    print("\n--- Similarity by corpus ---")
    by_corpus = defaultdict(list)
    for r in results:
        by_corpus[r["corpus"]].append(r["sim"])
    for corpus, vals in sorted(by_corpus.items()):
        arr = np.array(vals)
        short = corpus.split("(")[0].strip()[:35]
        print(f"  {short:<35}  n={len(arr):3d}  mean={arr.mean():.4f}  min={arr.min():.4f}")


if __name__ == "__main__":
    main()
