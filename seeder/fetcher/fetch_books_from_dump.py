#!/usr/bin/env python3
"""
Fetch books from Open Library bulk dumps (0 hit to API).

Pipeline:
  1. Download 3 dump files (editions, works, authors) — ~13 GB total
  2. Pass 1: stream editions.txt.gz → filter (publisher blacklist, ISBN+title+
             pages+year+language+work+publisher+author+cover), dedupe by work_key
             OVERSAMPLE 5x target untuk dapat quality pool
  3. Pass 2: stream works.txt.gz → lookup description & subjects
             FILTER: hanya keep buku dengan description >= 50 chars
  4. SHUFFLE filtered candidates, ambil target count untuk diversity
  5. Pass 3: stream authors.txt.gz → resolve author names
  6. Merge → write raw.csv

Disk usage:
  ~13 GB temporary (gzip files). Bisa dihapus setelah raw.csv jadi.
  Pakai gzip streaming, NO decompress (tidak butuh ~110 GB).

Quality strategy (vs versi pertama):
  - Publisher blacklist (skip GPO, dissertations, microfilm services)
  - Oversampling 5x untuk filter quality + diversity
  - Require real description (>= 50 chars)
  - Random shuffle hasil → distribusi tahun lebih beragam

Usage:
    python fetch_books_from_dump.py --count 100000 --output raw.csv
    python fetch_books_from_dump.py --count 1000 --oversample 10
    python fetch_books_from_dump.py --no-description-filter   # skip desc requirement
"""

import argparse
import csv
import gzip
import json
import os
import random
import re
import time

import requests

# Default dump dir: seeder/dumps/ (parent of this fetcher/ folder),
# resolved from the script location so it works regardless of CWD.
DEFAULT_DUMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dumps")

DUMP_URLS = {
    "editions": "https://openlibrary.org/data/ol_dump_editions_latest.txt.gz",
    "works": "https://openlibrary.org/data/ol_dump_works_latest.txt.gz",
    "authors": "https://openlibrary.org/data/ol_dump_authors_latest.txt.gz",
}

CSV_FIELDS = [
    "isbn", "title", "authors", "publisher",
    "published_year", "total_pages", "language", "subjects",
    "cover_url", "description",
]

PLACEHOLDER_COVER = "https://placehold.co/300x450?text=Book"
ENGLISH_LANG_KEY = "/languages/eng"
YEAR_RE = re.compile(r"\b(1[5-9]\d{2}|20[0-2]\d)\b")
USER_AGENT = "FondasiKehidupan-Seeder/1.0 (https://github.com/wiwokdetok)"

# Publisher patterns yang biasanya bukan buku untuk publik
# (government docs, microfilms, dissertations, etc.)
PUBLISHER_BLACKLIST_PATTERNS = [
    "g.p.o.", "gpo,", "supt. of docs", "superintendent of documents",
    "congressional sales office", "distributed by",
    "university microfilms", "proquest", "umi dissertation",
    "u.s. dept", "u.s. department of",
    "national technical information service",
    "ntis,",
]

MIN_DESCRIPTION_LENGTH = 50  # filter quality: buku harus punya description >= 50 chars


def download_dump(name: str, url: str, dump_dir: str) -> str:
    """Download dump with resume support & progress bar."""
    path = os.path.join(dump_dir, f"{name}.txt.gz")
    tmp_path = path + ".tmp"

    if os.path.exists(path):
        size_mb = os.path.getsize(path) / 1024 / 1024
        print(f"  [{name}] already exists ({size_mb:.0f} MB), skipping")
        return path

    resume_pos = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
    headers = {"User-Agent": USER_AGENT}
    if resume_pos > 0:
        headers["Range"] = f"bytes={resume_pos}-"
        print(f"  [{name}] resuming from {resume_pos / 1024 / 1024:.0f} MB")

    with requests.get(url, stream=True, headers=headers, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0)) + resume_pos
        downloaded = resume_pos
        start = time.time()
        last_print = 0

        mode = "ab" if resume_pos > 0 else "wb"
        with open(tmp_path, mode) as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded - last_print >= 8 * 1024 * 1024:
                    last_print = downloaded
                    pct = downloaded / total * 100 if total else 0
                    mb = downloaded / 1024 / 1024
                    total_mb = total / 1024 / 1024
                    elapsed = time.time() - start
                    chunk_done = downloaded - resume_pos
                    speed = chunk_done / 1024 / 1024 / elapsed if elapsed > 0 else 0
                    eta = (total - downloaded) / 1024 / 1024 / speed if speed > 0 else 0
                    print(f"    {mb:.0f}/{total_mb:.0f} MB ({pct:.1f}%) | "
                          f"{speed:.2f} MB/s | ETA {eta/60:.1f}m",
                          flush=True)

    os.rename(tmp_path, path)
    print(f"  [{name}] downloaded {os.path.getsize(path) / 1024 / 1024:.0f} MB")
    return path


def parse_tsv_line(line: str) -> tuple[str, str] | None:
    parts = line.rstrip("\n").split("\t", 4)
    if len(parts) < 5:
        return None
    return parts[1], parts[4]


def extract_year(publish_date: str) -> int | None:
    if not publish_date:
        return None
    m = YEAR_RE.search(publish_date)
    return int(m.group(1)) if m else None


def is_english(languages) -> bool:
    if not languages:
        return False
    for lang in languages:
        if isinstance(lang, dict) and lang.get("key") == ENGLISH_LANG_KEY:
            return True
    return False


def is_blacklisted_publisher(publisher: str) -> bool:
    lower = publisher.lower()
    return any(pat in lower for pat in PUBLISHER_BLACKLIST_PATTERNS)


def collect_editions(path: str, target_pool: int, language: str) -> list[dict]:
    """Pass 1: stream editions, filter & collect candidate pool (oversampled)."""
    candidates = []
    seen_works = set()
    seen_isbns = set()
    scanned = 0
    rejected_publisher = 0
    start = time.time()

    print(f"  Scanning editions for {target_pool} candidates (oversampled pool)...")
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            scanned += 1

            row = parse_tsv_line(line)
            if not row:
                continue
            _, json_str = row

            try:
                data = json.loads(json_str)
            except json.JSONDecodeError:
                continue

            # ISBN
            isbns = data.get("isbn_13") or data.get("isbn_10") or []
            if not isbns:
                continue
            isbn = isbns[0]
            if len(isbn) > 17 or isbn in seen_isbns:
                continue

            # Title
            title = (data.get("title") or "").strip()
            if not title or len(title) > 255:
                continue

            # Pages
            pages = data.get("number_of_pages")
            if not isinstance(pages, int) or pages <= 0:
                continue

            # Year
            year = extract_year(data.get("publish_date", ""))
            if not year or year < 1000:
                continue

            # Language
            if not is_english(data.get("languages")):
                continue

            # Work reference
            works = data.get("works") or []
            if not works:
                continue
            work_key = works[0].get("key") if isinstance(works[0], dict) else None
            if not work_key or work_key in seen_works:
                continue

            # Publisher (with blacklist)
            publishers = data.get("publishers") or []
            if not publishers:
                continue
            publisher = str(publishers[0]).strip()
            if not publisher:
                continue
            if is_blacklisted_publisher(publisher):
                rejected_publisher += 1
                continue

            # Authors
            author_refs = data.get("authors") or []
            author_keys = [
                a.get("key") for a in author_refs
                if isinstance(a, dict) and a.get("key")
            ]
            if not author_keys:
                continue

            # Cover (REQUIRED — no placeholder books in pool)
            covers = [c for c in (data.get("covers") or []) if isinstance(c, int) and c > 0]
            if not covers:
                continue
            cover_id = covers[0]

            seen_works.add(work_key)
            seen_isbns.add(isbn)
            candidates.append({
                "isbn": isbn,
                "title": title,
                "publisher": publisher,
                "published_year": year,
                "total_pages": pages,
                "language": language,
                "cover_id": cover_id,
                "work_key": work_key,
                "author_keys": author_keys[:5],
            })

            if len(candidates) % 500 == 0:
                elapsed = time.time() - start
                yield_pct = len(candidates) / scanned * 100 if scanned else 0
                rate = len(candidates) / elapsed if elapsed > 0 else 0
                eta = (target_pool - len(candidates)) / rate if rate > 0 else 0
                print(f"    [{len(candidates):>6}/{target_pool}] "
                      f"scanned {scanned:>9,} | yield {yield_pct:.2f}% | "
                      f"rate {rate:.0f}/s | ETA {eta/60:.1f}m",
                      flush=True)

            if len(candidates) >= target_pool:
                break

    elapsed = time.time() - start
    print(f"  Done. Collected {len(candidates)} from {scanned:,} editions "
          f"({elapsed/60:.1f}m, rejected {rejected_publisher} by publisher blacklist)")
    return candidates


def collect_works(path: str, work_keys: set[str],
                  require_description: bool, min_desc_len: int) -> dict[str, dict]:
    """Pass 2: extract descriptions & subjects for our work_keys.
    If require_description, only keep works with description >= min_desc_len chars."""
    result = {}
    scanned = 0
    matched = 0
    start = time.time()
    target = len(work_keys)

    if require_description:
        print(f"  Looking up {target} works (filter: desc >= {min_desc_len} chars)...")
    else:
        print(f"  Looking up {target} works...")

    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            scanned += 1

            row = parse_tsv_line(line)
            if not row:
                continue
            key, json_str = row

            if key not in work_keys:
                continue
            matched += 1

            try:
                data = json.loads(json_str)
            except json.JSONDecodeError:
                continue

            desc = data.get("description", "")
            if isinstance(desc, dict):
                desc = desc.get("value", "")
            desc = (desc or "").strip()

            if require_description and len(desc) < min_desc_len:
                continue  # skip works without real description

            result[key] = {
                "description": desc,
                "subjects": (data.get("subjects") or [])[:10],
            }

            if len(result) % 2000 == 0:
                elapsed = time.time() - start
                pct = len(result) / target * 100
                print(f"    [{len(result):>6}/{target}] kept ({pct:.1f}%) | "
                      f"matched {matched} | scanned {scanned:,} | {elapsed/60:.1f}m",
                      flush=True)

            # Early exit if all matched (whether kept or not)
            if matched >= target:
                break

    elapsed = time.time() - start
    print(f"  Done. Found {matched} works ({len(result)} pass description filter, "
          f"{elapsed/60:.1f}m)")
    return result


def collect_authors(path: str, author_keys: set[str]) -> dict[str, str]:
    """Pass 3: extract author names for our author_keys."""
    result = {}
    scanned = 0
    start = time.time()
    target = len(author_keys)

    print(f"  Looking up {target} authors for names...")
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            scanned += 1

            row = parse_tsv_line(line)
            if not row:
                continue
            key, json_str = row

            if key not in author_keys:
                continue

            try:
                data = json.loads(json_str)
            except json.JSONDecodeError:
                continue

            name = (data.get("name") or data.get("personal_name") or "").strip()
            if name:
                result[key] = name

            if len(result) % 5000 == 0:
                elapsed = time.time() - start
                pct = len(result) / target * 100
                print(f"    [{len(result):>6}/{target}] ({pct:.1f}%) | "
                      f"scanned {scanned:>9,} | {elapsed/60:.1f}m",
                      flush=True)

            if len(result) >= target:
                break

    elapsed = time.time() - start
    print(f"  Done. Found {len(result)}/{target} authors ({elapsed/60:.1f}m)")
    return result


def write_csv(output_path: str, candidates: list[dict],
              works_map: dict, authors_map: dict) -> int:
    written = 0
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for c in candidates:
            work_data = works_map.get(c["work_key"], {})
            author_names = [
                authors_map.get(k, "") for k in c["author_keys"]
            ]
            author_names = [n for n in author_names if n] or ["Unknown Author"]

            cover_url = (
                f"https://covers.openlibrary.org/b/id/{c['cover_id']}-M.jpg"
                if c["cover_id"] else PLACEHOLDER_COVER
            )

            writer.writerow({
                "isbn": c["isbn"],
                "title": c["title"],
                "authors": "|".join(author_names),
                "publisher": c["publisher"],
                "published_year": c["published_year"],
                "total_pages": c["total_pages"],
                "language": c["language"],
                "subjects": "|".join(work_data.get("subjects", [])),
                "cover_url": cover_url,
                "description": work_data.get("description", ""),
            })
            written += 1
    return written


def fetch_books_from_dump(target: int, output_path: str, language: str,
                          dump_dir: str, oversample: int = 5,
                          require_description: bool = True,
                          min_desc_len: int = MIN_DESCRIPTION_LENGTH) -> None:
    os.makedirs(dump_dir, exist_ok=True)
    pool_target = target * oversample

    print(f"Target: {target} books")
    print(f"Pool target: {pool_target} editions ({oversample}x oversample)")
    print(f"Description filter: {'ON (>= ' + str(min_desc_len) + ' chars)' if require_description else 'OFF'}")
    print(f"Publisher blacklist: ON ({len(PUBLISHER_BLACKLIST_PATTERNS)} patterns)")
    print(f"Dump dir: {dump_dir}/")

    total_start = time.time()

    print("\n[1/5] Downloading dumps from openlibrary.org...")
    paths = {}
    for name, url in DUMP_URLS.items():
        paths[name] = download_dump(name, url, dump_dir)

    print("\n[2/5] Pass 1: Scanning editions for candidate pool...")
    candidates = collect_editions(paths["editions"], pool_target, language)
    if not candidates:
        print("ERROR: No candidates collected.")
        return

    print("\n[3/5] Pass 2: Looking up descriptions in works (with quality filter)...")
    work_keys = {c["work_key"] for c in candidates}
    works_map = collect_works(paths["works"], work_keys,
                              require_description, min_desc_len)

    # Filter candidates: only keep those whose work passed description filter
    filtered = [c for c in candidates if c["work_key"] in works_map]
    print(f"\n     After description filter: {len(filtered)}/{len(candidates)} candidates "
          f"({len(filtered)/len(candidates)*100:.1f}% pass)")

    if len(filtered) < target:
        print(f"  ⚠️  WARNING: Only {len(filtered)} pass filter, less than target {target}.")
        print(f"  Consider increasing --oversample (current: {oversample}x)")
    else:
        # Shuffle for year/topic diversity, then take target
        print(f"  Shuffling for diversity...")
        random.shuffle(filtered)
        filtered = filtered[:target]

    print(f"\n[4/5] Pass 3: Looking up author names...")
    author_keys = {k for c in filtered for k in c["author_keys"]}
    authors_map = collect_authors(paths["authors"], author_keys)

    print("\n[5/5] Writing CSV...")
    written = write_csv(output_path, filtered, works_map, authors_map)

    total_elapsed = time.time() - total_start
    print(f"\n✓ Saved {written} books to '{output_path}'")
    print(f"  Total time: {total_elapsed/60:.1f} minutes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch books from Open Library bulk dumps (0 API hits)"
    )
    parser.add_argument("--count", type=int, default=100,
                        help="Number of books to fetch (target)")
    parser.add_argument("--output", default="books_raw.csv",
                        help="Output CSV file path")
    parser.add_argument("--language", default="English",
                        help="Language label (default: English)")
    parser.add_argument("--dump-dir", default=DEFAULT_DUMP_DIR,
                        help="Folder for .txt.gz dumps (default: ../dumps relative to this script)")
    parser.add_argument("--oversample", type=int, default=5,
                        help="Pool size multiplier (default: 5x). Higher = better quality "
                             "& diversity but slower parsing.")
    parser.add_argument("--no-description-filter", dest="require_description",
                        action="store_false", default=True,
                        help="Disable description filter (faster, but synopsis kebanyakan template)")
    parser.add_argument("--min-desc-len", type=int, default=MIN_DESCRIPTION_LENGTH,
                        help=f"Min description length (default: {MIN_DESCRIPTION_LENGTH})")
    args = parser.parse_args()

    fetch_books_from_dump(
        args.count, args.output, args.language,
        args.dump_dir, args.oversample,
        args.require_description, args.min_desc_len,
    )
