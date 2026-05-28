#!/usr/bin/env python3
"""
Fetch books from Open Library API and save to CSV (no DB interaction).

Usage:
    python fetch_books.py --count 20 --output books_raw.csv
    python fetch_books.py --count 100000 --with-description --output books_raw.csv

Flags:
    --with-description  Fetch real synopsis from Works API (slower: +1 request/book)
    --language          Set language for all rows (default: English).
                        Open Library's per-edition language is unreliable, so we
                        default everything to English (search queries are English).
    --overshoot         Multiplier to compensate for rows dropped at validation
                        (default 1.15 = fetch 15% more to guarantee target count).
"""

import argparse
import csv
import re
import time

import requests

OPEN_LIBRARY_SEARCH_URL = "https://openlibrary.org/search.json"
OPEN_LIBRARY_WORKS_URL = "https://openlibrary.org"
PAGE_SIZE = 100
PLACEHOLDER_COVER = "https://placehold.co/300x450?text=Book"

SEARCH_QUERIES = [
    "fiction", "fantasy", "science", "history", "romance", "thriller",
    "mystery", "biography", "self help", "philosophy", "horror", "drama",
    "adventure", "classic", "novel", "literature", "bestseller", "award winner",
    "prize winning", "young adult",
]

CSV_FIELDS = [
    "isbn", "title", "authors", "publisher",
    "published_year", "total_pages", "language", "subjects",
    "cover_url", "description",
]

# Markdown link: [text](url) → text
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
# Lines like "----------" or "==========" (3+ repeats)
_SEPARATOR_RE = re.compile(r"^[\-=_*~]{3,}\s*$", re.MULTILINE)
# "Contains:" footer block Open Library appends for anthologies/collections
_CONTAINS_BLOCK_RE = re.compile(r"\n+\s*Contains:.*$", re.DOTALL | re.IGNORECASE)
# Excess whitespace
_MULTI_WS_RE = re.compile(r"\s+")


def clean_description(text: str) -> str:
    """Strip markdown artifacts, separator lines, and collapse whitespace."""
    if not text:
        return ""
    # Drop "Contains:" footer (links to other works)
    text = _CONTAINS_BLOCK_RE.sub("", text)
    # Drop separator lines
    text = _SEPARATOR_RE.sub("", text)
    # Markdown links → plain text
    text = _MD_LINK_RE.sub(r"\1", text)
    # Collapse all whitespace (newlines, tabs, multi-spaces) to single space
    text = _MULTI_WS_RE.sub(" ", text)
    # Strip wrapping quotes if entire string is quoted
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ('"', "'"):
        text = text[1:-1].strip()
    return text


def fetch_description(works_key: str, session: requests.Session) -> str:
    """Fetch real synopsis from Works API. Returns empty string if unavailable."""
    for attempt in range(3):
        try:
            resp = session.get(f"{OPEN_LIBRARY_WORKS_URL}{works_key}.json", timeout=15)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            desc = resp.json().get("description", "")
            if isinstance(desc, dict):
                desc = desc.get("value", "")
            return clean_description(desc or "")
        except Exception:
            if attempt == 2:
                return ""
            time.sleep(1 + attempt)
    return ""


def extract_book(doc: dict, language: str, with_description: bool,
                 session: requests.Session) -> dict | None:
    """Extract & validate a single doc. Returns None if doc should be skipped."""
    # ISBN: prefer 13-digit, fall back to 10-digit
    isbns = doc.get("isbn") or []
    isbn = next((i for i in isbns if len(i) == 13), None) or \
           next((i for i in isbns if len(i) == 10), None)
    if not isbn:
        return None

    title = (doc.get("title") or "").strip()
    if not title:
        return None

    # Skip books with missing/invalid pages BEFORE making Works API call
    pages = doc.get("number_of_pages_median")
    if not pages or pages <= 0:
        return None

    year = doc.get("first_publish_year")
    if not year or year <= 0:
        return None

    cover_i = doc.get("cover_i")
    cover_url = (
        f"https://covers.openlibrary.org/b/id/{cover_i}-M.jpg"
        if cover_i else PLACEHOLDER_COVER
    )

    description = ""
    if with_description:
        works_key = doc.get("key", "")
        if works_key:
            description = fetch_description(works_key, session)
            time.sleep(0.05)

    return {
        "isbn": isbn,
        "title": title,
        "authors": "|".join((doc.get("author_name") or ["Unknown Author"])[:5]),
        "publisher": (doc.get("publisher") or ["Unknown Publisher"])[0],
        "published_year": year,
        "total_pages": int(pages),
        "language": language,
        "subjects": "|".join((doc.get("subject") or [])[:10]),
        "cover_url": cover_url,
        "description": description,
    }


def fetch_search_page(query: str, offset: int, session: requests.Session) -> list[dict]:
    """Fetch one page of results with retry on rate-limit/transient errors."""
    params = {
        "q": query,
        "limit": PAGE_SIZE,
        "offset": offset,
        "fields": ("key,title,author_name,isbn,first_publish_year,publisher,"
                   "subject,number_of_pages_median,cover_i,language"),
    }
    for attempt in range(4):
        try:
            resp = session.get(OPEN_LIBRARY_SEARCH_URL, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt
                print(f"\n  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json().get("docs", [])
        except Exception as e:
            if attempt == 3:
                print(f"\n  Search failed after retries: {e}")
                return []
            time.sleep(1 + attempt)
    return []


def fetch_books(target: int, output_path: str, with_description: bool,
                language: str, overshoot: float) -> None:
    fetch_target = int(target * overshoot)
    seen_isbns: set[str] = set()
    books = []
    query_idx = 0
    pages_consumed = 0

    mode = "with description (Works API)" if with_description else "search API only"
    print(f"Target: {target} valid books (fetching up to {fetch_target} for safety margin)")
    print(f"Mode: {mode}, language={language}")
    if with_description:
        print("  Note: --with-description adds 1 API call per book.")

    session = requests.Session()
    session.headers.update({"User-Agent": "FondasiKehidupan-Seeder/1.0"})

    start = time.time()
    while len(books) < fetch_target:
        query = SEARCH_QUERIES[query_idx % len(SEARCH_QUERIES)]
        offset = (query_idx // len(SEARCH_QUERIES)) * PAGE_SIZE
        query_idx += 1

        docs = fetch_search_page(query, offset, session)
        pages_consumed += 1

        if not docs:
            time.sleep(1)
            # If we've cycled through all queries with no results, stop
            if query_idx > len(SEARCH_QUERIES) * 200:
                print(f"\n  Exhausted search results at {len(books)} books")
                break
            continue

        for doc in docs:
            isbns = doc.get("isbn") or []
            isbn_preview = next((i for i in isbns if len(i) == 13), None) or \
                           next((i for i in isbns if len(i) == 10), None)
            if not isbn_preview or isbn_preview in seen_isbns:
                continue

            book = extract_book(doc, language, with_description, session)
            if book is None:
                continue

            seen_isbns.add(book["isbn"])
            books.append(book)

            elapsed = time.time() - start
            rate = len(books) / elapsed if elapsed > 0 else 0
            eta = (fetch_target - len(books)) / rate if rate > 0 else 0
            title_preview = book["title"][:40] + ("…" if len(book["title"]) > 40 else "")
            print(
                f"  [{len(books):>5}/{fetch_target}] "
                f"{rate:5.1f}/s | ETA {eta/60:5.1f}m | "
                f"pg={pages_consumed:>3} | {title_preview}",
                flush=True,
            )

            if len(books) >= fetch_target:
                break

        time.sleep(0.1)

    print()

    # Trim to exact target (overshoot was just safety margin)
    final = books[:target] if len(books) >= target else books

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(final)

    elapsed = time.time() - start
    print(f"\nSaved {len(final)} books to '{output_path}'")
    print(f"Stats: {pages_consumed} search pages, {len(books)} fetched, "
          f"{len(final)} written, {elapsed/60:.1f} min total")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch books from Open Library and dump to CSV")
    parser.add_argument("--count", type=int, default=1000,
                        help="Number of books to fetch (target)")
    parser.add_argument("--output", default="books_raw.csv",
                        help="Output CSV file path")
    parser.add_argument("--with-description", dest="with_description",
                        action="store_true",
                        help="Fetch real synopsis from Works API (slower)")
    parser.add_argument("--language", default="English",
                        help="Language to set for all rows (default: English)")
    parser.add_argument("--overshoot", type=float, default=1.15,
                        help="Fetch overshoot multiplier (default 1.15)")
    args = parser.parse_args()

    fetch_books(args.count, args.output, args.with_description,
                args.language, args.overshoot)
