#!/usr/bin/env python3
"""
Validate and clean books_raw.csv, output books_clean.csv.

Output mengikuti format yang IDENTIK dengan apa yang backend Spring Boot
(BookServiceImpl.createBook) akan simpan ke DB:
  - publisher, author, language di-capitalize-words (Title Case Java-style)
  - panjang field di-enforce per Entity Hibernate (BUKAN per schema.sql):
      * publisher.name  : 50 chars  (Entity length=50)
      * author.name     : 50 chars  (Entity length=50)
      * book_language.language : 30 chars (Entity length=30)
      * book.title      : 255 chars
      * book.book_picture: 255 chars
      * book.isbn       : 17 chars
  - synopsis: TEXT (no limit)
  - genre_id: harus ada di tabel genre (1-17)

Checks:
  - Missing/duplicate ISBN
  - Missing title
  - total_pages <= 0
  - published_year < 1000 or > current year
  - publisher > 50 chars setelah Title Case → drop row
  - SEMUA author > 50 chars setelah Title Case → drop row
    (partial: author > 50 chars di-skip individually, sisanya dipakai)
  - cover_url > 255 chars → fallback ke placeholder

Usage:
    python validate_books.py --input books_raw.csv --output books_clean.csv
"""

import argparse
import csv
import datetime

# Subject → genre_id mapping. Order matters: lebih spesifik dulu, generic terakhir.
SUBJECT_GENRE_MAP = {
    "romance": 1, "love story": 1, "cinta": 1,
    "science fiction": 3, "sci-fi": 3, "space opera": 3,
    "fantasy": 2, "fantasi": 2, "magic": 2, "wizard": 2,
    "horror": 4, "ghost": 4, "scary": 4,
    "mystery": 5, "detective": 5, "crime": 5,
    "thriller": 6, "suspense": 6, "espionage": 6,
    "historical fiction": 8, "historical novel": 8,
    "biography": 10, "autobiography": 10, "memoir": 10,
    "self-help": 11, "self help": 11, "personal development": 11,
    "business": 12, "leadership": 12, "economics": 12,
    "science": 13, "physics": 13, "biology": 13, "chemistry": 13,
    "history": 14, "sejarah": 14,
    "philosophy": 15, "filsafat": 15,
    "religion": 16, "spiritual": 16, "agama": 16,
    "education": 17, "reference": 17, "textbook": 17,
    # Generic terakhir:
    "drama": 7, "literary": 7, "fiction": 7,
}
DEFAULT_GENRE_ID = 7  # Drama / Fiksi
CURRENT_YEAR = datetime.datetime.now().year
PLACEHOLDER_COVER = "https://placehold.co/300x450?text=Book"

# Limits per Entity Hibernate (LEBIH KETAT dari schema.sql)
MAX_PUBLISHER = 50
MAX_AUTHOR = 50
MAX_LANGUAGE = 30
MAX_TITLE = 255
MAX_PICTURE = 255
MAX_ISBN = 17

CLEAN_FIELDS = [
    "isbn", "title", "authors", "publisher", "language",
    "published_year", "total_pages", "cover_url",
    "synopsis", "genre_id",
]


def capitalize_words(s: str) -> str:
    """
    Replikasi exact dari backend BookServiceImpl.capitalizeWords():
      - lowercase
      - split by whitespace
      - uppercase first char tiap word
      - join with space, trimmed

    Note: ini mempertahankan "bug" backend (e.g. "O'Brien" -> "O'brien",
    "Stuckey-French" -> "Stuckey-french"). Sengaja, supaya data dari seeder
    100% identik dengan apa yang backend akan generate via API.
    """
    if not s:
        return s
    words = s.lower().split()
    return " ".join(
        (w[0].upper() + w[1:]) if w else ""
        for w in words
    ).strip()


def map_genre(subjects_str: str) -> int:
    """Map first matching subject keyword → genre_id. Order in SUBJECT_GENRE_MAP matters."""
    subjects_lower = [s.lower().strip() for s in subjects_str.split("|") if s.strip()]
    for keyword, gid in SUBJECT_GENRE_MAP.items():
        for subject in subjects_lower:
            if keyword in subject:
                return gid
    return DEFAULT_GENRE_ID


def make_synopsis(title: str, authors: str, published_year: str, subjects_str: str) -> str:
    author_list = [a.strip() for a in authors.split("|") if a.strip()][:3]
    subjects = [s.strip() for s in subjects_str.split("|") if s.strip()][:5]
    return (
        f"{title} by {', '.join(author_list)}. "
        f"Published in {published_year or 'unknown year'}. "
        f"Subjects: {', '.join(subjects) if subjects else 'General Fiction'}."
    )


def validate(input_path: str, output_path: str) -> None:
    total = valid = 0
    reasons: dict[str, int] = {}
    seen_isbns: set[str] = set()

    with open(input_path, encoding="utf-8") as fin, \
         open(output_path, "w", newline="", encoding="utf-8") as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=CLEAN_FIELDS, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for row in reader:
            total += 1

            isbn = (row.get("isbn") or "").strip()
            title = (row.get("title") or "").strip()
            raw_authors = (row.get("authors") or "Unknown Author").strip()
            raw_publisher = (row.get("publisher") or "Unknown Publisher").strip()
            raw_language = (row.get("language") or "English").strip() or "English"
            subjects = (row.get("subjects") or "").strip()
            cover_url = (row.get("cover_url") or "").strip()
            description = (row.get("description") or "").strip()

            # ISBN checks
            if not isbn:
                reasons["missing_isbn"] = reasons.get("missing_isbn", 0) + 1
                continue
            if len(isbn) > MAX_ISBN:
                reasons["isbn_too_long"] = reasons.get("isbn_too_long", 0) + 1
                continue
            if isbn in seen_isbns:
                reasons["duplicate_isbn"] = reasons.get("duplicate_isbn", 0) + 1
                continue

            # Title checks
            if not title:
                reasons["missing_title"] = reasons.get("missing_title", 0) + 1
                continue
            if len(title) > MAX_TITLE:
                title = title[:MAX_TITLE]

            # Numeric checks
            try:
                total_pages = int(float(row.get("total_pages") or 0))
                if total_pages <= 0:
                    raise ValueError
            except (ValueError, TypeError):
                reasons["invalid_pages"] = reasons.get("invalid_pages", 0) + 1
                continue

            try:
                published_year = int(float(row.get("published_year") or 0))
                if published_year < 1000 or published_year > CURRENT_YEAR:
                    raise ValueError
            except (ValueError, TypeError):
                reasons["invalid_year"] = reasons.get("invalid_year", 0) + 1
                continue

            # Publisher: Title Case + max 50
            publisher = capitalize_words(raw_publisher)
            if len(publisher) > MAX_PUBLISHER:
                reasons["publisher_too_long"] = reasons.get("publisher_too_long", 0) + 1
                continue

            # Language: Title Case + max 30
            language = capitalize_words(raw_language)
            if len(language) > MAX_LANGUAGE:
                language = language[:MAX_LANGUAGE]

            # Authors: Title Case + filter individual > 50, skip row if NONE remain
            author_list = [
                capitalize_words(a.strip())
                for a in raw_authors.split("|")
                if a.strip()
            ]
            # Dedupe (preserve order)
            seen = set()
            author_list = [a for a in author_list if not (a in seen or seen.add(a))]
            # Drop authors > 50 chars
            author_list = [a for a in author_list if len(a) <= MAX_AUTHOR]
            if not author_list:
                reasons["all_authors_too_long"] = reasons.get("all_authors_too_long", 0) + 1
                continue

            # Cover URL: fallback to placeholder if missing or too long
            if not cover_url or len(cover_url) > MAX_PICTURE:
                cover_url = PLACEHOLDER_COVER

            seen_isbns.add(isbn)

            synopsis = description if description else make_synopsis(
                title, raw_authors, str(published_year), subjects
            )

            writer.writerow({
                "isbn": isbn,
                "title": title,
                "authors": "|".join(author_list[:5]),  # top 5 only
                "publisher": publisher,
                "language": language,
                "published_year": published_year,
                "total_pages": total_pages,
                "cover_url": cover_url,
                "synopsis": synopsis,
                "genre_id": map_genre(subjects),
            })
            valid += 1

    dropped = total - valid
    print(f"\nValidation Report")
    print(f"  {'Total rows':<25}: {total}")
    print(f"  {'Valid (kept)':<25}: {valid}")
    print(f"  {'Dropped':<25}: {dropped}")
    if reasons:
        print(f"\n  Drop reasons:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason:<25}: {count}")
    print(f"\nClean CSV saved to '{output_path}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate and clean raw book CSV")
    parser.add_argument("--input", default="books_raw.csv", help="Raw CSV from fetch_books.py")
    parser.add_argument("--output", default="books_clean.csv", help="Output clean CSV")
    args = parser.parse_args()

    validate(args.input, args.output)
