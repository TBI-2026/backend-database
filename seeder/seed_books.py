#!/usr/bin/env python3
"""
Book seeder for FondasiKehidupan.

Fetches books from the Open Library Search API and inserts them into PostgreSQL.
Optionally publishes book.created events to RabbitMQ so the search-service
indexes them in OpenSearch automatically.

Usage:
    python seed_books.py --count 1000
    python seed_books.py --count 5000 --no-rabbitmq

Environment variables (or .env file):
    DB_HOST, DB_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
    CREATED_BY_USER_ID   (UUID of user credited as book creator; defaults to a fixed UUID)
"""

import argparse
import json
import os
import sys
import time
import uuid
from typing import Optional

import pika
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "fondasikehidupan"),
    "user": os.getenv("DB_USERNAME", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

RABBITMQ_CONFIG = {
    "host": os.getenv("RABBITMQ_HOST", "localhost"),
    "port": int(os.getenv("RABBITMQ_PORT", 5672)),
    "virtual_host": "/",
    "credentials": pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"),
        os.getenv("RABBITMQ_PASS", "guest"),
    ),
}

CREATED_BY_USER_ID = os.getenv(
    "CREATED_BY_USER_ID", "00000000-0000-0000-0000-000000000001"
)

EXCHANGE_NAME = "wiwokdetok.exchange"
ROUTING_KEY = "book.created"

OPEN_LIBRARY_SEARCH_URL = "https://openlibrary.org/search.json"

# Map Open Library subjects → our genre IDs (best-effort)
SUBJECT_GENRE_MAP = {
    "romance": 1, "love": 1, "cinta": 1,
    "fantasy": 2, "fantasi": 2, "magic": 2, "wizard": 2,
    "science fiction": 3, "sci-fi": 3, "space": 3,
    "horror": 4, "ghost": 4, "scary": 4,
    "mystery": 5, "detective": 5, "crime": 5,
    "thriller": 6, "suspense": 6, "espionage": 6,
    "drama": 7, "fiction": 7, "literary": 7,
    "historical fiction": 8, "history": 8, "sejarah": 8,
    "biography": 10, "autobiography": 10, "memoir": 10,
    "self-help": 11, "self help": 11, "personal development": 11,
    "business": 12, "leadership": 12, "economics": 12,
    "science": 13, "physics": 13, "biology": 13, "chemistry": 13,
    "philosophy": 15, "filsafat": 15,
    "religion": 16, "spiritual": 16, "agama": 16,
    "education": 17, "reference": 17, "textbook": 17,
}

DEFAULT_GENRE_ID = 7  # Drama / Fiksi (fallback)
PLACEHOLDER_COVER = "https://placehold.co/300x450?text=Book"
PAGE_SIZE = 100       # Open Library returns up to 100 per request


# ---------------------------------------------------------------------------
# Open Library fetching
# ---------------------------------------------------------------------------

SEARCH_QUERIES = [
    "fiction", "fantasy", "science", "history", "romance", "thriller",
    "mystery", "biography", "self help", "philosophy", "horror", "drama",
    "adventure", "classic", "novel", "literature", "bestseller", "award winner",
    "prize winning", "young adult",
]


def fetch_books_from_open_library(total: int) -> list[dict]:
    books = []
    seen_isbns: set[str] = set()
    query_idx = 0

    print(f"Fetching {total} books from Open Library…")

    while len(books) < total:
        query = SEARCH_QUERIES[query_idx % len(SEARCH_QUERIES)]
        query_idx += 1
        offset = (query_idx // len(SEARCH_QUERIES)) * PAGE_SIZE

        params = {
            "q": query,
            "limit": PAGE_SIZE,
            "offset": offset,
            "fields": "key,title,author_name,isbn,first_publish_year,publisher,subject,number_of_pages_median,cover_i",
        }

        try:
            resp = requests.get(OPEN_LIBRARY_SEARCH_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Warning: request failed ({e}), retrying after 2s…")
            time.sleep(2)
            continue

        docs = data.get("docs", [])
        if not docs:
            time.sleep(1)
            continue

        for doc in docs:
            isbns = doc.get("isbn") or []
            isbn = next((i for i in isbns if len(i) == 13), None)
            if isbn is None:
                isbn = next((i for i in isbns if len(i) == 10), None)
            if isbn is None or isbn in seen_isbns:
                continue
            if not doc.get("title"):
                continue

            seen_isbns.add(isbn)
            books.append(doc)

            if len(books) >= total:
                break

        print(f"  Collected {len(books)}/{total} books…", end="\r")
        time.sleep(0.1)  # be polite to the API

    print()
    return books[:total]


# ---------------------------------------------------------------------------
# Genre mapping
# ---------------------------------------------------------------------------

def map_subjects_to_genre_id(subjects: list[str]) -> int:
    for subject in subjects:
        lower = subject.lower()
        for keyword, genre_id in SUBJECT_GENRE_MAP.items():
            if keyword in lower:
                return genre_id
    return DEFAULT_GENRE_ID


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def upsert_publisher(cur, name: str) -> int:
    cur.execute(
        "INSERT INTO publisher (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
        (name,),
    )
    return cur.fetchone()[0]


def upsert_language(cur, language: str) -> int:
    cur.execute(
        "INSERT INTO book_language (language) VALUES (%s) ON CONFLICT DO NOTHING RETURNING id",
        (language,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT id FROM book_language WHERE language = %s", (language,))
    return cur.fetchone()[0]


def upsert_author(cur, name: str) -> int:
    cur.execute(
        "INSERT INTO author (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
        (name,),
    )
    return cur.fetchone()[0]


def insert_book(cur, book_id: str, isbn: str, title: str, synopsis: str,
                cover_url: str, total_pages: int, published_year: int,
                language_id: int, publisher_id: int, created_by: str) -> None:
    cur.execute(
        """
        INSERT INTO book (id, isbn, title, synopsis, book_picture,
                          total_pages, published_year, id_language,
                          id_publisher, created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (isbn) DO NOTHING
        """,
        (book_id, isbn, title, synopsis, cover_url, total_pages,
         published_year, language_id, publisher_id, created_by),
    )


def link_author(cur, book_id: str, author_id: int) -> None:
    cur.execute(
        "INSERT INTO authored_by (id_book, id_author) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (book_id, author_id),
    )


def link_genre(cur, book_id: str, genre_id: int) -> None:
    cur.execute(
        "INSERT INTO having_genre (id_book, id_genre) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (book_id, genre_id),
    )


# ---------------------------------------------------------------------------
# RabbitMQ publishing
# ---------------------------------------------------------------------------

def create_rabbitmq_channel():
    conn = pika.BlockingConnection(pika.ConnectionParameters(**RABBITMQ_CONFIG))
    ch = conn.channel()
    ch.exchange_declare(
        exchange=EXCHANGE_NAME, exchange_type="topic", durable=True
    )
    return conn, ch


def publish_book_created(ch, book_id: str, title: str, synopsis: str, cover: str) -> None:
    payload = json.dumps({
        "id": book_id,
        "title": title,
        "synopsis": synopsis,
        "bookPicture": cover,
        "createdBy": CREATED_BY_USER_ID,
    })
    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY,
        body=payload,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,  # persistent
        ),
    )


# ---------------------------------------------------------------------------
# Main seeding logic
# ---------------------------------------------------------------------------

def seed(total: int, use_rabbitmq: bool) -> None:
    raw_books = fetch_books_from_open_library(total)

    print(f"Connecting to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}…")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    rmq_conn: Optional[object] = None
    rmq_ch: Optional[object] = None
    if use_rabbitmq:
        print(f"Connecting to RabbitMQ at {RABBITMQ_CONFIG['host']}:{RABBITMQ_CONFIG['port']}…")
        try:
            rmq_conn, rmq_ch = create_rabbitmq_channel()
        except Exception as e:
            print(f"  Warning: RabbitMQ unavailable ({e}). Books will NOT be indexed automatically.")
            use_rabbitmq = False

    inserted = 0
    skipped = 0
    batch_size = 100

    for i, doc in enumerate(raw_books):
        try:
            isbns = doc.get("isbn") or []
            isbn = next((x for x in isbns if len(x) == 13), None) or \
                   next((x for x in isbns if len(x) == 10), None)
            if not isbn:
                skipped += 1
                continue

            title = (doc.get("title") or "Unknown Title")[:255]
            authors = doc.get("author_name") or ["Unknown Author"]
            publisher_name = ((doc.get("publisher") or ["Unknown Publisher"])[0])[:255]
            subjects = doc.get("subject") or []
            total_pages = doc.get("number_of_pages_median") or 200
            published_year = doc.get("first_publish_year") or 2000
            cover_i = doc.get("cover_i")
            cover_url = (
                f"https://covers.openlibrary.org/b/id/{cover_i}-M.jpg"
                if cover_i else PLACEHOLDER_COVER
            )
            synopsis = (
                f"{title} by {', '.join(authors[:3])}. "
                f"Published in {published_year}. "
                f"Subjects: {', '.join(subjects[:5]) if subjects else 'General Fiction'}."
            )

            publisher_id = upsert_publisher(cur, publisher_name)
            language_id = upsert_language(cur, "English")

            book_id = str(uuid.uuid4())
            insert_book(
                cur, book_id, isbn, title, synopsis, cover_url,
                max(1, int(total_pages)), max(0, int(published_year)),
                language_id, publisher_id, CREATED_BY_USER_ID,
            )

            # Check if book was actually inserted (ON CONFLICT skips duplicates)
            cur.execute("SELECT id FROM book WHERE isbn = %s", (isbn,))
            actual_book_id = str(cur.fetchone()[0])

            author_ids = [upsert_author(cur, a[:255]) for a in authors[:5]]
            for aid in author_ids:
                link_author(cur, actual_book_id, aid)

            genre_id = map_subjects_to_genre_id(subjects)
            link_genre(cur, actual_book_id, genre_id)

            if (i + 1) % batch_size == 0:
                conn.commit()

            if use_rabbitmq and actual_book_id == book_id:
                publish_book_created(rmq_ch, book_id, title, synopsis, cover_url)

            inserted += 1

        except Exception as e:
            conn.rollback()
            print(f"\n  Error on doc {i}: {e}")
            skipped += 1
            continue

    conn.commit()
    cur.close()
    conn.close()

    if rmq_conn:
        rmq_conn.close()

    print(f"\nDone. Inserted: {inserted}, Skipped: {skipped}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed FondasiKehidupan with book data from Open Library")
    parser.add_argument("--count", type=int, default=1000, help="Number of books to seed (default: 1000)")
    parser.add_argument("--no-rabbitmq", dest="no_rabbitmq", action="store_true",
                        help="Skip publishing to RabbitMQ (books won't be auto-indexed in OpenSearch)")
    args = parser.parse_args()

    seed(args.count, use_rabbitmq=not args.no_rabbitmq)
