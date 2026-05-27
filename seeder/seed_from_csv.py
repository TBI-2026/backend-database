#!/usr/bin/env python3
"""
Seed PostgreSQL from clean.csv (output of fetcher/validate_books.py).

Hybrid pipeline: direct DB insert + publish 'book.created' event ke RabbitMQ
agar search-service auto-index ke OpenSearch.

Default input: books_100000/clean.csv (relative to this script).

Usage:
    python seed_from_csv.py
    python seed_from_csv.py --input books_100000/clean.csv
    python seed_from_csv.py --count 100              # test 100 rows dulu
    python seed_from_csv.py --no-rabbitmq            # skip event publish
    python seed_from_csv.py --truncate               # WIPE book-related tables dulu
    python seed_from_csv.py --truncate --yes         # skip confirmation prompt

Environment variables (or .env file):
    DB_HOST, DB_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
    CREATED_BY_USER_ID   (UUID user; defaults to a fixed UUID)
"""

import argparse
import csv
import json
import os
import time
import uuid
from typing import Optional

import pika
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT = os.path.join(SCRIPT_DIR, "books_100000", "clean.csv")

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
BATCH_SIZE = 500

# Tables wiped on --truncate. `genre` sengaja dikecualikan (lookup table dengan
# id 1-17 yang di-reference oleh genre_id di CSV).
TRUNCATE_TABLES = [
    "having_genre", "authored_by", "book_location",
    "review", "having_user_book",
    "book", "publisher", "author", "book_language",
]

# Field length limits (Hibernate Entity, strictest layer)
MAX_TITLE = 255
MAX_BOOK_PICTURE = 255
MAX_PUBLISHER = 50
MAX_LANGUAGE = 30
MAX_AUTHOR = 50
MAX_AUTHORS_PER_BOOK = 5


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def upsert_publisher(cur, name: str) -> int:
    cur.execute(
        "INSERT INTO publisher (name) VALUES (%s) "
        "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
        (name,),
    )
    return cur.fetchone()[0]


def upsert_language(cur, language: str) -> int:
    cur.execute(
        "INSERT INTO book_language (language) VALUES (%s) "
        "ON CONFLICT DO NOTHING RETURNING id",
        (language,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT id FROM book_language WHERE language = %s", (language,))
    return cur.fetchone()[0]


def upsert_author(cur, name: str) -> int:
    cur.execute(
        "INSERT INTO author (name) VALUES (%s) "
        "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
        (name,),
    )
    return cur.fetchone()[0]


def insert_book(cur, book_id, isbn, title, synopsis, cover_url,
                total_pages, published_year, language_id, publisher_id,
                created_by) -> None:
    cur.execute(
        """
        INSERT INTO book (id, isbn, title, synopsis, book_picture,
                          total_pages, published_year, id_language,
                          id_publisher, created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (isbn) DO NOTHING
        """,
        (book_id, isbn, title, synopsis, cover_url,
         total_pages, published_year, language_id, publisher_id, created_by),
    )


def link_author(cur, book_id: str, author_id: int) -> None:
    cur.execute(
        "INSERT INTO authored_by (id_book, id_author) VALUES (%s, %s) "
        "ON CONFLICT DO NOTHING",
        (book_id, author_id),
    )


def link_genre(cur, book_id: str, genre_id: int) -> None:
    cur.execute(
        "INSERT INTO having_genre (id_book, id_genre) VALUES (%s, %s) "
        "ON CONFLICT DO NOTHING",
        (book_id, genre_id),
    )


# ---------------------------------------------------------------------------
# RabbitMQ
# ---------------------------------------------------------------------------

def create_rabbitmq_channel():
    conn = pika.BlockingConnection(pika.ConnectionParameters(**RABBITMQ_CONFIG))
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    return conn, ch


def publish_book_created(ch, book_id, title, synopsis, cover_url, created_by):
    payload = json.dumps({
        "id": book_id,
        "title": title,
        "synopsis": synopsis,
        "bookPicture": cover_url,
        "createdBy": created_by,
    })
    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY,
        body=payload,
        properties=pika.BasicProperties(
            content_type="application/json", delivery_mode=2,
        ),
    )


# ---------------------------------------------------------------------------
# Destructive ops
# ---------------------------------------------------------------------------

def confirm_truncate(skip_prompt: bool) -> bool:
    """Show warning + ask user to confirm. Returns True if go ahead."""
    print("\n" + "!" * 72)
    print("!!  DESTRUCTIVE OPERATION — TABEL BERIKUT AKAN DI-WIPE TOTAL:")
    for t in TRUNCATE_TABLES:
        print(f"!!     - {t}")
    print(f"!!  Target DB: {DB_CONFIG['user']}@{DB_CONFIG['host']}:"
          f"{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print("!" * 72)

    if skip_prompt:
        print("  --yes passed, skipping confirmation.")
        return True

    answer = input("Ketik 'WIPE' (huruf kapital) untuk lanjut, lainnya = batal: ").strip()
    return answer == "WIPE"


def truncate_book_data(cur, conn) -> None:
    # Cek tabel mana yang beneran ada (handle case dimana book_location skip
    # karena PostGIS tidak terinstall).
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = ANY(%s)",
        (TRUNCATE_TABLES,),
    )
    existing = [r[0] for r in cur.fetchall()]
    missing = set(TRUNCATE_TABLES) - set(existing)
    if missing:
        print(f"  (Skipping non-existent tables: {', '.join(sorted(missing))})")

    if not existing:
        print("  No tables to truncate.")
        return

    table_list = ", ".join(existing)
    print(f"\nTruncating: {table_list}...")
    cur.execute(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE")
    conn.commit()
    print("  ✓ Done. SERIAL sequences direset, genre table tidak disentuh.")


# ---------------------------------------------------------------------------
# Row normalization
# ---------------------------------------------------------------------------

def normalize_row(row: dict) -> Optional[dict]:
    """Defensive parsing. Returns None if row is invalid."""
    isbn = (row.get("isbn") or "").strip()
    title = (row.get("title") or "").strip()
    synopsis = (row.get("synopsis") or "").strip()

    if not isbn or not title or not synopsis:
        return None

    authors_raw = (row.get("authors") or "").strip()
    authors = [a.strip()[:MAX_AUTHOR] for a in authors_raw.split("|") if a.strip()]
    if not authors:
        authors = ["Unknown Author"]

    try:
        total_pages = max(1, int(row["total_pages"]))
        published_year = max(0, int(row["published_year"]))
        genre_id = int(row["genre_id"])
    except (ValueError, KeyError, TypeError):
        return None

    return {
        "isbn": isbn[:17],
        "title": title[:MAX_TITLE],
        "synopsis": synopsis,
        "cover_url": (row.get("cover_url") or "")[:MAX_BOOK_PICTURE],
        "total_pages": total_pages,
        "published_year": published_year,
        "publisher": (row.get("publisher") or "Unknown Publisher").strip()[:MAX_PUBLISHER],
        "language": (row.get("language") or "English").strip()[:MAX_LANGUAGE],
        "authors": authors[:MAX_AUTHORS_PER_BOOK],
        "genre_id": genre_id,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def seed(input_path: str, limit: Optional[int], use_rabbitmq: bool,
         truncate: bool, skip_prompt: bool) -> None:
    print(f"Reading '{input_path}'...")
    with open(input_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if limit:
        rows = rows[:limit]
    total = len(rows)
    print(f"  {total:,} rows to process")

    print(f"Connecting to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    if truncate:
        if not confirm_truncate(skip_prompt):
            print("Aborted (truncate not confirmed).")
            cur.close()
            conn.close()
            return
        truncate_book_data(cur, conn)

    rmq_conn = None
    rmq_ch = None
    if use_rabbitmq:
        print(f"Connecting to RabbitMQ at {RABBITMQ_CONFIG['host']}:{RABBITMQ_CONFIG['port']}...")
        try:
            rmq_conn, rmq_ch = create_rabbitmq_channel()
        except Exception as e:
            print(f"  Warning: RabbitMQ unavailable ({e}). Continuing without indexing.")
            use_rabbitmq = False

    inserted = skipped_existing = skipped_invalid = errored = 0
    published = 0
    start = time.time()

    for i, raw in enumerate(rows):
        data = normalize_row(raw)
        if data is None:
            skipped_invalid += 1
            continue

        try:
            publisher_id = upsert_publisher(cur, data["publisher"])
            language_id = upsert_language(cur, data["language"])

            book_id = str(uuid.uuid4())
            insert_book(
                cur, book_id, data["isbn"], data["title"], data["synopsis"],
                data["cover_url"], data["total_pages"], data["published_year"],
                language_id, publisher_id, CREATED_BY_USER_ID,
            )

            # Resolve actual book_id (ON CONFLICT may have skipped insert)
            cur.execute("SELECT id FROM book WHERE isbn = %s", (data["isbn"],))
            actual_book_id = str(cur.fetchone()[0])
            is_new = actual_book_id == book_id

            for author_name in data["authors"]:
                author_id = upsert_author(cur, author_name)
                link_author(cur, actual_book_id, author_id)

            link_genre(cur, actual_book_id, data["genre_id"])

            if is_new:
                inserted += 1
                if use_rabbitmq:
                    publish_book_created(
                        rmq_ch, actual_book_id, data["title"],
                        data["synopsis"], data["cover_url"], CREATED_BY_USER_ID,
                    )
                    published += 1
            else:
                skipped_existing += 1

        except Exception as e:
            conn.rollback()
            errored += 1
            print(f"\n  Error on row {i} (isbn={raw.get('isbn')}): {e}")
            continue

        if (i + 1) % BATCH_SIZE == 0:
            conn.commit()
            elapsed = time.time() - start
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (total - (i + 1)) / rate if rate > 0 else 0
            print(f"  [{i + 1:>6}/{total}] inserted={inserted} existing={skipped_existing} "
                  f"invalid={skipped_invalid} errors={errored} | "
                  f"{rate:.0f} rows/s | ETA {eta/60:.1f}m")

    conn.commit()
    cur.close()
    conn.close()
    if rmq_conn:
        rmq_conn.close()

    elapsed = time.time() - start
    print(f"\nDone in {elapsed/60:.1f}m")
    print(f"  Inserted (new)       : {inserted:,}")
    print(f"  Skipped (existing)   : {skipped_existing:,}")
    print(f"  Skipped (invalid CSV): {skipped_invalid:,}")
    print(f"  Errored              : {errored:,}")
    print(f"  Events published     : {published:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seed PostgreSQL from clean book CSV "
                    "(direct DB insert + RabbitMQ event publish)"
    )
    parser.add_argument("--input", default=DEFAULT_INPUT,
                        help=f"Clean CSV path (default: {DEFAULT_INPUT})")
    parser.add_argument("--count", type=int, default=None,
                        help="Limit number of rows to process (for testing)")
    parser.add_argument("--no-rabbitmq", dest="no_rabbitmq", action="store_true",
                        help="Skip RabbitMQ publishing "
                             "(books won't be indexed in OpenSearch)")
    parser.add_argument("--truncate", action="store_true",
                        help="DESTRUCTIVE: TRUNCATE book-related tables before "
                             "seeding (book, publisher, author, book_language, "
                             "and all junctions). Requires typed confirmation.")
    parser.add_argument("--yes", action="store_true",
                        help="Skip --truncate confirmation prompt (use with care)")
    args = parser.parse_args()

    seed(args.input, args.count,
         use_rabbitmq=not args.no_rabbitmq,
         truncate=args.truncate, skip_prompt=args.yes)
