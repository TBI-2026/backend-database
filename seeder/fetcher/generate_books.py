#!/usr/bin/env python3
"""
One-command book CSV pipeline: fetch (API atau bulk dump) → validate → output folder.

Output structure:
    books_<count>/
        raw.csv     (mentah)
        clean.csv   (sudah dinormalisasi, siap di-seed)

Sources:
    --source api    : Open Library Search + Works API (default, cocok untuk < 5000 buku)
    --source dump   : Bulk dump (paling etis, butuh ~13 GB disk, cocok untuk > 10k buku)

Usage:
    python generate_books.py --count 20                       # API mode (default)
    python generate_books.py --count 100000 --source dump     # Bulk dump mode
    python generate_books.py --count 1000 --no-description    # API tanpa Works call
    python generate_books.py --count 5000 --output-dir my_books
"""

import argparse
import os

from validate_books import validate

# Default dirs resolved from script location (seeder/fetcher/), so paths point
# into seeder/ regardless of CWD.
SEEDER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DUMP_DIR = os.path.join(SEEDER_DIR, "dumps")


def fetch_via_api(count, raw_path, with_description, language, overshoot):
    from fetch_books import fetch_books
    fetch_books(count, raw_path, with_description, language, overshoot)


def fetch_via_dump(count, raw_path, language, oversample, dump_dir):
    from fetch_books_from_dump import fetch_books_from_dump
    fetch_books_from_dump(count, raw_path, language, dump_dir, oversample)


def generate(count: int, source: str, with_description: bool, language: str,
             overshoot: float, oversample: int, output_dir: str | None,
             dump_dir: str) -> None:
    folder = output_dir or os.path.join(SEEDER_DIR, f"books_{count}")
    os.makedirs(folder, exist_ok=True)

    raw_path = os.path.join(folder, "raw.csv")
    clean_path = os.path.join(folder, "clean.csv")

    print(f"╔{'═' * 68}╗")
    print(f"║ STEP 1/2 — FETCH ({source.upper()}) → {raw_path}")
    print(f"╚{'═' * 68}╝")

    if source == "dump":
        fetch_via_dump(count, raw_path, language, oversample, dump_dir)
    else:
        fetch_via_api(count, raw_path, with_description, language, overshoot)

    print(f"\n╔{'═' * 68}╗")
    print(f"║ STEP 2/2 — VALIDATE & NORMALIZE → {clean_path}")
    print(f"╚{'═' * 68}╝")
    validate(raw_path, clean_path)

    print(f"\n✓ Done. Output folder: {folder}/")
    print(f"  ├── raw.csv     (mentah)")
    print(f"  └── clean.csv   (siap di-seed)")
    if source == "dump":
        print(f"\nDump files masih di '{dump_dir}/'.")
        print(f"Bisa dihapus manual jika sudah selesai: rm -rf {dump_dir}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate book CSV (fetch + validate) dalam satu command",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Test 20 buku via API:
    python generate_books.py --count 20

  Production 100k via bulk dump (paling etis):
    python generate_books.py --count 100000 --source dump

  Production 100k via API (slower tapi tidak butuh disk besar):
    python generate_books.py --count 100000 --source api
        """
    )
    parser.add_argument("--count", type=int, default=20,
                        help="Jumlah buku target (default: 20)")
    parser.add_argument("--source", choices=["api", "dump"], default="api",
                        help="Data source: 'api' (default, cepat untuk < 5k) atau "
                             "'dump' (etis, butuh ~13 GB disk, cocok untuk > 10k)")
    parser.add_argument("--no-description", dest="with_description",
                        action="store_false", default=True,
                        help="[API only] Skip Works API call (synopsis pakai template)")
    parser.add_argument("--language", default="English",
                        help="Bahasa untuk semua row (default: English)")
    parser.add_argument("--overshoot", type=float, default=1.15,
                        help="[API only] Multiplier fetch untuk kompensasi drop validasi (default: 1.15)")
    parser.add_argument("--oversample", type=int, default=5,
                        help="[dump only] Pool size multiplier untuk filter quality "
                             "(default: 5x). Higher = better quality, slower parsing.")
    parser.add_argument("--output-dir", default=None,
                        help="Folder output custom (default: books_<count>)")
    parser.add_argument("--dump-dir", default=DEFAULT_DUMP_DIR,
                        help="[dump only] Folder untuk simpan .txt.gz (default: ../dumps relative to this script)")
    args = parser.parse_args()

    generate(args.count, args.source, args.with_description, args.language,
             args.overshoot, args.oversample, args.output_dir, args.dump_dir)
