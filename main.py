"""
main.py – Orchestrator utama ETL HCMS v2.

Program membaca file Excel, mentransformasi data, dan menyimpannya
sebagai beberapa file CSV.

Cara pakai:
  python main.py                            # interaktif, pilih file dari input/
  python main.py path/to/file.xlsx          # tentukan file langsung
  python main.py path/to/file.xlsx -o out/  # tentukan file + folder output
"""

import argparse
import os
import sys
import glob

from src.extractor.main import run as extract
from src.transformer.main import run as transform
from src.loader.main import run as load


# ── Default paths ──────────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT   = os.path.join(BASE_DIR, "input")
DEFAULT_OUTPUT  = os.path.join(BASE_DIR, "output")


def pick_file_interactively(input_dir: str) -> str:
    """Menampilkan daftar file .xlsx di input_dir dan meminta user memilih."""
    xlsx_files = glob.glob(os.path.join(input_dir, "*.xlsx"))

    if not xlsx_files:
        print(f"[Main] Tidak ada file .xlsx di folder: {input_dir}")
        sys.exit(1)

    if len(xlsx_files) == 1:
        chosen = xlsx_files[0]
        print(f"[Main] Ditemukan 1 file: {chosen}")
        return chosen

    print("\n[Main] Pilih file Excel yang akan diproses:")
    for i, path in enumerate(xlsx_files, start=1):
        print(f"  {i}. {os.path.basename(path)}")

    while True:
        try:
            choice = int(input(f"\nMasukkan nomor (1-{len(xlsx_files)}): "))
            if 1 <= choice <= len(xlsx_files):
                return xlsx_files[choice - 1]
        except (ValueError, KeyboardInterrupt):
            pass
        print("Pilihan tidak valid, coba lagi.")


def run_pipeline(input_path: str, output_dir: str) -> None:
    """Menjalankan pipeline ETL lengkap: Extract → Transform → Load."""
    print("=" * 60)
    print("  ETL HCMS v2")
    print("=" * 60)

    # 1. Extract
    data = extract(input_path)

    # 2. Transform
    datasets = transform(data["raw_data"], data["assesment"])

    # 3. Load
    saved = load(datasets, output_dir)

    # Ringkasan
    print("=" * 60)
    print("  ETL selesai! File yang dihasilkan:")
    for path in saved.values():
        print(f"    ✓ {os.path.basename(path)}")
    print(f"\n  Output folder: {output_dir}")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL HCMS v2 – Konversi Excel ke CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Contoh:
  python main.py
  python main.py input/data.xlsx
  python main.py input/data.xlsx -o hasil/
        """,
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        default=None,
        help=(
            "Path ke file Excel (.xlsx). "
            "Jika tidak diberikan, program akan menampilkan daftar file di folder input/."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Folder output untuk menyimpan CSV (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    # Tentukan file input
    if args.input_file:
        input_path = os.path.abspath(args.input_file)
        if not os.path.exists(input_path):
            print(f"[Main] Error: File tidak ditemukan: {input_path}")
            sys.exit(1)
    else:
        input_path = pick_file_interactively(DEFAULT_INPUT)

    output_dir = os.path.abspath(args.output)

    try:
        run_pipeline(input_path, output_dir)
    except Exception as exc:
        print(f"\n[Main] ❌  Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
