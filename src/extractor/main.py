"""
extractor/main.py
-----------------
Menjalankan proses ekstraksi data dari file Excel.
Mendukung dua mode:
  - run(file_path)         : dari path lokal (CLI / standalone)
  - run_from_bytes(bytes)  : dari bytes Supabase Storage (API)
"""

import sys
import pandas as pd

from .utils import read_excel, read_excel_from_bytes


def run(file_path: str) -> dict[str, pd.DataFrame]:
    """
    Ekstraksi dari file path lokal.

    Args:
        file_path: Path ke file Excel (.xlsx)

    Returns:
        dict berisi DataFrame 'raw_data' dan 'assesment'
    """
    print("\n[Extractor] ── Mulai ekstraksi (dari file lokal) ──")
    data = read_excel(file_path)
    print("[Extractor] ── Ekstraksi selesai ──\n")
    return data


def run_from_bytes(file_bytes: bytes) -> dict[str, pd.DataFrame]:
    """
    Ekstraksi dari bytes (hasil download Supabase Storage).
    Dipakai oleh api.py.

    Args:
        file_bytes: Konten file .xlsx sebagai bytes

    Returns:
        dict berisi DataFrame 'raw_data' dan 'assesment'
    """
    print("\n[Extractor] ── Mulai ekstraksi (dari Supabase Storage) ──")
    data = read_excel_from_bytes(file_bytes)
    print("[Extractor] ── Ekstraksi selesai ──\n")
    return data


# ── Standalone entry point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.extractor.main <path/to/file.xlsx>")
        sys.exit(1)

    result = run(sys.argv[1])
    for sheet, df in result.items():
        print(f"\nSheet '{sheet}' ({len(df)} baris):")
        print(df.head())
