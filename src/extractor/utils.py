"""
extractor/utils.py
------------------
Fungsi-fungsi helper dan fungsi utama untuk membaca file Excel.
Mendukung dua mode input:
  - File path lokal  : read_excel(file_path)
  - Bytes dari memory: read_excel_from_bytes(file_bytes)
"""

import io
import os
import pandas as pd


def find_sheet(sheet_names: list[str], target: str) -> str | None:
    """Mencari nama sheet secara case-insensitive."""
    for name in sheet_names:
        if name.strip().lower() == target.strip().lower():
            return name
    return None


def _parse_excel_file(xls: pd.ExcelFile) -> dict[str, pd.DataFrame]:
    """
    Logika parsing bersama — dipakai oleh read_excel() dan read_excel_from_bytes().
    Membaca sheet 'Raw Data' dan 'Assesment'/'Assessment'.
    """
    sheet_names = xls.sheet_names
    print(f"[Extractor] Sheet tersedia: {sheet_names}")

    # Cari sheet 'Raw Data'
    raw_data_sheet = find_sheet(sheet_names, "Raw Data")
    if raw_data_sheet is None:
        raise ValueError(
            f"Sheet 'Raw Data' tidak ditemukan. Sheet tersedia: {sheet_names}"
        )

    # Cari sheet 'Assesment' (toleransi typo: Assesment / Assessment)
    assesment_sheet = find_sheet(sheet_names, "Assesment") or find_sheet(
        sheet_names, "Assessment"
    )
    if assesment_sheet is None:
        raise ValueError(
            f"Sheet 'Assesment'/'Assessment' tidak ditemukan. Sheet tersedia: {sheet_names}"
        )

    raw_data_df = pd.read_excel(xls, sheet_name=raw_data_sheet, dtype=str)
    assesment_df = pd.read_excel(xls, sheet_name=assesment_sheet, dtype=str)

    raw_data_df = _clean_dataframe(raw_data_df)
    assesment_df = _clean_dataframe(assesment_df)

    print(f"[Extractor] Raw Data  : {len(raw_data_df)} baris, {len(raw_data_df.columns)} kolom")
    print(f"[Extractor] Assesment : {len(assesment_df)} baris, {len(assesment_df.columns)} kolom")

    return {"raw_data": raw_data_df, "assesment": assesment_df}


def read_excel(file_path: str) -> dict[str, pd.DataFrame]:
    """
    Membaca file Excel dari path lokal.
    Dipakai saat ETL dijalankan secara CLI / standalone.

    Args:
        file_path: Path lengkap ke file Excel (.xlsx)

    Returns:
        dict dengan key 'raw_data' dan 'assesment'
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    if not file_path.lower().endswith(".xlsx"):
        raise ValueError(f"File harus berformat .xlsx: {file_path}")

    print(f"[Extractor] Membaca file: {file_path}")
    xls = pd.ExcelFile(file_path)
    return _parse_excel_file(xls)


def read_excel_from_bytes(file_bytes: bytes) -> dict[str, pd.DataFrame]:
    """
    Membaca file Excel dari bytes (hasil download Supabase Storage).
    Dipakai saat ETL dipanggil via API (FastAPI).

    Args:
        file_bytes: Konten file .xlsx sebagai bytes

    Returns:
        dict dengan key 'raw_data' dan 'assesment'
    """
    print(f"[Extractor] Membaca file dari bytes ({len(file_bytes):,} bytes)")
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    return _parse_excel_file(xls)


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace dari nama kolom dan nilai, ganti string kosong dengan NaN."""
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)
    df.replace(["nan", "NaN", ""], pd.NA, inplace=True)
    return df
