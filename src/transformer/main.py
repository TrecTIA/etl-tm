"""
transformer/main.py
-------------------
Menjalankan seluruh pipeline transformasi data.
Sesuai context.md v2: tambah step enrich employee_id dari DB.
"""

import sys
import os
import pandas as pd

# Tambah path agar bisa import dari src
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from logger import get_logger

logger = get_logger("MainTransformer")

from .utils import (
    fill_missing_nik,
    get_last_unk_counter,
    enrich_raw_with_employee_id,
    enrich_assesment_with_employee_id,
    normalize_gender,
    transform_employees_from_raw,
    transform_employees_from_assesment,
    transform_riwayat_pendidikan,
    transform_riwayat_pekerjaan_from_raw,
    transform_riwayat_pekerjaan_from_assesment,
    transform_riwayat_assesment,
    transform_individual_career_roadmap,
)


def run_phase1(
    raw_data_df: pd.DataFrame,
    assesment_df: pd.DataFrame,
    unk_start_counter: int,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Phase 1: Extract + Transform dari sheet Raw Data.

    Pre-proses KEDUA sheet (agar NIK unk-xxxx konsisten lintas sheet),
    lalu hanya transform dataset yang bersumber dari Raw Data:
      - employees        (dari Raw Data)
      - riwayat_pendidikan (dari Raw Data)
      - riwayat_pekerjaan  (dari Raw Data)

    Returns:
        (datasets_phase1, preprocessed_assesment_df)
        - datasets_phase1        : dict untuk di-load ke DB
        - preprocessed_assesment_df : assesment yang sudah diisi NIK unk-xxxx,
                                      siap dikirim ke run_phase2 SETELAH phase 1 di-load
    """
    logger.info("\n[Transformer Phase 1] ── Mulai transformasi Raw Data ──")

    # Pre-process KEDUA sheet untuk NIK yang konsisten lintas sheet
    raw_data_df = normalize_gender(raw_data_df)
    raw_data_df, assesment_df = fill_missing_nik(raw_data_df, assesment_df, start_counter=unk_start_counter)

    # DB lookup untuk Raw Data (Phase 1)
    raw_data_df = enrich_raw_with_employee_id(raw_data_df)

    datasets: dict[str, pd.DataFrame] = {
        "employees":          transform_employees_from_raw(raw_data_df),
        "riwayat_pendidikan": transform_riwayat_pendidikan(raw_data_df),
        "riwayat_pekerjaan":  transform_riwayat_pekerjaan_from_raw(raw_data_df),
    }

    logger.info("[Transformer Phase 1] ── Transformasi Raw Data selesai ──\n")
    # Kembalikan juga assesment yang sudah pre-processed agar Phase 2 tidak perlu fill_missing_nik ulang
    return datasets, assesment_df


def run_phase2(
    assesment_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    Phase 2: Extract + Transform dari sheet Assesment.

    Dipanggil SETELAH Phase 1 berhasil di-load ke DB.
    DB di-query ulang (fresh) agar employee_id yang baru di-load dari Phase 1
    dapat dikenali dan di-match dengan benar.

    Datasets yang di-transform:
      - employees              (hanya karyawan baru yang tidak ada di Raw Data)
      - riwayat_pekerjaan      (hanya karyawan yang tidak ada di riwayat_pekerjaan DB)
      - riwayat_assesment      (semua dari Assesment)
      - individual_career_roadmap (semua dari Assesment)
    """
    logger.info("\n[Transformer Phase 2] ── Mulai transformasi Assesment ──")

    # Fresh DB lookup SETELAH Phase 1 di-load — kini mencakup karyawan dari Raw Data
    assesment_df = enrich_assesment_with_employee_id(assesment_df)

    datasets: dict[str, pd.DataFrame] = {
        "employees":                 transform_employees_from_assesment(assesment_df),
        "riwayat_pekerjaan":         transform_riwayat_pekerjaan_from_assesment(assesment_df),
        "riwayat_assesment":         transform_riwayat_assesment(assesment_df),
        "individual_career_roadmap": transform_individual_career_roadmap(assesment_df),
    }

    logger.info("[Transformer Phase 2] ── Transformasi Assesment selesai ──\n")
    return datasets


def run(
    raw_data_df: pd.DataFrame,
    assesment_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    Backward-compatible wrapper untuk mode batch/CSV.

    Menjalankan kedua phase transformasi lalu menggabungkan dataset dengan nama
    yang sama agar orchestrator lama (`main.py`) tetap berfungsi setelah refactor
    ke phase-based API.
    """
    datasets_phase1, preprocessed_assesment_df = run_phase1(raw_data_df, assesment_df)
    datasets_phase2 = run_phase2(preprocessed_assesment_df)

    merged_datasets = dict(datasets_phase1)
    for name, dataframe in datasets_phase2.items():
        if name in merged_datasets:
            merged_datasets[name] = pd.concat(
                [merged_datasets[name], dataframe],
                ignore_index=True,
            )
        else:
            merged_datasets[name] = dataframe

    return merged_datasets


# ── Standalone entry point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 3:
        logger.info(
            "Usage: python -m src.transformer.main "
            "<path/raw_data.csv> <path/assesment.csv>"
        )
        sys.exit(1)

    raw_df = pd.read_csv(sys.argv[1], dtype=str)
    ass_df = pd.read_csv(sys.argv[2], dtype=str)

    datasets_p1, preprocessed_ass = run_phase1(raw_df, ass_df)
    logger.info("── Phase 1 datasets ──")
    for name, df in datasets_p1.items():
        logger.info(f"\n── {name} ({len(df)} baris) ──")
        logger.info(df.head())

    datasets_p2 = run_phase2(preprocessed_ass)
    logger.info("── Phase 2 datasets ──")
    for name, df in datasets_p2.items():
        logger.info(f"\n── {name} ({len(df)} baris) ──")
        logger.info(df.head())
