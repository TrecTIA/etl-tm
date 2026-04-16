"""
loader/supabase_utils.py
------------------------
Fungsi untuk meng-upsert DataFrame hasil transformasi ke Supabase.

Mapping dataset_name → config tabel Supabase:
  pegawai                 → employees         (upsert by NIK/NIP → employee_no_subholding)
  riwayat_pendidikan      → education_history (upsert by NIK/NIP + tingkat_pendidikan + ...)
  riwayat_pekerjaan       → career_history    (upsert by NIK/NIP + tanggal_masuk + ...)
  riwayat_assesment       → assessments       (upsert by NIK/NIP)
  individual_career_roadmap → career_roadmap  (upsert by NIK/NIP + year)
"""

import os
import pandas as pd
from supabase import create_client, Client


# ── Supabase config ────────────────────────────────────────────────────────────
SUPABASE_URL         = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")


# ── Mapping dataset → tabel Supabase ──────────────────────────────────────────
# on_conflict: kolom-kolom yang menjadi unique key untuk upsert (comma-separated)
# column_map : rename kolom DataFrame (hasil transform) → nama kolom di tabel DB
#              kolom yang tidak ada di column_map akan di-drop otomatis
DATASET_CONFIG: dict[str, dict] = {
    "pegawai": {
        "table": "employees",
        "on_conflict": "employee_no_subholding",
        "column_map": {
            "NIK/NIP":        "employee_no_subholding",
            "NAMA":           "fullname",
            "TANGGAL LAHIR":  "birth_date",
            "JENIS KELAMIN":  "gender",
            "AGAMA":          "religion",
        },
    },
    "riwayat_pendidikan": {
        "table": "education_history",
        "on_conflict": "employee_no_subholding,tingkat_pendidikan,universitas,jurusan,tanggal_masuk",
        "column_map": {
            "NIK/NIP":                    "employee_no_subholding",
            "TINGKAT PENDIDIKAN":         "tingkat_pendidikan",
            "UNIVERSITAS":                "universitas",
            "JURUSAN":                    "jurusan",
            "TANGGAL MASUK PENDIDIKAN":   "tanggal_masuk",
            "TANGGAL KELUAR PENDIDIKAN":  "tanggal_keluar",
        },
    },
    "riwayat_pekerjaan": {
        "table": "career_history",
        "on_conflict": "employee_no_subholding,tanggal_masuk,subholding,subco",
        "column_map": {
            "NIK/NIP":               "employee_no_subholding",
            "SUBHOLDING":            "subholding",
            "SUBCO":                 "subco",
            "DIVISI":                "divisi",
            "DEPARTMENT":            "department",
            "LEVEL":                 "level",
            "JOB TITLE":             "job_title",
            "FUNCTION":              "function",
            "GOL":                   "golongan",
            "KJ":                    "kj",
            "TANGGAL MASUK KERJA":         "tanggal_masuk",
            "TANGGAL RESIGN/MUTASI": "tanggal_resign_mutasi",
        },
    },
    "riwayat_assesment": {
        "table": "assessments",
        "on_conflict": "employee_no_subholding",
        "column_map": {
            "NIK/NIP":              "employee_no_subholding",
            "Remarks":              "remarks",
            "DoB":                  "date_of_birth",
            "AGE":                  "age",
            "INT_INT":              "int_int",
            "EXC_INT":              "exc_int",
            "COMP_INT":             "comp_int",
            "HUM_INT":              "hum_int",
            "ADM_INT":              "adm_int",
            "PLC_INT":              "plc_int",
            "MCH_INT":              "mch_int",
            "FTW_INT":              "ftw_int",
            "EMP_INT":              "emp_int",
            "PEOPLE":               "people",
            "ORGANIZATION":         "organization",
            "BUSINESS":             "business",
            "PERSISTENT":           "persistent",
            "PERSEVERANCE":         "perseverance",
            "PASSION":              "passion",
            "GRIT":                 "grit",
            "Value Driven":         "value_driven",
            "Leadership Comp":      "leadership_comp",
            "Business Comp":        "business_comp",
            "Technical Comp":       "technical_comp",
            "OVERALL_SKOR":         "overall_skor",
            "TALENT CLASS":         "talent_class",
            "STRENGTH":             "strength",
            "AREA OF DEVELOPMENT (GAP)": "area_of_development",
            "IDP":                  "idp",
            "IQ":                   "iq",
            "DISC":                 "disc",
        },
    },
    "individual_career_roadmap": {
        "table": "career_roadmap",
        "on_conflict": "employee_no_subholding,year",
        "column_map": {
            "NIK/NIP":           "employee_no_subholding",
            "planned_position":  "planned_position",
            "year":              "year",
        },
    },
}


def get_supabase_client() -> Client:
    """Buat Supabase client dengan service_role key (bypass RLS)."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise ValueError(
            "SUPABASE_URL dan SUPABASE_SERVICE_KEY harus diset di .env. "
            "Gunakan service_role key (bukan anon key) agar bisa bypass RLS."
        )
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def upsert_dataset(
    supabase: Client,
    dataset_name: str,
    df: pd.DataFrame,
) -> dict[str, int]:
    """
    Upsert satu DataFrame ke tabel Supabase sesuai DATASET_CONFIG.

    Args:
        supabase:     Supabase client
        dataset_name: Nama dataset (key di DATASET_CONFIG), e.g. 'pegawai'
        df:           DataFrame hasil transformasi

    Returns:
        dict {"rows_upserted": N, "rows_skipped": 0}
    """
    config = DATASET_CONFIG.get(dataset_name)
    if not config:
        print(f"[Loader-Supabase] ⚠  Dataset '{dataset_name}' tidak ada di DATASET_CONFIG, skip.")
        return {"rows_upserted": 0, "rows_skipped": 0}

    # Rename kolom sesuai mapping
    col_map = config["column_map"]
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Hanya ambil kolom target yang ada di DataFrame
    target_cols = [c for c in col_map.values() if c in df.columns]
    if not target_cols:
        print(f"[Loader-Supabase] ⚠  Tidak ada kolom yang cocok untuk '{dataset_name}', skip.")
        return {"rows_upserted": 0, "rows_skipped": 0}

    df = df[target_cols].copy()

    # Hapus baris kosong semua kolom
    df = df.dropna(how="all")

    # Konversi ke records (handle NaN → None)
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    if not records:
        print(f"[Loader-Supabase] ⚠  Tidak ada data untuk di-upsert ke '{config['table']}'.")
        return {"rows_upserted": 0, "rows_skipped": 0}

    # Upsert dalam batch 500 rows
    batch_size = 500
    total = 0
    on_conflict = config["on_conflict"]

    for i in range(0, len(records), batch_size):
        batch = records[i: i + batch_size]
        response = supabase.table(config["table"]).upsert(
            batch, on_conflict=on_conflict
        ).execute()
        total += len(response.data) if response.data else len(batch)

    print(f"[Loader-Supabase] ✓  '{dataset_name}' → tabel '{config['table']}': {total} rows upserted")
    return {"rows_upserted": total, "rows_skipped": 0}


def save_to_supabase(datasets: dict[str, pd.DataFrame]) -> dict[str, dict]:
    """
    Upsert semua dataset ke Supabase.

    Args:
        datasets: dict mapping nama_dataset → DataFrame (output dari transformer)

    Returns:
        dict mapping nama_dataset → {"rows_upserted": N}
    """
    supabase = get_supabase_client()
    results: dict[str, dict] = {}

    print(f"\n[Loader-Supabase] Meng-upsert {len(datasets)} dataset ke Supabase...")

    for name, df in datasets.items():
        try:
            result = upsert_dataset(supabase, name, df)
            results[name] = result
        except Exception as e:
            print(f"[Loader-Supabase] ❌  Error upsert '{name}': {e}")
            results[name] = {"rows_upserted": 0, "error": str(e)}

    return results
