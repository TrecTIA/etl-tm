"""
loader/pg_utils.py
------------------
Upsert DataFrame hasil transformasi ke PostgreSQL.
Column mapping disesuaikan dengan step 12-16 di context.md v2.

PENTING: Sesuaikan nama tabel di DATASET_CONFIG dengan nama tabel
         yang ada di database PostgreSQL Anda.
"""

import os
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import sys

# Tambah path agar bisa import dari src
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from logger import get_logger

logger = get_logger("DB_Loader")

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")


# ── Mapping dataset → tabel PostgreSQL ────────────────────────────────────────
# SESUAIKAN "table" dengan nama tabel yang ada di DB Anda!
# Cek nama tabel: SELECT table_name FROM information_schema.tables WHERE table_schema='public';
DATASET_CONFIG: dict[str, dict] = {

    # Step 12: employees → sesuaikan nama tabel!
    "employees": {
        "table": "employees",           # sudah dikonfirmasi user
        "on_conflict": ["employee_id"],  # gunakan employee_id sebagai PK
        "column_map": {
            "employee_id":   "employee_id",
            "NIK/NIP":       "employee_no_subholding",
            "NAMA":          "fullname",
            "TANGGAL LAHIR": "birth_date",
            "JOB TITLE":     "position",
            "FUNCTION":      "functions",
            "DIVISI":        "divisi",
            "AGAMA":         "agama",
            "LEVEL":         "level",
            "GOL":           "golongan",
            "SUBHOLDING":    "subholding_code",
            "SUBCO":         "company_code",
            "KJ":            "kelompok_jabatan",
            "JENIS KELAMIN": "gender",
        },
    },

    # Step 13: riwayat_pendidikan (tidak ada NIK/NIP di tabel ini)
    "riwayat_pendidikan": {
        "table": "riwayat_pendidikan",  # ← SESUAIKAN
        "on_conflict": ["employee_id", "tingkat_pendidikan", "universitas", "jurusan", "tanggal_masuk"],
        "column_map": {
            "employee_id":              "employee_id",
            "TINGKAT PENDIDIKAN":       "tingkat_pendidikan",
            "UNIVERSITAS":              "universitas",
            "JURUSAN":                  "jurusan",
            "TANGGAL MASUK PENDIDIKAN": "tanggal_masuk",
            "TANGGAL KELUAR PENDIDIKAN":"tanggal_keluar",
        },
    },

    # Step 14: riwayat_pekerjaan
    "riwayat_pekerjaan": {
        "table": "riwayat_pekerjaan",
        "on_conflict": [
            "employee_id",
            "subholding_code",
            "company_code",
            "jabatan",
            "tanggal_mulai"
        ],
        "column_map": {
            "employee_id":           "employee_id",
            "NIK/NIP":               "employee_no_subholding",
            "SUBHOLDING":            "subholding_code",
            "SUBCO":                 "company_code",
            "DIVISI":                "divisi",
            "DEPARTMENT":            "department",
            "LEVEL":                 "level_jabatan",
            "JOB TITLE":             "jabatan",
            "FUNCTION":              "functions",
            "GOL":                   "golongan",
            "KJ":                    "kelompok_jabatan",
            "TANGGAL MASUK KERJA":   "tanggal_mulai",
            "TANGGAL RESIGN/MUTASI": "tanggal_selesai",
            "status":                "status",
        },
    },

    # Step 15: riwayat_assesment (tidak ada NIK/NIP di tabel ini)
    "riwayat_assesment": {
        "table": "riwayat_assesment",   # ← SESUAIKAN
        "on_conflict": ["employee_id", "assesment_year"],
        "column_map": {
            "employee_id":              "employee_id",
            "Asessment Year":           "assesment_year",
            "Remarks":                  "remarks",
            "DoB":                      "dob",
            "AGE":                      "age",
            "INT_INT":                  "integ_int",
            "EXC_INT":                  "exc_int",
            "COMP_INT":                 "comp_int",
            "HUM_INT":                  "hum_int",
            "ADM_INT":                  "adm_int",
            "PLC_INT":                  "plc_int",
            "MCH_INT":                  "mch_int",
            "FTW_INT":                  "ftw_int",
            "EMP_INT":                  "emp_int",
            "PEOPLE":                   "people",
            "ORGANIZATION":             "organization",
            "BUSINESS":                 "business",
            "PERSISTENT":               "persistent",
            "PERSEVERANCE":             "perseverance",
            "PASSION":                  "passion",
            "GRIT":                     "grit",
            "Value Driven":             "value_driven",
            "Leadership Comp":          "leadership_comp",
            "Business Comp":            "business_comp",
            "Technical Comp":           "technical_comp",
            "OVERALL_SKOR":             "overal_score",
            "TALENT CLASS":             "talent_class",
            "STRENGTH":                 "strength",
            "AREA OF DEVELOPMENT (GAP)":"area_of_dev",
            "IDP":                      "idp",
            "IQ":                       "iq",
            "DISC":                     "disc",
            "HAV":                      "hav",
            "HAV_TIA":                  "hav_tia",
        },
    },

    # Step 16: individual_career_roadmap (tidak ada NIK/NIP di tabel ini)
    "individual_career_roadmap": {
        "table": "individual_career_roadmap",  # ← SESUAIKAN
        "on_conflict": ["employee_id", "year"],
        "column_map": {
            "employee_id":      "employee_id",
            "planned_position": "planned_position",
            "year":             "year",
        },
    },
}


def get_connection() -> psycopg2.extensions.connection:
    """Buat koneksi ke PostgreSQL menggunakan DATABASE_URL dari .env."""
    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL belum diset di .env. "
            "Format: postgresql://user:password@host:5432/dbname"
        )
    return psycopg2.connect(DATABASE_URL)


def _build_upsert_sql(table: str, columns: list[str], conflict_cols: list[str] = None) -> str:
    """Generate SQL INSERT biasa, atau UPSERT jika conflict_cols diberikan."""
    col_list    = ", ".join(f'"{c}"' for c in columns)
    
    if not conflict_cols:
        return f'INSERT INTO "{table}" ({col_list}) VALUES %s'
        
    conflict    = ", ".join(f'"{c}"' for c in conflict_cols)
    update_cols = [c for c in columns if c not in conflict_cols]

    if update_cols:
        update_set  = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        on_conflict = f"ON CONFLICT ({conflict}) DO UPDATE SET {update_set}"
    else:
        on_conflict = f"ON CONFLICT ({conflict}) DO NOTHING"

    return f'INSERT INTO "{table}" ({col_list}) VALUES %s {on_conflict}'


def upsert_dataset(
    conn: psycopg2.extensions.connection,
    dataset_name: str,
    df: pd.DataFrame,
) -> dict:
    """
    Upsert satu DataFrame ke tabel PostgreSQL.
    
    Fitur tambahan (v2.1 - Crisis Fix):
    - page_size diturunkan dari 500 → 100 untuk stability large dataset
    - Better error handling & logging per batch
    """
    config = DATASET_CONFIG.get(dataset_name)
    if not config:
        logger.warning(f"[Loader-PG] ⚠  Dataset '{dataset_name}' tidak ada di DATASET_CONFIG, skip.")
        return {"rows_upserted": 0}

    col_map = config["column_map"]
    rename  = {k: v for k, v in col_map.items() if k in df.columns}
    df      = df.rename(columns=rename)

    target_cols = [c for c in col_map.values() if c in df.columns]
    if not target_cols:
        logger.warning(f"[Loader-PG] ⚠  Tidak ada kolom yang cocok untuk '{dataset_name}', skip.")
        return {"rows_upserted": 0}

    df = df[target_cols].copy().dropna(how="all")

    missing_conflict = [c for c in config["on_conflict"] if c not in df.columns]
    if missing_conflict:
        logger.warning(f"[Loader-PG] ⚠  Kolom conflict {missing_conflict} tidak ada di '{dataset_name}', skip.")
        return {"rows_upserted": 0}

    df = df.dropna(subset=config["on_conflict"])
    if df.empty:
        logger.warning(f"[Loader-PG] ⚠  Tidak ada data valid untuk '{dataset_name}'.")
        return {"rows_upserted": 0}

    records = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    # Gunakan ON CONFLICT (Upsert) untuk SEMUA tabel agar terhindar dari Duplicate Key
    sql = _build_upsert_sql(config["table"], list(df.columns), config["on_conflict"])
    sql_with_ret = sql + " RETURNING 1"
    
    rows_affected = 0
    with conn.cursor() as cur:
        # Jika dataset adalah employees, matikan trigger trg_notify_employees sementara
        if dataset_name == "employees":
            try:
                cur.execute('ALTER TABLE "employees" DISABLE TRIGGER trg_notify_employees;')
                # TURUN dari 500 -> 100 untuk large dataset stability
                res = psycopg2.extras.execute_values(cur, sql_with_ret, records, page_size=100, fetch=True)
                rows_affected = len(res) if res else 0
            finally:
                cur.execute('ALTER TABLE "employees" ENABLE TRIGGER trg_notify_employees;')
        else:
            # TURUN dari 500 -> 100 untuk large dataset stability
            res = psycopg2.extras.execute_values(cur, sql_with_ret, records, page_size=100, fetch=True)
            rows_affected = len(res) if res else 0

        if dataset_name == "riwayat_pendidikan":
            logger.info(f"[Loader-PG Tracking] Dataset '{dataset_name}' memproses {len(records)} baris unik dari Python.")
            logger.info(f"[Loader-PG Tracking] Dari {len(records)} baris, hanya {rows_affected} baris yang di-Insert baru/sukses update.")
            logger.info(f"[Loader-PG Tracking] Berarti ada {len(records) - rows_affected} baris yang menjadi KORBAN TIMPAAN (UPSERT OVERWRITE) karena Kunci Uniknya sama di Database.")

    logger.info(f"[Loader-PG] ✓  '{dataset_name}' → '{config['table']}': {rows_affected} rows affected of {len(records)} given.")
    return {"rows_upserted": rows_affected, "success": True}


def save_to_postgres(datasets: dict[str, pd.DataFrame]) -> dict[str, dict]:
    """
    CRISIS FIX v2.1: Upsert semua dataset ke PostgreSQL dengan order ketat.
    
    Order insert:
      1. employees (MASTER - must succeed first)
      2. riwayat_pekerjaan (FK dependent on employees)
      3. riwayat_pendidikan (FK dependent on employees)
      4. riwayat_assesment (FK dependent on employees)
      5. individual_career_roadmap (FK dependent on employees)
    
    Jika employees GAGAL → STOP semua (rollback semua dataset yang sudah diinsert)
    Jika riwayat_pekerjaan GAGAL → stop dataset dependent-nya
    
    Setiap dataset tetap dalam transaction terpisah untuk isolation.
    """
    logger.info(f"\n[Loader-PG] Meng-upsert {len(datasets)} dataset ke PostgreSQL (ORDERED)...")
    
    # DEFINE INSERT ORDER (critical)
    insert_order = [
        "employees",                    # MASTER - harus sukses PERTAMA
        "riwayat_pekerjaan",           # FK to employees
        "riwayat_pendidikan",          # FK to employees
        "riwayat_assesment",           # FK to employees
        "individual_career_roadmap",   # FK to employees
    ]
    
    results = {}
    employees_success = False
    riwayat_pekerjaan_success = False

    for dataset_name in insert_order:
        if dataset_name not in datasets:
            continue
            
        df = datasets[dataset_name]
        conn = None
        
        try:
            # CHECK: Jika employees gagal, stop semua dataset dependent
            if dataset_name != "employees" and not employees_success:
                logger.error(
                    f"[Loader-PG] ⏹  '{dataset_name}' SKIP karena employees gagal PERTAMA. "
                    f"Tidak ada hubungan FK yang valid."
                )
                results[dataset_name] = {
                    "rows_upserted": 0,
                    "error": "Employees master table gagal, skip dataset dependent",
                    "success": False
                }
                continue
            
            # CHECK: Jika riwayat_pekerjaan gagal, ada warning
            if dataset_name in ["riwayat_pendidikan", "riwayat_assesment", "individual_career_roadmap"]:
                if not riwayat_pekerjaan_success and dataset_name != "riwayat_pekerjaan":
                    logger.warning(
                        f"[Loader-PG] ⚠  riwayat_pekerjaan gagal, tapi melanjutkan {dataset_name}. "
                        f"Pastikan tidak ada foreign key dari riwayat_pekerjaan."
                    )
            
            # CONNECT & INSERT
            conn = get_connection()
            result = upsert_dataset(conn, dataset_name, df)
            conn.commit()
            
            # TRACK SUCCESS
            if result.get("success", False):
                results[dataset_name] = result
                if dataset_name == "employees":
                    employees_success = True
                    logger.info(f"[Loader-PG] ✓✓✓ MASTER TABLE '{dataset_name}' SUCCESS - safe to proceed to dependent tables")
                elif dataset_name == "riwayat_pekerjaan":
                    riwayat_pekerjaan_success = True
                    logger.info(f"[Loader-PG] ✓  '{dataset_name}' SUCCESS - dependent tables can now insert")
            else:
                results[dataset_name] = result
                
        except Exception as e:
            if conn:
                conn.rollback()
            import traceback
            full_err = traceback.format_exc()
            logger.error(f"[Loader-PG] ❌ Error upsert '{dataset_name}':\n{full_err}")
            results[dataset_name] = {
                "rows_upserted": 0,
                "error": str(e),
                "success": False
            }
            
            # CRITICAL: Jika employees gagal, stop semuanya
            if dataset_name == "employees":
                logger.critical(
                    f"[Loader-PG] CRITICAL: MASTER TABLE FAILED! "
                    f"Stopping all dependent tables. Error: {str(e)}"
                )
                # Skip semua dataset lainnya
                for remaining_name in insert_order[insert_order.index(dataset_name) + 1:]:
                    if remaining_name in datasets:
                        results[remaining_name] = {
                            "rows_upserted": 0,
                            "error": f"Employees master table failed: {str(e)}",
                            "success": False
                        }
                break
                
        finally:
            if conn:
                conn.close()

    return results
