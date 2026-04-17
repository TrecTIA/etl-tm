"""
transformer/utils.py
--------------------
Seluruh fungsi helper dan fungsi transformasi per dataset.
Disesuaikan dengan context.md v2:
  - NIK/NIP kosong diisi dengan "unk-{n}" mulai n=5001
  - employee_id di-lookup dari DB, jika tidak ada generate UUID baru
"""

import uuid
import os
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

import os
import sys

# Tambah path agar bisa import dari src
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from logger import get_logger

logger = get_logger("Transformer")

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")


# ─────────────────────────────────────────────────────────
#  COLUMN HELPER UTILITIES
# ─────────────────────────────────────────────────────────

def _clean_col_name(name: str) -> str:
    """
    Normalisasi nama kolom untuk perbandingan:
    - Strip whitespace & newline
    - Lowercase
    - Hapus suffix dalam tanda kurung, e.g. '(DD/MM/YY)' atau '(DD/MM/YYYY)'
    Contoh: 'TANGGAL LAHIR\n(DD/MM/YY)' → 'tanggal lahir'
    """
    # Ambil bagian sebelum newline atau tanda kurung pertama
    cleaned = name.split("\n")[0].split("(")[0]
    return cleaned.strip().lower()


def normalize_col(df: pd.DataFrame, col: str) -> str | None:
    """
    Mencari nama kolom secara case-insensitive & toleran spasi.
    Juga toleran terhadap suffix dalam tanda kurung dan newline di nama kolom Excel,
    misalnya 'TANGGAL LAHIR (DD/MM/YY)' akan cocok dengan pencarian 'TANGGAL LAHIR'.
    Mengembalikan nama kolom asli jika ditemukan, None jika tidak.
    """
    target = _clean_col_name(col)
    for c in df.columns:
        if _clean_col_name(c) == target:
            return c
    return None


def require_col(df: pd.DataFrame, col: str, context: str = "") -> str:
    """Seperti normalize_col tapi raise ValueError jika tidak ditemukan."""
    found = normalize_col(df, col)
    if found is None:
        raise ValueError(
            f"Kolom '{col}' tidak ditemukan"
            f"{' di ' + context if context else ''}. "
            f"Kolom tersedia: {list(df.columns)}"
        )
    return found


def get_cols(df: pd.DataFrame, wanted: list[str], context: str = "") -> dict[str, str]:
    """
    Mengembalikan mapping {nama_canonical → nama_asli_di_df}.
    Kolom yang tidak ada di DataFrame akan di-skip dengan warning.
    """
    mapping: dict[str, str] = {}
    for col in wanted:
        found = normalize_col(df, col)
        if found:
            mapping[col] = found
        else:
            logger.info(f"[Transformer] ⚠  Kolom '{col}' tidak ditemukan di {context}, dilewati.")
    return mapping


def select_and_rename(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    """Select kolom berdasarkan col_map lalu rename ke nama canonical."""
    available_cols = list(col_map.values())
    result = df[available_cols].copy()
    result.rename(columns={v: k for k, v in col_map.items()}, inplace=True)
    return result


# ─────────────────────────────────────────────────────────
#  STEP 0 – Normalisasi JENIS KELAMIN
# ─────────────────────────────────────────────────────────

def normalize_gender(raw_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 2 sesuai context.md:
    Ubah value kolom 'JENIS KELAMIN':
      - Uppercase dulu agar tidak case-sensitive
      - 'LAKI-LAKI' → 'Male'
      - 'PEREMPUAN'  → 'Female'
    """
    gender_col = normalize_col(raw_data_df, "JENIS KELAMIN")
    if gender_col is None:
        logger.info("[Transformer] ⚠  Kolom 'JENIS KELAMIN' tidak ditemukan, skip normalisasi gender.")
        return raw_data_df

    df = raw_data_df.copy()
    df[gender_col] = df[gender_col].astype(str).str.strip().str.upper()
    df[gender_col] = df[gender_col].replace({
        "LAKI-LAKI": "Male",
        "PEREMPUAN":  "Female",
        "L":          "Male",
        "P":          "Female",
        "M":          "Male",
        "F":          "Female",
        "NAN":        "Male",   # string 'nan' dari astype(str)
        "NONE":       "Male",
        "":           "Male",
    })
    # Nilai yang tidak cocok dengan mapping di atas: isi default 'Male'
    valid = {"Male", "Female"}
    mask_invalid = ~df[gender_col].isin(valid)
    if mask_invalid.any():
        logger.info(f"[Transformer] Gender tidak dikenali ({mask_invalid.sum()} baris) → diisi 'Male' sebagai default.")
        df.loc[mask_invalid, gender_col] = "Male"
    logger.info("[Transformer] Normalisasi JENIS KELAMIN selesai.")
    return df


# ─────────────────────────────────────────────────────────
#  STEP 1 – Handle NIK/NIP kosong (unk-5001, unk-5002, ...)
# ─────────────────────────────────────────────────────────

def get_last_unk_counter() -> int:
    """
    Query riwayat_pekerjaan untuk mendapatkan nilai counter unk- terakhir.
    Ambil semua employee_no_subholding yang berawalan 'unk-', strip prefix-nya,
    parse sebagai integer, ambil yang terbesar, lalu +1 sebagai titik mulai.
    Jika tidak ada → mulai dari 5001 (nilai awal default).
    """
    if not DATABASE_URL:
        return 5001
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT REPLACE(employee_no_subholding, 'unk-', '')::int AS nomor_urut
                FROM public.riwayat_pekerjaan
                WHERE employee_no_subholding ~ '^unk-[0-9]+$'
                ORDER BY nomor_urut DESC
                LIMIT 1
            """)
            row = cur.fetchone()
        conn.close()

        if row is None:
            logger.info("[Transformer] ┌─ get_last_unk_counter ─────────────────────────")
            logger.info("[Transformer] │  Belum ada data unk- di DB")
            logger.info("[Transformer] │  ETL akan memulai dari : unk-5001")
            logger.info("[Transformer] └────────────────────────────────────────────────")
            return 5001

        max_num = row[0]
        next_counter = max_num + 1
        logger.info("[Transformer] ┌─ get_last_unk_counter ─────────────────────────")
        logger.info(f"[Transformer] │  ID unk- terbesar di DB : unk-{max_num}")
        logger.info(f"[Transformer] │  ETL akan memulai dari  : unk-{next_counter}")
        logger.info("[Transformer] └────────────────────────────────────────────────")
        return next_counter
    except Exception as e:
        logger.info(f"[Transformer] ⚠  Gagal query counter unk- dari DB: {e}. Mulai dari 5001.")
        return 5001


def _lookup_existing_nik_by_name(names: list[str]) -> dict[str, str]:
    """
    Untuk daftar nama (lowercase), query DB dan kembalikan
    {lowercase_fullname → employee_no_subholding} yang sudah ada di DB.
    Hanya mengambil NIK yang bukan 'unk-' (prioritas NIK asli).
    Jika tidak ada NIK asli, fallback ke unk- yang sudah ada di DB.
    """
    if not DATABASE_URL or not names:
        return {}
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT LOWER(e.fullname) AS nama_lower,
                       rp.employee_no_subholding
                FROM employees e
                JOIN riwayat_pekerjaan rp ON e.employee_id = rp.employee_id
                WHERE LOWER(e.fullname) = ANY(%(names)s)
                ORDER BY
                    -- Prioritaskan NIK asli (bukan unk-)
                    CASE WHEN rp.employee_no_subholding NOT LIKE 'unk-%%' THEN 0 ELSE 1 END
            """, {"names": names})
            rows = cur.fetchall()
        conn.close()

        result: dict[str, str] = {}
        for row in rows:
            nama_lower = row["nama_lower"]
            nik = row["employee_no_subholding"]
            # Jangan override NIK asli dengan unk-
            if nama_lower not in result or not nik.startswith("unk-"):
                result[nama_lower] = nik

        logger.info(
            f"[Transformer] _lookup_existing_nik_by_name: "
            f"{len(result)} dari {len(names)} nama ditemukan di DB."
        )
        return result
    except Exception as e:
        logger.info(f"[Transformer] ⚠  Gagal lookup NIK by name dari DB: {e}")
        return {}


def fill_missing_nik(
    raw_data_df: pd.DataFrame,
    assesment_df: pd.DataFrame,
    start_counter: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Mengecek kolom NIK/NIP di kedua sheet.
    Jika ada baris yang NIK/NIP-nya kosong:
      1. Group by NAMA → kumpulkan NAMA unik yang NIK/NIP-nya kosong
      2. Generate ID format "unk-{n}" melanjutkan dari start_counter (sudah di-query di api.py)
      3. Isi NIK/NIP yang kosong dengan ID yang telah di-generate

    Args:
        start_counter: nilai awal counter unk- (didapat dari get_last_unk_counter() di api.py)

    Returns:
        (raw_data_df, assesment_df) yang sudah terisi NIK/NIP-nya
    """
    raw_nik_col  = require_col(raw_data_df, "NIK/NIP", "Raw Data")
    raw_nama_col = require_col(raw_data_df, "NAMA", "Raw Data")

    nama_to_id: dict[str, str] = {}

    def collect_missing(df: pd.DataFrame, nik_col: str, nama_col: str) -> None:
        missing_mask = df[nik_col].isna() | (df[nik_col].astype(str).str.strip() == "")
        for nama in df.loc[missing_mask, nama_col].dropna().unique():
            if nama not in nama_to_id:
                nama_to_id[nama] = None  # placeholder, di-assign setelah counter diketahui

    collect_missing(raw_data_df, raw_nik_col, raw_nama_col)

    ass_nik_col  = normalize_col(assesment_df, "NIK/NIP")
    ass_nama_col = normalize_col(assesment_df, "NAMA")
    if ass_nik_col and ass_nama_col:
        collect_missing(assesment_df, ass_nik_col, ass_nama_col)

    if not nama_to_id:
        logger.info("[Transformer] Semua NIK/NIP sudah terisi, tidak perlu generate ID.")
        return raw_data_df, assesment_df

    # Cek DB dulu: ada nama yang sudah punya NIK di DB?
    names_lower = [n.strip().lower() for n in nama_to_id.keys()]
    db_nik_map = _lookup_existing_nik_by_name(names_lower)

    # Assign: pakai NIK dari DB jika ada, generate unk- hanya jika benar-benar baru
    counter = start_counter
    reused = 0
    for nama in nama_to_id:
        existing = db_nik_map.get(nama.strip().lower())
        if existing:
            nama_to_id[nama] = existing
            reused += 1
        else:
            nama_to_id[nama] = f"unk-{counter}"
            counter += 1

    generated = counter - start_counter
    logger.info(
        f"[Transformer] NIK/NIP kosong ({len(nama_to_id)} nama): "
        f"{reused} diambil dari DB, {generated} di-generate baru "
        f"(unk-{start_counter} ... unk-{counter - 1})."
    )

    def fill_df(df: pd.DataFrame, nik_col: str, nama_col: str) -> pd.DataFrame:
        df = df.copy()
        missing_mask = df[nik_col].isna() | (df[nik_col].astype(str).str.strip() == "")
        df.loc[missing_mask, nik_col] = df.loc[missing_mask, nama_col].map(
            lambda n: nama_to_id.get(n, f"unk-{uuid.uuid4().hex[:6]}")
        )
        return df

    raw_data_df = fill_df(raw_data_df, raw_nik_col, raw_nama_col)
    if ass_nik_col and ass_nama_col:
        assesment_df = fill_df(assesment_df, ass_nik_col, ass_nama_col)

    return raw_data_df, assesment_df


# ─────────────────────────────────────────────────────────
#  STEP 2 – Enrich dengan employee_id dari DB
# ─────────────────────────────────────────────────────────

def _lookup_employee_ids_from_db() -> tuple[dict[str, str], dict[str, str]]:
    """
    Query DB dengan JOIN employees + riwayat_pekerjaan.
    Return (nik_to_id, nama_to_id):
      nik_to_id  : {employee_no_subholding → employee_id}  ← prioritas utama
      nama_to_id : {fullname → employee_id}                ← fallback
    Dipanggil terpisah untuk Phase 1 dan Phase 2 agar Phase 2 mendapat
    data terkini setelah Phase 1 di-load.
    """
    if not DATABASE_URL:
        return {}, {}
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT
                    e.employee_id,
                    e.fullname,
                    rp.employee_no_subholding
                FROM employees e
                LEFT JOIN riwayat_pekerjaan rp ON e.employee_id = rp.employee_id
            """)
            rows = cur.fetchall()
        conn.close()

        nik_to_id: dict[str, str] = {}
        nama_to_id: dict[str, str] = {}  # key: lowercase fullname
        for row in rows:
            if not row["employee_id"]:
                continue
            eid = str(row["employee_id"])
            if row["fullname"]:
                nama_to_id[str(row["fullname"]).strip().lower()] = eid
            if row["employee_no_subholding"]:
                nik_to_id[str(row["employee_no_subholding"]).strip()] = eid

        logger.info(
            f"[Transformer] DB lookup: {len(nama_to_id)} employees, "
            f"{len(nik_to_id)} NIK unik ditemukan dari DB."
        )
        return nik_to_id, nama_to_id
    except Exception as e:
        logger.info(f"[Transformer] ⚠  Gagal query DB untuk employee_id: {e}")
        logger.info("[Transformer]    Semua employee_id di-generate UUID baru (fallback tersinkron).")
        return {}, {}


def enrich_raw_with_employee_id(raw_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 1: Tambah kolom 'employee_id' ke raw_data_df.
    Prioritas matching:
      1. NIK/NIP cocok dengan employee_no_subholding di riwayat_pekerjaan → pakai employee_id dari DB
      2. NAMA cocok dengan fullname di employees                           → pakai employee_id dari DB
      3. Tidak ada yang cocok                                              → generate UUID baru
    """
    raw_nama_col = require_col(raw_data_df, "NAMA", "Raw Data (enrich)")
    raw_nik_col  = normalize_col(raw_data_df, "NIK/NIP")
    df_raw = raw_data_df.copy()

    # UUID baru per-nama sebagai fallback (konsisten: nama sama → UUID sama)
    all_names = set(df_raw[raw_nama_col].dropna().astype(str).str.strip())
    fallback_uuid: dict[str, str] = {nama: str(uuid.uuid4()) for nama in all_names}

    nik_db_map, nama_db_map = _lookup_employee_ids_from_db()

    matched_nik = 0
    matched_nama = 0

    def resolve(row) -> str:
        nonlocal matched_nik, matched_nama
        nik  = str(row[raw_nik_col]).strip() if raw_nik_col and pd.notna(row[raw_nik_col]) else ""
        nama = str(row[raw_nama_col]).strip()
        if nik and nik in nik_db_map:
            matched_nik += 1
            return nik_db_map[nik]
        # Skip pencocokan nama jika hanya 1 kata (rawan duplikat)
        if len(nama.split()) >= 2:
            nama_lower = nama.lower()
            if nama_lower in nama_db_map:
                matched_nama += 1
                return nama_db_map[nama_lower]
        return fallback_uuid.get(nama, str(uuid.uuid4()))

    df_raw["employee_id"] = df_raw.apply(resolve, axis=1)
    new_count = len(df_raw) - matched_nik - matched_nama
    logger.info(
        f"[Transformer] employee_id: {matched_nik} matched by NIK, "
        f"{matched_nama} matched by NAMA, {new_count} pakai UUID baru."
    )
    return df_raw


def enrich_assesment_with_employee_id(assesment_df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2: Tambah kolom 'employee_id' ke assesment_df.
    DB di-query ULANG (fresh) agar mencakup employees yang baru di-load di Phase 1.
    Prioritas matching:
      1. NIK/NIP cocok dengan employee_no_subholding di riwayat_pekerjaan → pakai employee_id dari DB
      2. NAMA cocok dengan fullname di employees                           → pakai employee_id dari DB
      3. Tidak ada yang cocok                                              → generate UUID baru
    """
    ass_nama_col = normalize_col(assesment_df, "NAMA")
    ass_nik_col  = normalize_col(assesment_df, "NIK/NIP")
    df_ass = assesment_df.copy()

    if not ass_nama_col and not ass_nik_col:
        logger.info("[Transformer] ⚠  Kolom 'NAMA'/'NIK/NIP' tidak ditemukan di Assesment, semua employee_id di-generate UUID baru.")
        df_ass["employee_id"] = [str(uuid.uuid4()) for _ in range(len(df_ass))]
        return df_ass

    # UUID baru per-nama sebagai fallback
    fallback_uuid: dict[str, str] = {}
    if ass_nama_col:
        for nama in df_ass[ass_nama_col].dropna().astype(str).str.strip().unique():
            fallback_uuid[nama] = str(uuid.uuid4())

    nik_db_map, nama_db_map = _lookup_employee_ids_from_db()

    matched_nik = 0
    matched_nama = 0

    def resolve(row) -> str:
        nonlocal matched_nik, matched_nama
        nik  = str(row[ass_nik_col]).strip()  if ass_nik_col  and pd.notna(row[ass_nik_col])  else ""
        nama = str(row[ass_nama_col]).strip() if ass_nama_col and pd.notna(row[ass_nama_col]) else ""
        if nik and nik in nik_db_map:
            matched_nik += 1
            return nik_db_map[nik]
        # Skip pencocokan nama jika hanya 1 kata (rawan duplikat)
        if nama and len(nama.split()) >= 2:
            nama_lower = nama.lower()
            if nama_lower in nama_db_map:
                matched_nama += 1
                return nama_db_map[nama_lower]
        return fallback_uuid.get(nama, str(uuid.uuid4()))

    df_ass["employee_id"] = df_ass.apply(resolve, axis=1)
    new_count = len(df_ass) - matched_nik - matched_nama
    logger.info(
        f"[Transformer] employee_id (Assesment): {matched_nik} matched by NIK, "
        f"{matched_nama} matched by NAMA, {new_count} pakai UUID baru."
    )
    return df_ass


# ─────────────────────────────────────────────────────────
#  STEP 3 – Pegawai
# ─────────────────────────────────────────────────────────

_WANTED_EMPLOYEES_RAW = [
    "TAHUN",
    "employee_id",
    "NIK/NIP",
    "NAMA",
    "TANGGAL LAHIR",
    "AGAMA",
    "JENIS KELAMIN",
    "SUBHOLDING",
    "SUBCO",
    "JOB TITLE",
    "FUNCTION",
]


def transform_employees_from_raw(raw_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 1: Transform tabel employees dari sheet Raw Data saja.
    Hanya data dari Raw Data yang di-load. Sheet Assesment diproses di Phase 2.
    """
    col_map = get_cols(raw_data_df, _WANTED_EMPLOYEES_RAW, "Raw Data (employees)")
    df = select_and_rename(raw_data_df, col_map)

    if "JOB TITLE" in df.columns:
        df["JOB TITLE"] = df["JOB TITLE"].replace(r'^\s*$', "Unknown", regex=True)
        df["JOB TITLE"] = df["JOB TITLE"].fillna("Unknown")

    if "TAHUN" in df.columns:
        df["TAHUN"] = pd.to_numeric(df["TAHUN"], errors="coerce")
        df = df.sort_values(by="TAHUN", ascending=False)
        df = df.drop(columns=["TAHUN"])

    df = df.groupby("employee_id", as_index=False).first()
    df.reset_index(drop=True, inplace=True)
    logger.info(f"[Transformer] employees (Raw Data)      : {len(df)} baris")
    return df


def transform_employees_from_assesment(assesment_df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2: Transform tabel employees dari sheet Assesment.
    Hanya karyawan yang BELUM ada di DB (baru ditemukan via Assesment) yang di-insert.
    Karyawan yang sudah ada (sudah di-load dari Raw Data di Phase 1) di-skip agar
    data lengkap dari Raw Data tidak tertimpa oleh data parsial dari Assesment.
    """
    WANTED_ASS = [
        "employee_id",
        "NIK/NIP",
        "NAMA",
        "SUB HOLDING",
        "SUB COMPANY",
    ]
    col_map = get_cols(assesment_df, WANTED_ASS, "Assesment (employees)")
    df = select_and_rename(assesment_df, col_map)
    df.rename(columns={"SUB HOLDING": "SUBHOLDING", "SUB COMPANY": "SUBCO"}, inplace=True)

    if "JOB TITLE" in df.columns:
        df["JOB TITLE"] = df["JOB TITLE"].replace(r'^\s*$', "Unknown", regex=True)
        df["JOB TITLE"] = df["JOB TITLE"].fillna("Unknown")

    # Filter: hanya karyawan yang BELUM ada di DB
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            with conn.cursor() as cur:
                cur.execute('SELECT employee_id FROM "employees"')
                existing_ids = {str(row[0]) for row in cur.fetchall()}
            conn.close()
            before = len(df)
            df = df[~df["employee_id"].isin(existing_ids)]
            logger.info(
                f"[Transformer] employees (Assesment): {before - len(df)} baris di-skip "
                f"(sudah ada di DB dari Phase 1), {len(df)} baris baru."
            )
        except Exception as e:
            logger.info(f"[Transformer] ⚠  Gagal query existing employees: {e}")

    df = df.groupby("employee_id", as_index=False).first()
    df.reset_index(drop=True, inplace=True)
    logger.info(f"[Transformer] employees (Assesment new) : {len(df)} baris")
    return df


# ─────────────────────────────────────────────────────────
#  STEP 4 – Riwayat Pendidikan
# ─────────────────────────────────────────────────────────

def transform_riwayat_pendidikan(raw_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Select employee_id, TINGKAT PENDIDIKAN, UNIVERSITAS, JURUSAN,
    TANGGAL MASUK PENDIDIKAN, TANGGAL KELUAR PENDIDIKAN dari Raw Data.
    NIK/NIP tidak disertakan (tidak ada di tabel DB).
    """
    WANTED = [
        "employee_id",
        "TINGKAT PENDIDIKAN",
        "UNIVERSITAS",
        "JURUSAN",
        "TANGGAL MASUK PENDIDIKAN",
        "TANGGAL KELUAR PENDIDIKAN",
        "TANGGAL SELESAI PENDIDIKAN",
    ]
    col_map = get_cols(raw_data_df, WANTED, "Raw Data (pendidikan)")

    # Normalise: rename 'TANGGAL SELESAI PENDIDIKAN' → 'TANGGAL KELUAR PENDIDIKAN'
    if "TANGGAL SELESAI PENDIDIKAN" in col_map and "TANGGAL KELUAR PENDIDIKAN" not in col_map:
        col_map["TANGGAL KELUAR PENDIDIKAN"] = col_map.pop("TANGGAL SELESAI PENDIDIKAN")
    df = select_and_rename(raw_data_df, col_map)
    # --- LOGGING TRACKING BARIS PENDIDIKAN ---
    logger.info(f"[Transformer] riwayat_pendidikan: Total baris mentah dari Excel: {len(df)}")
    
    # Cek baris kosong
    df_non_empty = df.dropna(subset=["TINGKAT PENDIDIKAN", "UNIVERSITAS", "JURUSAN"], how='all')
    logger.info(f"[Transformer] riwayat_pendidikan: Setelah dibuang baris yg kosong melompong: {len(df_non_empty)}")
    
    df_final = df_non_empty.drop_duplicates().reset_index(drop=True)
    
    terbuang = len(df_non_empty) - len(df_final)
    logger.info(f"[Transformer] riwayat_pendidikan: Baris terbuang karena 'drop_duplicates' (kembar identik sejajar): {terbuang}")
    
    logger.info(f"[Transformer] riwayat_pendidikan: Selesai diekstrak. Total siap dikirim ke DB Loader: {len(df_final)} baris\n")
    return df_final


# ─────────────────────────────────────────────────────────
#  STEP 5 – Riwayat Pekerjaan
# ─────────────────────────────────────────────────────────

_WANTED_PEKERJAAN_RAW = [
    "employee_id",
    "NAMA",
    "NIK/NIP",
    "SUBHOLDING",
    "SUBCO",
    "DIVISI",
    "DEPARTMENT",
    "LEVEL",
    "JOB TITLE",
    "FUNCTION",
    "GOL",
    "KJ",
    "TANGGAL MASUK KERJA",
    "TANGGAL RESIGN/MUTASI",
]

_DEDUP_SUBSET_PEKERJAAN = ["employee_id", "SUBHOLDING", "SUBCO", "JOB TITLE", "TANGGAL MASUK KERJA"]


def _finalize_pekerjaan(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """Shared post-processing: fill JOB TITLE, dedup, drop helper cols."""
    if "JOB TITLE" in df.columns:
        df["JOB TITLE"] = df["JOB TITLE"].replace(r'^\s*$', "Unknown", regex=True)
        df["JOB TITLE"] = df["JOB TITLE"].fillna("Unknown")

    subset_cols = [c for c in _DEDUP_SUBSET_PEKERJAAN if c in df.columns]
    duplicates = df[df.duplicated(subset=subset_cols, keep="first")]
    if not duplicates.empty:
        logger.info(f"[Transformer] {label}: Ditemukan {len(duplicates)} baris duplikat yang akan dibuang:")
        for _, row in duplicates.iterrows():
            nama   = row.get("NAMA",      "Unknown")
            nik    = row.get("NIK/NIP",   "Unknown")
            jabatan = row.get("JOB TITLE", "Unknown")
            logger.info(f"   - [DROP] Nama: {nama} | NIK: {nik} | Jabatan: {jabatan}")

    df_final = df.drop_duplicates(subset=subset_cols).reset_index(drop=True)
    cols_to_drop = [c for c in ["NAMA"] if c in df_final.columns]
    df_final = df_final.drop(columns=cols_to_drop)
    return df_final


def transform_riwayat_pekerjaan_from_raw(raw_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 1: Transform riwayat_pekerjaan dari sheet Raw Data saja.
    """
    col_map = get_cols(raw_data_df, _WANTED_PEKERJAAN_RAW, "Raw Data (pekerjaan)")
    df = select_and_rename(raw_data_df, col_map)
    df_final = _finalize_pekerjaan(df, "riwayat_pekerjaan (Raw)")
    df_final["status"] = "active"
    logger.info(f"[Transformer] riwayat_pekerjaan (Raw Data)  : {len(df_final)} baris")
    return df_final


def transform_riwayat_pekerjaan_from_assesment(assesment_df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2: Transform riwayat_pekerjaan dari sheet Assesment.
    Hanya karyawan yang BELUM memiliki record di tabel riwayat_pekerjaan DB
    (yaitu karyawan yang hanya ada di Assesment, tidak di Raw Data).
    Menggunakan 'Asessment Year' sebagai TANGGAL MASUK KERJA fallback.
    """
    # Ambil employee_id yang SUDAH ada di DB (di-load dari Raw Data di Phase 1)
    existing_pekerjaan_ids: set[str] = set()
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            with conn.cursor() as cur:
                cur.execute('SELECT DISTINCT employee_id FROM "riwayat_pekerjaan"')
                existing_pekerjaan_ids = {str(row[0]) for row in cur.fetchall()}
            conn.close()
        except Exception as e:
            logger.info(f"[Transformer] ⚠  Gagal query existing riwayat_pekerjaan: {e}")

    df_new = assesment_df[~assesment_df["employee_id"].isin(existing_pekerjaan_ids)].copy()

    if df_new.empty:
        logger.info("[Transformer] riwayat_pekerjaan (Assesment): 0 baris (semua sudah ada di DB)")
        return pd.DataFrame()

    WANTED_ASS = [
        "employee_id",
        "NAMA",
        "NIK/NIP",
        "SUB HOLDING",
        "SUB COMPANY",
        "JOB TITLE",
        "Asessment Year",
    ]
    col_map_ass = get_cols(df_new, WANTED_ASS, "Assesment (pekerjaan fallback)")
    df = select_and_rename(df_new, col_map_ass)
    df.rename(columns={
        "SUB HOLDING":   "SUBHOLDING",
        "SUB COMPANY":   "SUBCO",
        "Asessment Year": "TANGGAL MASUK KERJA",
    }, inplace=True)

    if "TANGGAL MASUK KERJA" in df.columns:
        df["TANGGAL MASUK KERJA"] = (
            df["TANGGAL MASUK KERJA"].astype(str).str.extract(r"(\d{4})")[0] + "-01-01"
        )

    df_final = _finalize_pekerjaan(df, "riwayat_pekerjaan (Assesment)")
    df_final["status"] = "active"
    logger.info(f"[Transformer] riwayat_pekerjaan (Assesment) : {len(df_final)} baris")
    return df_final


# ─────────────────────────────────────────────────────────
#  STEP 6 – Riwayat Assesment
# ─────────────────────────────────────────────────────────

def transform_riwayat_assesment(assesment_df: pd.DataFrame) -> pd.DataFrame:
    """
    Select employee_id + kolom assessment dari sheet Assesment.
    assesment_df sudah punya employee_id dari step enrich_assesment_with_employee_id.
    """
    WANTED = [
        "employee_id",
        "NAMA",
        "NIK/NIP",
        "Asessment Year",
        "Remarks", "AGE",
        "INT_INT", "EXC_INT", "COMP_INT", "HUM_INT", "ADM_INT",
        "PLC_INT", "MCH_INT", "FTW_INT", "EMP_INT",
        "PEOPLE", "ORGANIZATION", "BUSINESS",
        "PERSISTENT", "PERSEVERANCE", "PASSION", "GRIT",
        "Value Driven", "Leadership Comp", "Business Comp", "Technical Comp",
        "OVERALL_SKOR", "TALENT CLASS", "STRENGTH",
        "AREA OF DEVELOPMENT (GAP)", "IDP", "IQ", "DISC",
        "HAV", "HAV_TIA"
    ]
    col_map = get_cols(assesment_df, WANTED, "Assesment")
    df = select_and_rename(assesment_df, col_map)

    # --- LOGGING TRACKING BARIS ASSESMENT ---
    duplicates = df[df.duplicated(keep='first')]
    if not duplicates.empty:
        logger.info(f"[Transformer] riwayat_assesment: Ditemukan {len(duplicates)} baris duplikat yang akan dibuang:")
        for _, row in duplicates.iterrows():
            nama = row.get('NAMA', 'Unknown')
            nik = row.get('NIK/NIP', 'Unknown')
            tahun = row.get('Asessment Year', 'Unknown')
            logger.info(f"   - [DROP] Nama: {nama} | NIK: {nik} | Tahun: {tahun}")

    df_final = df.drop_duplicates().reset_index(drop=True)

    # Hapus kolom helper NAMA & NIK/NIP sebelum dikembalikan
    cols_to_drop = [c for c in ["NAMA", "NIK/NIP"] if c in df_final.columns]
    df_final = df_final.drop(columns=cols_to_drop)

    logger.info(f"[Transformer] riwayat_assesment         : {len(df_final)} baris")
    return df_final


# ─────────────────────────────────────────────────────────
#  STEP 7 – Individual Career Roadmap
# ─────────────────────────────────────────────────────────

ROADMAP_YEARS = [
    "road_map_2023", "road_map_2024", "road_map_2025",
    "road_map_2026", "road_map_2027", "road_map_2028",
    "road_map_2029", "road_map_2030", "road_map_2031",
    "road_map_2032",
]


def transform_individual_career_roadmap(assesment_df: pd.DataFrame) -> pd.DataFrame:
    """
    Select employee_id + kolom road_map_XXXX dari sheet Assesment.
    assesment_df sudah punya employee_id dari step enrich_with_employee_id.
    Pivot wide → long: employee_id | planned_position | year.
    """
    eid_col = normalize_col(assesment_df, "employee_id")
    if eid_col is None:
        logger.info("[Transformer] ⚠  Kolom 'employee_id' tidak ada di assesment_df, skip roadmap.")
        return pd.DataFrame(columns=["employee_id", "planned_position", "year"])

    roadmap_col_map: dict[str, str] = {}
    for rm in ROADMAP_YEARS:
        found = normalize_col(assesment_df, rm)
        if found:
            roadmap_col_map[rm] = found
        else:
            logger.info(f"[Transformer] ⚠  Kolom roadmap '{rm}' tidak ditemukan, dilewati.")

    if not roadmap_col_map:
        logger.info("[Transformer] Tidak ada kolom roadmap, mengembalikan DataFrame kosong.")
        return pd.DataFrame(columns=["employee_id", "planned_position", "year"])

    select_cols = [eid_col] + list(roadmap_col_map.values())
    df = assesment_df[select_cols].copy()
    df.rename(columns={v: k for k, v in roadmap_col_map.items()}, inplace=True)
    if eid_col != "employee_id":
        df.rename(columns={eid_col: "employee_id"}, inplace=True)

    # Wide → Long
    df_melted = df.melt(
        id_vars=["employee_id"],
        value_vars=list(roadmap_col_map.keys()),
        var_name="road_map_col",
        value_name="planned_position",
    )

    df_melted["year"] = df_melted["road_map_col"].str.extract(r"(\d{4})")
    df_melted.drop(columns=["road_map_col"], inplace=True)

    # Hapus baris dengan planned_position kosong
    df_melted = df_melted[df_melted["planned_position"].notna()]
    df_melted = df_melted[df_melted["planned_position"].astype(str).str.strip() != ""]

    df_melted = (
        df_melted[["employee_id", "planned_position", "year"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    logger.info(f"[Transformer] individual_career_roadmap : {len(df_melted)} baris")
    return df_melted
