Read [](file:///c%3A/Temp/REPO/Dashboar_TREC/ETL%20HCMS%20v2/src/extractor/main.py)

Read [](file:///c%3A/Temp/REPO/Dashboar_TREC/ETL%20HCMS%20v2/src/loader/pg_utils.py#270-270), lines 270 to 365

Read [](file:///c%3A/Temp/REPO/Dashboar_TREC/ETL%20HCMS%20v2/src/loader/pg_utils.py#145-145), lines 145 to 170

Read [](file:///c%3A/Temp/REPO/Dashboar_TREC/ETL%20HCMS%20v2/src/extractor/utils.py#1-1), lines 1 to 80

---

## Alur ETL HCMS v2 — Penjelasan Detail

---

### Gambaran Besar

```
Angular (Frontend)
      │  POST /upload  multipart .xlsx
      ▼
   api.py  (FastAPI)
      │
      ├─► EXTRACT  (baca file Excel)
      │
      ├─► PHASE 1 TRANSFORM  (Raw Data)
      ├─► PHASE 1 LOAD       (→ PostgreSQL)
      │        ▼ (hanya jika Phase 1 sukses)
      ├─► PHASE 2 TRANSFORM  (Assesment)
      └─► PHASE 2 LOAD       (→ PostgreSQL)
```

---

## 1. EXTRACT — extractor

**File:** main.py, utils.py

1. api.py membaca bytes file dari request HTTP (`await file.read()`)
2. `run_from_bytes(file_bytes)` dipanggil — file tidak pernah ditulis ke disk
3. `pd.ExcelFile` membuka file dari memory (`io.BytesIO`)
4. Sheet dicari secara **case-insensitive**: `Raw Data` dan `Assesment`/`Assessment`
5. Kedua sheet dibaca dengan `dtype=str` (semua kolom tetap sebagai string, tidak ada konversi otomatis)
6. `_clean_dataframe()` dijalankan: strip whitespace pada nama kolom, buang baris yang **seluruh** kolomnya kosong
7. Hasil: `{"raw_data": DataFrame, "assesment": DataFrame}`

---

## 2. PHASE 1 TRANSFORM — `run_phase1(raw_data_df, assesment_df)`

**File:** main.py, utils.py

### Step 0 — Normalisasi JENIS KELAMIN (hanya Raw Data)
Kolom `JENIS KELAMIN` di-uppercase lalu di-map:
| Excel | → DB |
|---|---|
| LAKI-LAKI, L, M | → `Male` |
| PEREMPUAN, P, F | → `Female` |
| Tidak dikenali / kosong | → `Male` (default) |

### Step 1 — Isi NIK/NIP kosong (KEDUA sheet sekaligus)
- Kumpulkan semua NAMA yang NIK/NIP-nya kosong dari **Raw Data + Assesment**
- Generate ID format `unk-5001`, `unk-5002`, ... secara konsisten per-NAMA
- NAMA yang sama di kedua sheet mendapat ID yang **sama**
- Ini dilakukan di Phase 1 agar ID `unk-xxxx` konsisten sebelum Assesment diproses di Phase 2

### Step 2 — DB Lookup untuk Raw Data
- Query: `SELECT fullname, employee_id FROM employees`
- Buat dictionary `{nama: employee_id}` dari DB
- Setiap NAMA di Raw Data:
  - **Match di DB** → pakai `employee_id` yang sudah ada (integer/UUID dari DB)
  - **Tidak match** → generate UUID baru (karyawan baru)
- Kolom `employee_id` ditempelkan ke `raw_data_df`

### Step 3 — Transform 3 Dataset dari Raw Data

**`employees` (dari Raw Data):**
- Ambil kolom: `employee_id`, `NIK/NIP`, `NAMA`, `TANGGAL LAHIR`, `AGAMA`, `JENIS KELAMIN`, `SUBHOLDING`, `SUBCO`, `JOB TITLE`, `FUNCTION`
- Sort by `TAHUN` descending → drop `TAHUN`
- `groupby("employee_id").first()` → satu baris per karyawan, ambil data terbaru

**`riwayat_pendidikan` (dari Raw Data):**
- Ambil kolom: `employee_id`, `TINGKAT PENDIDIKAN`, `UNIVERSITAS`, `JURUSAN`, `TANGGAL MASUK PENDIDIKAN`, `TANGGAL KELUAR PENDIDIKAN`
- Buang baris yang semua kolom pendidikannya kosong
- `drop_duplicates()` untuk buang baris kembar identik

**`riwayat_pekerjaan` (dari Raw Data):**
- Ambil kolom: `employee_id`, `NIK/NIP`, `SUBHOLDING`, `SUBCO`, `DIVISI`, `DEPARTMENT`, `LEVEL`, `JOB TITLE`, `FUNCTION`, `GOL`, `KJ`, `TANGGAL MASUK KERJA`, `TANGGAL RESIGN/MUTASI`
- `JOB TITLE` kosong → diisi `"Unknown"`
- Dedup by: `[employee_id, SUBHOLDING, SUBCO, JOB TITLE, TANGGAL MASUK KERJA]`
- Hapus kolom helper `NAMA`

---

## 3. PHASE 1 LOAD — `save_to_postgres(datasets_p1)`

**File:** pg_utils.py

Insert dilakukan dengan urutan ketat karena Foreign Key:

```
1. employees          ← MASTER TABLE (harus sukses pertama)
2. riwayat_pekerjaan  ← FK ke employees
3. riwayat_pendidikan ← FK ke employees
```

Setiap tabel pakai **koneksi + transaksi terpisah** → isolasi error per-tabel.

Mekanisme upsert:
```sql
INSERT INTO "employees" (...) VALUES %s
ON CONFLICT (employee_id) DO UPDATE SET col = EXCLUDED.col, ...
```

Tabel `employees` memiliki trigger `trg_notify_employees` yang **dimatikan sementara** selama batch insert untuk performa, lalu dinyalakan kembali di blok `finally`.

**Jika `employees` gagal** → semua dataset lain di-skip, Phase 2 tidak dijalankan.

---

## 4. PHASE 2 TRANSFORM — `run_phase2(assesment_df)`

**File:** main.py

### Step 2 (ulang) — DB Lookup FRESH untuk Assesment
- Query DB **ulang** (bukan cache Phase 1) → kini sudah mencakup karyawan yang baru di-insert di Phase 1
- Match NAMA dari Assesment ke `fullname` di DB:
  - **Match** → pakai `employee_id` dari DB (termasuk yang baru dari Phase 1)
  - **Tidak match** → UUID baru (karyawan eksklusif di Assesment saja)

### Step 3 — Transform 4 Dataset dari Assesment

**`employees` (hanya yang baru):**
- Query DB: ambil semua `employee_id` yang sudah ada
- Filter: hanya baris yang `employee_id`-nya **belum** ada di DB
- Hasil: data karyawan yang hanya ada di sheet Assesment, tidak ada di Raw Data
- Data dari Raw Data **tidak tertimpa**

**`riwayat_pekerjaan` (supplement):**
- Query DB: ambil `employee_id` yang sudah ada di tabel `riwayat_pekerjaan`
- Filter: hanya baris karyawan yang **belum** punya record pekerjaan
- Gunakan `Asessment Year` sebagai `TANGGAL MASUK KERJA` (format `YYYY-01-01`)

**`riwayat_assesment` (semua):**
- Ambil semua kolom skor assessment: `INT_INT`, `EXC_INT`, `COMP_INT`, ..., `TALENT CLASS`, `STRENGTH`, `IQ`, `DISC`, `HAV`, dll.
- Buang duplikat by semua kolom
- Hapus kolom helper `NAMA`, `NIK/NIP` sebelum dikirim ke loader

**`individual_career_roadmap` (semua):**
- Ambil kolom `road_map_2023` s.d. `road_map_2032`
- Pivot **wide → long**: satu baris per `(employee_id, year)`
- Kolom `planned_position` dari value kolom roadmap
- Buang baris dengan `planned_position` kosong

---

## 5. PHASE 2 LOAD — `save_to_postgres(datasets_p2)`

Urutan insert:
```
1. employees               ← karyawan baru dari Assesment
2. riwayat_pekerjaan       ← supplement dari Assesment
3. riwayat_assesment       ← skor assessment
4. individual_career_roadmap ← rencana karir
```

---

## 6. Response ke Angular

```json
{
  "success": true,
  "message": "✓ ETL BERHASIL. 5 dataset diproses, 1234 rows di-upsert. (dalam 3.2s)",
  "sheets_processed": ["employees", "riwayat_pendidikan", "riwayat_pekerjaan", "riwayat_assesment", "individual_career_roadmap"],
  "rows_inserted": { "employees": 402, "riwayat_pendidikan": 394, ... },
  "errors": [],
  "duration_seconds": 3.2
}
```

`rows_inserted` untuk dataset yang muncul di **kedua** phase (employees, riwayat_pekerjaan) dijumlahkan dari Phase 1 + Phase 2.

---

## Ringkasan Dependency

```
Excel File
 ├── Sheet "Raw Data"
 │     ├─ Phase 1 Transform
 │     │     ├─ employees          → DB [master]
 │     │     ├─ riwayat_pendidikan → DB [FK employees]
 │     │     └─ riwayat_pekerjaan  → DB [FK employees]
 │     │              ↓ (DB fresh query)
 └── Sheet "Assesment"
       └─ Phase 2 Transform
             ├─ employees (baru)          → DB
             ├─ riwayat_pekerjaan (supp.) → DB
             ├─ riwayat_assesment         → DB [FK employees]
             └─ individual_career_roadmap → DB [FK employees]
```