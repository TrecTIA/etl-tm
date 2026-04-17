# Panduan Testing ETL HCMS v2

## 1. Persiapan Data Test

File test tersedia di folder `input/`:
- `input/test_raw_data.csv` — data dummy sheet Raw Data (10 baris, separator `;`)
- `input/test_assesment.csv` — data dummy sheet Assesment (7 baris, separator `;`)

Semua nama karyawan test diakhiri dengan **" Test"** (spasi + Test) agar mudah diidentifikasi dan dihapus tanpa risiko menimpa data asli.

---

## 2. Cara Upload ke ETL

Konversi CSV ke Excel `.xlsx` sesuai format template (2 sheet: `Raw Data` dan `Assesment`), lalu upload via endpoint `POST /upload`.

---

## 3. Verifikasi Data Masuk (Query DB)

### 3a. Ringkasan Jumlah Per Tabel
```sql
SELECT 'employees'                AS tabel, COUNT(*) FROM employees                WHERE fullname ILIKE '% test'
UNION ALL
SELECT 'riwayat_pekerjaan',         COUNT(*) FROM riwayat_pekerjaan       WHERE employee_id IN (SELECT employee_id FROM employees WHERE fullname ILIKE '% test')
UNION ALL
SELECT 'riwayat_pendidikan',        COUNT(*) FROM riwayat_pendidikan      WHERE employee_id IN (SELECT employee_id FROM employees WHERE fullname ILIKE '% test')
UNION ALL
SELECT 'riwayat_assesment',         COUNT(*) FROM riwayat_assesment       WHERE employee_id IN (SELECT employee_id FROM employees WHERE fullname ILIKE '% test')
UNION ALL
SELECT 'individual_career_roadmap', COUNT(*) FROM individual_career_roadmap WHERE employee_id IN (SELECT employee_id FROM employees WHERE fullname ILIKE '% test');
```

**Hasil yang diharapkan:**
| Tabel | Count |
|---|---|
| employees | 11 |
| riwayat_pekerjaan | 9 |
| riwayat_pendidikan | 7 |
| riwayat_assesment | 6 |
| individual_career_roadmap | ≥10 |

---

### 3b. Cek Employee yang Datanya Tidak Lengkap
```sql
-- Harusnya Guntur Test (pendidikan kosong) + karyawan yang hanya ada di Assesment (Sari, Dimas, Putri)
-- Sari Test, Dimas Prasetyo Nugraha Test, Putri Handayani Test tidak punya Raw Data → tidak ada riwayat_pendidikan
SELECT e.fullname FROM employees e
WHERE e.fullname ILIKE '% test'
  AND e.employee_id NOT IN (SELECT employee_id FROM riwayat_pendidikan);

-- Harusnya 5 orang: Hendra, Agus, Nur Fadilah (tidak ada di Assesment sheet)
--                   + Siti Aminah Budi, Guntur (tidak ada di Assesment sheet)
SELECT e.fullname FROM employees e
WHERE e.fullname ILIKE '% test'
  AND e.employee_id NOT IN (SELECT employee_id FROM riwayat_assesment);

-- Harusnya 5 orang yang sama: Hendra, Agus, Nur Fadilah, Siti Aminah Budi, Guntur
SELECT e.fullname FROM employees e
WHERE e.fullname ILIKE '% test'
  AND e.employee_id NOT IN (SELECT employee_id FROM individual_career_roadmap);
```

---

### 3c. Cek Transformasi Berjalan Benar
```sql
-- Gender WANITA → harus 'Male' (Nur Fadilah Test)
-- Gender P/PEREMPUAN → harus 'Female' (Siti Aminah, Dewi, Rina)
SELECT fullname, gender FROM employees WHERE fullname ILIKE '% test';

-- JOB TITLE kosong → harus 'Unknown' di riwayat_pekerjaan (Agus Wijaya Test)
SELECT e.fullname, rp.jabatan
FROM riwayat_pekerjaan rp
JOIN employees e ON e.employee_id = rp.employee_id
WHERE e.fullname ILIKE 'Agus Wijaya%';

-- NIK kosong → harus dapat unk- ID (Siti, Guntur, Dimas, Sari)
SELECT e.fullname, rp.employee_no_subholding
FROM riwayat_pekerjaan rp
JOIN employees e ON e.employee_id = rp.employee_id
WHERE e.fullname ILIKE '% test'
  AND rp.employee_no_subholding LIKE 'unk-%';

-- Status Hendra → harus 'inactive'
SELECT e.fullname, rp.status
FROM riwayat_pekerjaan rp
JOIN employees e ON e.employee_id = rp.employee_id
WHERE e.fullname ILIKE 'Hendra Gunawan%';
```

---

### 3d. Cek Tidak Ada Duplikat di employees
```sql
-- Harusnya 0 baris (EMP001 di Excel ada 3 baris, tapi di DB harus 1)
SELECT fullname, COUNT(*)
FROM employees
WHERE fullname ILIKE '% test'
GROUP BY fullname
HAVING COUNT(*) > 1;
```

---

## 4. Hapus Data Test (Cleanup)

> ⚠️ Jalankan **satu per satu** dari atas ke bawah.

```sql
-- Preview dulu sebelum hapus
SELECT employee_id, fullname FROM employees WHERE fullname ILIKE '% test';

-- 1. Tabel dependent dulu
DELETE FROM individual_career_roadmap
WHERE employee_id IN (SELECT employee_id FROM employees WHERE fullname ILIKE '% test');

DELETE FROM riwayat_assesment
WHERE employee_id IN (SELECT employee_id FROM employees WHERE fullname ILIKE '% test');

DELETE FROM riwayat_pendidikan
WHERE employee_id IN (SELECT employee_id FROM employees WHERE fullname ILIKE '% test');

DELETE FROM riwayat_pekerjaan
WHERE employee_id IN (SELECT employee_id FROM employees WHERE fullname ILIKE '% test');

-- 2. Terakhir tabel master
DELETE FROM employees
WHERE fullname ILIKE '% test';
```

> **Kenapa urutan ini?** Tabel dependent punya FK ke `employees` dengan `NO ACTION`, sehingga `employees` tidak bisa dihapus sebelum semua tabel yang mereferensinya dibersihkan terlebih dahulu.

---

## 5. Edge Cases yang Dicakup Data Test

| Case | Data | Yang Diuji |
|---|---|---|
| Gender tidak dikenal | `WANITA` (Nur Fadilah) | Default ke `Male` |
| Gender singkatan | `P`, `L` (Siti, Guntur) | Normalisasi ke `Female`/`Male` |
| NIK kosong | Siti, Guntur, Dimas, Sari | Generate `unk-` ID |
| Nama 1 kata + NIK kosong | Guntur Test, Sari Test | Skip name-matching, tetap dapat `unk-` |
| JOB TITLE kosong | Agus Wijaya Test | Diisi `Unknown` |
| TANGGAL RESIGN diisi | Hendra Gunawan Test | `status = inactive` |
| Duplikat identik | EMP001 row 1 = row 10 | Hanya 1 baris masuk DB |
| EMP001 beda tahun | 2024 Junior, 2025 Senior | Ambil TAHUN terbesar (Senior) |
| Cross-sheet matching | EMP005 Rina di Raw + Assesment | employee_id sama di kedua sheet |
| Karyawan baru dari Assesment | EMP_NEW001 Putri | Masuk employees + riwayat dari Assesment |
