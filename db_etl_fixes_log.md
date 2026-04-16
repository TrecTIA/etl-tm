# Log Perubahan & Perbaikan Database untuk Sistem ETL

Dokumen ini mencatat seluruh rekam jejak kendala (error) yang dialami saat proses integrasi sinkronisasi data Excel ke PostgreSQL (ETL), beserta langkah-langkah resolusi yang telah diaplikasikan di sisi Database PostgreSQL.

---

## 1. Otorisasi Trigger (Hak Akses Tabel)
* **Error**: `must be owner of table employees` / `current transaction is aborted`
* **Penyebab**: Script Python ETL memiliki instruksi untuk mematikan _trigger_ (`DISABLE TRIGGER trg_notify_employees`) sementara waktu selama sinkronisasi data agar sistem tidak dibanjiri notifikasi palsu. Namun, user `dbfufufafa` yang digunakan oleh program Python tidak memiliki level otorisasi sebagai Pemilik (Owner) dari tabel `employees`.
* **Solusi di Database**:
  * Memberikan privilese Owner kepada role `dbfufufafa` khusus untuk tabel `employees`:
    ```sql
    ALTER TABLE "employees" OWNER TO dbfufufafa;
    ```
  * _Atau_, menaikkan level `dbfufufafa` menjadi Superuser:
    ```sql
    ALTER USER dbfufufafa WITH SUPERUSER;
    ```

## 2. Penyesuaian Nullable Constraint (Toleransi Kolom Kosong)
* **Error**: `null value in column "tanggal_mulai" of relation "riwayat_pekerjaan" violates not-null constraint` dan error serupa pada kolom `remarks` di `riwayat_assesment`.
* **Penyebab**: Struktur tabel database sebelumnya mewajibkan (NOT NULL) kolom-kolom tersebut untuk selalu diisi. Padahal, pada kenyataannya di dalam file Excel sumber, banyak sel-sel _Tanggal Masuk Kerja_ maupun _Remarks Assesment_ yang dibiarkan kosong oleh Admin/HR. Database menolak keras menyimpan baris yang kolom wajibnya kosong.
* **Solusi di Database**:
  * Mengubah pengaturan kolom menjadi _Nullable_ (boleh kosong / NULL):
    ```sql
    ALTER TABLE "riwayat_pekerjaan" ALTER COLUMN "tanggal_mulai" DROP NOT NULL;
    ALTER TABLE "riwayat_assesment" ALTER COLUMN "remarks" DROP NOT NULL;
    ```

## 3. Pemasangan Tameng Kunci Unik (Unique Constraints untuk UPSERT)
* **Error**: `there is no unique or exclusion constraint matching the ON CONFLICT specification` dan juga error efek samping `duplicate key value violates unique constraint` saat UPSERT dimatikan.
* **Penyebab**: Proses ETL dirancang bersifat Idempotent / "Upsert" (Update or Insert) melalui klausa `ON CONFLICT` di Python. Jika Excel yang sama diupload 2x, ia tak akan membuat duplikat, melainkan hanya menimpa datanya. Agar PostgreSQL mengizinkan aksi `ON CONFLICT` ini, ia **mewajibkan** tabelnya dipasangi aturan resmi (Unique Constraint) mengenai kombinasi kolom apa saja yang menentukan kekembaran sebuah data. Sebelumnya, aturan kombinasi unik ini tidak ada di DB sehingga PostgreSQL kebingungan mendeteksi mana data yang berstatus kembar.
* **Solusi di Database**:
  * Membersihkan tabel dari data yang terlanjur kembar/kotor (akibat *Insert* murni sebelumnya):
    ```sql
    TRUNCATE TABLE "riwayat_pendidikan" CASCADE;
    TRUNCATE TABLE "riwayat_pekerjaan" CASCADE;
    -- Dsb untuk tabel lain yang terindikasi ada baris ganda
    ```
  * Menambahkan (ALTER) Konstrain Kombinasi Unik untuk tiap tabel pendukung:
    ```sql
    -- Tabel Pendidikan (Kombinasi 5 kolom)
    ALTER TABLE "riwayat_pendidikan"
    ADD CONSTRAINT "uniq_riwayat_pendidikan" 
    UNIQUE ("employee_id", "tingkat_pendidikan", "universitas", "jurusan", "tanggal_masuk");

    -- Tabel Pekerjaan (Kombinasi 5 Kolom - Opsi 1)
    ALTER TABLE "riwayat_pekerjaan"
    ADD CONSTRAINT "uniq_riwayat_pekerjaan" 
    UNIQUE (
        "employee_id", 
        "subholding_code", 
        "company_code", 
        "jabatan", 
        "tanggal_mulai"
    );

    -- Tabel Assesment (Kombinasi berdasarkan revisi aturan bisnis: Karyawan + Tahun)
    -- Asumsi di tabel sudah ada kolom "assesment_year"
    ALTER TABLE "riwayat_assesment"
    ADD CONSTRAINT "uniq_assesment_employee_year" 
    UNIQUE ("employee_id", "assesment_year");

    -- Tabel Career Roadmap (Sudah terpasang dari awal dengan nama uq_career_roadmap_employee_year)
    -- UNIQUE ("employee_id", "year");
    ```

## 4. Referensi Foreign Key (Sinkronisasi Data yang Tidak Konsisten)
* **Error**: `insert or update on table "riwayat_assesment" violates foreign key constraint "riwayat_assesment_employee_id_fkey" ... Key is not present in employees`
* **Penyebab**: Terjadi karena ketidakseragaman Master Data antara Sheet "Raw Data" dan "Assesment". Terdapat karyawan yang namanya **hanya** didaftarkan di Sheet Assesment (tidak ada di Raw Data). Padahal, aturan Foreign Key mewajibkan setiap ID Karyawan yang ada di tabel *Assesment* harus terlebih dahulu terdaftar di "Buku Induk" tabel `employees`. 
* **Solusi**: 
  * Diubah di sisi **Python Code**, di mana ETL sekarang dipaksa mengumpulkan semua NAMA unik tak terkecuali, baik dari Raw Data maupun Assesment, memberikan UUID yang seragam, dan mengekspor mereka semua sebagai Buku Induk ke tabel `employees`.
  * *Catatan Sampingan*: Untuk data karyawan yang *hanya* ada di Sheet Assesment, karena Sheet Assesment tidak mencatat Profil Identitas (Tanggal Lahir, Agama, dsb), otomatis data identitas pribadinya di Master Database PostgreSQL akan kosong/NULL kecuali untuk Subholding & Subcompany yang baru saja dipetakan.

---
### Kesimpulan
Secara garis besar, database PostgreSQL telah di-tuning ulang konfigurasinya dari yang tadinya sangat ketat, menjadi lebih fleksibel/toleran (terhadap baris kosong), sekaligus diberikan parameter integritas khusus (Unique Constraints) agar mendukung siklus *Re-upload/Upserting* yang lancar dalam pipelin ETL sehari-hari.
