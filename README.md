# ETL HCMS v2

Pipeline ETL untuk memproses data Excel HCMS → PostgreSQL.
Tersedia dalam dua mode: **API Server** (menerima upload dari Angular) dan **Script Batch** (jalankan langsung dari file lokal).

> **Satu `docker-compose.yml` untuk local dan production** — image Docker yang dihasilkan identik di mana pun dijalankan.

---

## Struktur File

```
ETL HCMS v2/
├── Dockerfile              ← build image Python 3.11-slim
├── docker-compose.yml      ← sama untuk local & VPS
├── nginx.conf.example      ← template Nginx (khusus VPS, di luar Docker)
├── .env                    ← konfigurasi rahasia (JANGAN di-commit ke Git)
├── .env.example            ← template .env
├── .dockerignore
├── api.py                  ← FastAPI server (Mode API)
├── main.py                 ← ETL pipeline (Mode Script)
├── requirements.txt
├── input/                  ← taruh file .xlsx di sini
├── output/                 ← hasil CSV muncul di sini
└── src/
    ├── extractor/
    ├── transformer/
    └── loader/
```

---

## Langkah 0 — Konfigurasi `.env`

Wajib dilakukan sebelum menjalankan apapun, baik di local maupun VPS.

```bash
# Windows
copy .env.example .env

# Linux / Mac
cp .env.example .env
```

Edit nilai berikut di `.env`:

```env
# PostgreSQL — koneksi ke database target
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME

# Supabase — untuk storage (dipakai mode API)
SUPABASE_URL=https://trec-api.triputra-group.com
SUPABASE_SERVICE_KEY=your_service_role_key_here

# CORS — URL frontend Angular
FRONTEND_URL=http://localhost:4200
```

---

## 🖥️ LOCAL (Windows)

### Prasyarat
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) terinstall dan berjalan

### Mode A — API Server

Jalankan FastAPI server yang menerima upload file Excel dari Angular.

```powershell
cd "c:\Temp\REPO\Dashboar_TREC\ETL HCMS v2"

# Build image & jalankan di background
docker compose up -d --build
```

| URL | Fungsi |
|-----|--------|
| `http://localhost:8010/health` | Cek status server |
| `http://localhost:8010/docs` | Swagger UI / dokumentasi API |

```powershell
# Lihat log real-time
docker compose logs -f

# Stop
docker compose down
```

### Mode B — Script Batch

Jalankan ETL pipeline satu kali dari file `.xlsx` lokal.

```powershell
# 1. Taruh file Excel ke folder input/

# 2. Jalankan (otomatis pilih file jika hanya ada 1)
docker compose run --rm etl-script

# Atau tentukan file secara eksplisit
docker compose run --rm etl-script input/data_hcms.xlsx
```

> File CSV hasil ETL akan muncul di folder `output/` di Windows.

### Rebuild (setelah ubah requirements.txt atau kode)

```powershell
docker compose build --no-cache
docker compose up -d
```

---

## 🚀 PRODUCTION (VPS Linux)

Image Docker yang di-build **sama persis** dengan local. Yang berbeda hanya:
1. Cara upload kode ke VPS
2. Nginx sebagai pintu masuk dari internet (reverse proxy + HTTPS)

### Prasyarat
- VPS dengan Ubuntu/Debian
- Docker Engine terinstall
- (Opsional) Domain yang sudah diarahkan ke IP VPS

### Langkah 1 — Install Docker di VPS

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# Logout & login ulang agar group docker aktif
exit
```

### Langkah 2 — Upload Kode ke VPS

**Via Git (direkomendasikan):**
```bash
# Di VPS
git clone https://github.com/your-org/your-repo.git /opt/etl-hcms
cd /opt/etl-hcms
```

**Via SCP dari Windows:**
```powershell
scp -r "c:\Temp\REPO\Dashboar_TREC\ETL HCMS v2" user@IP_VPS:/opt/etl-hcms
```

### Langkah 3 — Buat `.env` di VPS

```bash
cd /opt/etl-hcms
cp .env.example .env
nano .env   # isi DATABASE_URL production, FRONTEND_URL production
```

### Langkah 4 — Jalankan Container

```bash
cd /opt/etl-hcms

# Perintah SAMA dengan local
docker compose up -d --build

# Cek status
docker compose ps

# Lihat log
docker compose logs -f
```

Setelah ini, API berjalan di `http://IP_VPS:8010`.
Jika tidak perlu domain/HTTPS, cukup sampai di sini — buka port 8010 di firewall VPS.

### Langkah 5 — Nginx + HTTPS (Opsional, untuk Domain)

Setup ini di luar Docker — Nginx hanya bertindak sebagai pintu masuk dari internet.

```bash
# Install Nginx
sudo apt update && sudo apt install nginx -y

# Copy template config
sudo cp /opt/etl-hcms/nginx.conf.example /etc/nginx/sites-available/etl-hcms

# Edit: ganti server_name dengan domain kamu
sudo nano /etc/nginx/sites-available/etl-hcms

# Aktifkan & reload
sudo ln -s /etc/nginx/sites-available/etl-hcms /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

```bash
# SSL gratis dengan Certbot
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d etl.yourdomain.com
```

Setelah selesai → API bisa diakses di `https://etl.yourdomain.com`

### Update Kode di VPS

```bash
cd /opt/etl-hcms
git pull                              # ambil perubahan terbaru
docker compose up -d --build          # rebuild & restart (perintah sama)
```

---

## Diagram Alur

```
LOCAL                              VPS
─────                              ───────────────────────────────
Angular → localhost:8010           Internet
(langsung ke container)            → Nginx :80/:443 (SSL)
                                       ↓
                                   127.0.0.1:8010
                                   (container, sama dengan local)
                                       ↓
                                   PostgreSQL
```

---

## Perintah Docker Berguna

```bash
docker compose ps                    # lihat status container
docker compose logs -f               # lihat log real-time
docker compose down                  # stop & hapus container
docker compose exec etl-api bash     # masuk ke dalam container
docker compose build --no-cache      # rebuild image dari awal
docker compose restart etl-api       # restart container tanpa down
```
