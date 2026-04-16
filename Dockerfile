# ──────────────────────────────────────────────────────────────
# ETL HCMS v2 — Dockerfile
# Base: Python 3.11-slim (stabil, ringan)
# ──────────────────────────────────────────────────────────────

# ── Stage 1: Builder (install deps) ──────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build tools yang dibutuhkan psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements dulu (caching layer)
COPY requirements.txt .

# Install semua dependency ke folder /install
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime (image final, lebih kecil) ──────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Copy dependency yang sudah di-install dari builder
COPY --from=builder /install /usr/local

# Copy source code
COPY . .

# Buat folder input & output agar volume mount bekerja
RUN mkdir -p input output

# Expose port untuk FastAPI (mode API)
EXPOSE 8000

# Default CMD: jalankan FastAPI server
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
