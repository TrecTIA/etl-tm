"""
api.py – FastAPI server untuk ETL HCMS v2
------------------------------------------
Opsi B: Menerima file Excel langsung dari Angular via multipart upload.
Tidak perlu Supabase Storage untuk proses ETL.

Jalankan:
    python -m uvicorn api:app --reload --port 8000
"""

import os
import time
import traceback
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

from src.extractor.main import run_from_bytes
from src.transformer.main import run_phase1, run_phase2
from src.loader.pg_utils import save_to_postgres

load_dotenv()

app = FastAPI(
    title="ETL HCMS v2 API",
    description="API untuk memproses file Excel langsung ke PostgreSQL",
    version="2.0.0",
)

# CORS — izinkan request dari Angular
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://localhost:4201",
        os.getenv("FRONTEND_URL", ""),
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ProcessResponse(BaseModel):
    success: bool
    message: str
    sheets_processed: list[str] = []
    rows_inserted: dict[str, int] = {}
    rows_updated: dict[str, int] = {}
    errors: list[str] = []
    duration_seconds: float = 0.0


@app.get("/health")
async def health_check():
    """Health check untuk Angular EtlUploadService."""
    return {"status": "ok", "service": "ETL HCMS v2 API"}


@app.post("/upload", response_model=ProcessResponse)
async def upload_and_process(file: UploadFile = File(...)):
    """
    Terima file Excel langsung dari Angular (multipart/form-data),
    lalu jalankan pipeline ETL → PostgreSQL.

    Pipeline:
        1. Baca file bytes dari request (tidak perlu Storage)
        2. Extract: baca sheet 'Raw Data' dan 'Assesment'
        3. Transform: jalankan semua transformasi
        4. Load: upsert ke PostgreSQL (ORDERED: employees first)
    
    Crisis Fix v2.1:
    - page_size di-turunkan 500→100 untuk stability
    - Insert order ketat (employees FIRST)
    - Jika employees gagal, semua stop
    """
    # Validasi tipe file
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Hanya file .xlsx yang didukung")

    start_time = time.time()
    errors: list[str] = []

    print(f"\n{'='*60}")
    print(f"  ETL HCMS v2 — Direct Upload Mode (v2.1 Crisis Fix)")
    print(f"  File: {file.filename} ({file.size} bytes)" if file.size else f"  File: {file.filename}")
    print(f"{'='*60}")

    try:
        # 1. Baca bytes langsung dari request (tanpa Storage)
        file_bytes = await file.read()
        print(f"[API] File diterima: {len(file_bytes):,} bytes")

        # 2. Extract
        data = run_from_bytes(file_bytes)

        # 3a. Transform Phase 1: Raw Data → employees, riwayat_pendidikan, riwayat_pekerjaan
        datasets_p1, preprocessed_ass = run_phase1(data["raw_data"], data["assesment"])

        # 4a. Load Phase 1 → PostgreSQL
        load_results_p1 = save_to_postgres(datasets_p1)

        # Cek apakah employees Phase 1 berhasil sebelum lanjut ke Phase 2
        employees_ok = load_results_p1.get("employees", {}).get("success", False)
        if not employees_ok:
            load_results = load_results_p1
        else:
            # 3b. Transform Phase 2: Assesment → employees (baru), riwayat_pekerjaan (supplement),
            #                                     riwayat_assesment, individual_career_roadmap
            datasets_p2 = run_phase2(preprocessed_ass)

            # 4b. Load Phase 2 → PostgreSQL
            load_results_p2 = save_to_postgres(datasets_p2)

            # Gabungkan hasil: Phase 2 bisa override Phase 1 untuk dataset yang sama (employees, riwayat_pekerjaan)
            # tapi rows_upserted dijumlahkan agar total tetap akurat
            load_results = {}
            all_keys = set(load_results_p1) | set(load_results_p2)
            for k in all_keys:
                r1 = load_results_p1.get(k, {})
                r2 = load_results_p2.get(k, {})
                if r1 and r2:
                    load_results[k] = {
                        "rows_upserted": r1.get("rows_upserted", 0) + r2.get("rows_upserted", 0),
                        "success": r1.get("success", False) or r2.get("success", False),
                    }
                else:
                    load_results[k] = r1 or r2

        # Susun response
        sheets_processed = []
        rows_inserted = {}
        rows_updated = {}
        employees_failed = False

        for name, result in load_results.items():
            if result.get("success", False) and result.get("rows_upserted", 0) > 0:
                sheets_processed.append(name)
                rows_inserted[name] = result["rows_upserted"]
            elif result.get("error"):
                errors.append(f"[{name}] {result['error']}")
                if name == "employees":
                    employees_failed = True

        duration = round(time.time() - start_time, 2)
        
        # SUCCESS kondisi:
        # - employees OK
        # - minimal 1 dataset berhasil
        success = (not employees_failed) and len(sheets_processed) > 0
        total_rows = sum(rows_inserted.values())

        if success:
            message = (
                f"✓ ETL BERHASIL. {len(sheets_processed)} dataset diproses, {total_rows} rows di-upsert. "
                f"(dalam {duration}s)"
            )
        elif employees_failed:
            message = (
                f"❌ ETL GAGAL: Master table 'employees' tidak berhasil diinsert. "
                f"Semua dataset terkait DIBATALKAN. Cek constraint DB atau validasi data Excel."
            )
        else:
            message = (
                f"⚠ ETL PARTIALLY FAILED. "
                f"{len(sheets_processed)} dari {len(datasets)} dataset berhasil. "
                f"{total_rows} total rows di-upsert. Cek error details."
            )

        return ProcessResponse(
            success=success,
            message=message,
            sheets_processed=sheets_processed,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            errors=errors,
            duration_seconds=duration,
        )

    except Exception as e:
        traceback.print_exc()
        duration = round(time.time() - start_time, 2)
        return ProcessResponse(
            success=False,
            message=f"❌ ETL GAGAL: {str(e)}",
            errors=[str(e)],
            duration_seconds=duration,
        )
