"""
loader/main.py
--------------
Menjalankan proses pemuatan (load) DataFrame ke file CSV.
Bisa dipanggil sebagai fungsi oleh orchestrator (main.py utama)
atau dijalankan langsung sebagai standalone script.

Standalone usage:
    python -m src.loader.main  (tidak berguna tanpa data; gunakan via orchestrator)
"""

import sys
import os
import pandas as pd

from .utils import save_to_csv


def run(
    datasets: dict[str, pd.DataFrame],
    output_dir: str,
) -> dict[str, str]:
    """
    Menjalankan proses load: simpan semua DataFrame ke CSV.

    Args:
        datasets   : dict mapping nama_dataset → DataFrame
        output_dir : folder tujuan output

    Returns:
        dict mapping nama_dataset → path file CSV yang disimpan
    """
    print("\n[Loader] ── Mulai loading ──")
    saved_paths = save_to_csv(datasets, output_dir)
    print("[Loader] ── Loading selesai ──\n")
    return saved_paths


# ── Standalone entry point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print(
        "Loader standalone: jalankan melalui orchestrator (main.py utama) "
        "atau lihat src/loader/utils.py → save_to_csv()."
    )
    sys.exit(0)
