"""
loader/utils.py
---------------
Fungsi untuk menyimpan DataFrames hasil transformasi ke file CSV.
"""

import os
import pandas as pd


def save_to_csv(datasets: dict[str, pd.DataFrame], output_dir: str) -> dict[str, str]:
    """
    Menyimpan semua DataFrame ke file CSV di output_dir.

    Jika sebuah file sedang dibuka/dikunci oleh proses lain (Permission Denied),
    proses akan di-skip dengan pesan peringatan — program tidak akan crash.

    Args:
        datasets   : dict mapping nama_dataset → DataFrame
        output_dir : folder tujuan output (akan dibuat jika belum ada)

    Returns:
        dict mapping nama_dataset → path file CSV yang berhasil disimpan
    """
    os.makedirs(output_dir, exist_ok=True)
    saved_paths: dict[str, str] = {}
    skipped: list[str] = []

    print(f"\n[Loader] Menyimpan output ke: {output_dir}")

    for name, df in datasets.items():
        file_name = f"{name}.csv"
        file_path = os.path.join(output_dir, file_name)
        try:
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
            saved_paths[name] = file_path
            print(f"[Loader] ✓  {file_name:<40} ({len(df)} baris)")
        except PermissionError:
            print(
                f"[Loader] ⚠  {file_name:<40} GAGAL — file sedang terbuka di program lain. "
                "Tutup file tersebut lalu jalankan ulang."
            )
            skipped.append(file_name)

    if skipped:
        print(
            f"\n[Loader] ⚠  {len(skipped)} file dilewati karena sedang terbuka: "
            + ", ".join(skipped)
        )

    return saved_paths
