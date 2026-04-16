import logging
import os
from datetime import datetime

def get_logger(name: str) -> logging.Logger:
    """Mengembalikan logger yang terkonfigurasi untuk output ke file 'etl_process.log' dan console."""
    logger = logging.getLogger(name)
    
    # Mencegah duplikasi handler jika dipanggil berkali-kali
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.DEBUG)

    # Buat format log standar
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. Output ke Console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # 2. Output ke File .log
    # Simpan di root project (naik 1 folder dari src)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_file_path = os.path.join(project_root, "etl_process.log")
    
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Tambahkan handler ke logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
