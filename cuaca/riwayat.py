"""Riwayat run di riwayat_run.jsonl: satu baris JSON per run, dibaca oleh kesehatan.py."""

import json
from datetime import datetime, timezone
from pathlib import Path

NAMA_FILE = "riwayat_run.jsonl"
FORMAT_WAKTU = "%Y-%m-%dT%H:%M:%SZ"


def waktu_sekarang():
    return datetime.now(timezone.utc).strftime(FORMAT_WAKTU)


def catat_run(log_dir, data):
    """Tambah satu baris ke riwayat_run.jsonl."""
    path = Path(log_dir) / NAMA_FILE
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def baca_riwayat(log_dir):
    """Baca semua baris riwayat. Baris rusak dilewati, file tidak ada menghasilkan []."""
    path = Path(log_dir) / NAMA_FILE
    if not path.is_file():
        return []
    hasil = []
    with path.open(encoding="utf-8") as f:
        for baris in f:
            try:
                data = json.loads(baris)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict):
                hasil.append(data)
    return hasil
