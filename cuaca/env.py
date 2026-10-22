"""Memuat variabel dari file .env di akar project (jika ada) ke os.environ.

Variabel yang sudah ada di environment tidak ditimpa, jadi nilai dari shell, Docker Compose,
atau Airflow selalu diutamakan.
"""

from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[assignment]

AKAR_PROJECT = Path(__file__).resolve().parent.parent


def muat_env(path=None):
    """Hasilnya True jika ada variabel yang diisi dari file, False jika tidak (termasuk file tidak ada)."""
    if load_dotenv is None:
        return False
    return bool(load_dotenv(path or AKAR_PROJECT / ".env", override=False))
