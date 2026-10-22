"""Koneksi PostgreSQL dari PGHOST, PGPORT, PGDATABASE, PGUSER, dan PGPASSWORD.

Nilai bawaannya cocok dengan docker-compose.yml.
"""

import os

try:
    import psycopg2

    ErrorDB = psycopg2.Error
except ImportError:
    psycopg2 = None  # type: ignore[assignment]

    class ErrorDB(Exception):  # type: ignore[no-redef]
        """Pengganti psycopg2.Error jika driver tidak ada."""


def buka(db_name=None):
    """Buka koneksi baru. db_name kosong berarti pakai PGDATABASE."""
    if psycopg2 is None:
        raise ErrorDB("psycopg2 belum terpasang: pip install psycopg2-binary")
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=db_name or os.getenv("PGDATABASE", "cuaca"),
        user=os.getenv("PGUSER", "cuaca"),
        password=os.getenv("PGPASSWORD", "cuaca"),
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "10")),
    )
