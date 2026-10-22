"""DAG harian cuaca: jalankan ETL, lalu cek kesehatan.

Jadwal 06:00 WIB (zona waktu ditentukan lewat start_date, bukan UTC bawaan Airflow).
Tiap run mengambil data aktual 7 hari terakhir dan prakiraan 3 hari ke depan. Rentang
data aktual yang tumpang tindih antar run sengaja dibiarkan: data sumber bisa direvisi
dan terlambat tersedia, dan karena load bersifat idempotent, run ulang tidak menggandakan
data dan hari yang terlewat terisi sendiri di run berikutnya.

Task etl memanggil "python -m cuaca" sebagai proses terpisah supaya logging Airflow
tidak tertimpa dan riwayat run (bahan cek_kesehatan) tertulis di LOG_DIR. Cara
menjalankan ada di README.md.
"""

import os
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

REPO = os.getenv("CUACA_REPO", "/opt/cuaca")
LOG_DIR = os.getenv("LOG_DIR", "/opt/airflow/logs/cuaca")
FOLDER_OUTPUT = os.getenv("FOLDER_OUTPUT", "/opt/airflow/data/output")
FOLDER_MENTAH = os.getenv("FOLDER_MENTAH", "/opt/airflow/data/mentah")


def _cek_kesehatan():
    import sys

    sys.path.insert(0, REPO)
    from cuaca.kesehatan import periksa
    from cuaca.riwayat import baca_riwayat

    hasil = periksa(baca_riwayat(LOG_DIR), datetime.now(timezone.utc))
    for level, pesan in hasil:
        print(f"[{level}] {pesan}")
    kritis = [pesan for level, pesan in hasil if level == "CRITICAL"]
    if kritis:
        raise AirflowException("kesehatan CRITICAL: " + "; ".join(kritis))


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
}

with DAG(
    dag_id="cuaca_harian",
    description="ETL cuaca per jam dari Open-Meteo ke PostgreSQL, plus cek kesehatan",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "cuaca"],
) as dag:
    etl = BashOperator(
        task_id="etl",
        bash_command=(
            f"cd {REPO} && python -m cuaca "
            f"--output {FOLDER_OUTPUT} --mentah {FOLDER_MENTAH}"
        ),
        env={**os.environ, "LOG_DIR": LOG_DIR},
        append_env=False,
    )
    cek = PythonOperator(task_id="cek_kesehatan", python_callable=_cek_kesehatan)
    etl >> cek
