"""Tulis hasil dari database ke CSV: cuaca_terbaru, ringkasan_lokasi, ekstrem_cuaca,
akurasi_prakiraan, revisi_cuaca, dan karantina.

Ambang hari hujan, hujan lebat, dan angin kencang ditetapkan project ini untuk keperluan
analisis, bukan standar resmi. Nilainya ada di konstanta di bagian atas file.
"""

import csv
import logging
from pathlib import Path

from .koneksi import buka
from .logging_setup import RUN_ID, timed

logger = logging.getLogger(__name__)

HARI_HUJAN_MM = 1
HUJAN_LEBAT_MM_JAM = 10
ANGIN_KENCANG_KMH = 50
JENDELA_HARI = 30

SQL_TERBARU = """
SELECT DISTINCT ON (l.nama)
       l.nama AS lokasi, l.provinsi, f.waktu_utc, f.tanggal_lokal, f.jam_lokal,
       f.suhu_c, f.kelembapan_persen, f.curah_hujan_mm, f.kecepatan_angin_kmh, f.tekanan_hpa,
       c.deskripsi AS kondisi
FROM fact_cuaca_jam f
JOIN dim_lokasi l ON l.lokasi_id = f.lokasi_id
LEFT JOIN dim_kode_cuaca c ON c.kode_cuaca = f.kode_cuaca
ORDER BY l.nama, f.waktu_utc DESC
"""

SQL_RINGKASAN = """
SELECT lokasi, provinsi,
       COUNT(*) AS jumlah_hari,
       ROUND(AVG(suhu_rata_c), 1) AS suhu_rata_c,
       MIN(suhu_min_c) AS suhu_min_c,
       MAX(suhu_maks_c) AS suhu_maks_c,
       ROUND(SUM(curah_hujan_total_mm), 1) AS curah_hujan_total_mm,
       COUNT(*) FILTER (WHERE curah_hujan_total_mm >= %s) AS hari_hujan,
       MAX(angin_maks_kmh) AS angin_maks_kmh
FROM mart_cuaca_harian
WHERE hari_lengkap
  AND tanggal > (SELECT MAX(tanggal_lokal) FROM fact_cuaca_jam) - %s
GROUP BY lokasi, provinsi
ORDER BY curah_hujan_total_mm DESC NULLS LAST, lokasi
"""

SQL_EKSTREM = """
SELECT l.nama AS lokasi, f.waktu_utc, f.tanggal_lokal, f.jam_lokal,
       f.curah_hujan_mm, f.kecepatan_angin_kmh, c.deskripsi AS kondisi,
       CASE WHEN f.curah_hujan_mm >= %s AND f.kecepatan_angin_kmh >= %s THEN 'hujan lebat dan angin kencang'
            WHEN f.curah_hujan_mm >= %s THEN 'hujan lebat'
            ELSE 'angin kencang' END AS jenis
FROM fact_cuaca_jam f
JOIN dim_lokasi l ON l.lokasi_id = f.lokasi_id
LEFT JOIN dim_kode_cuaca c ON c.kode_cuaca = f.kode_cuaca
WHERE (f.curah_hujan_mm >= %s OR f.kecepatan_angin_kmh >= %s)
  AND f.tanggal_lokal > (SELECT MAX(tanggal_lokal) FROM fact_cuaca_jam) - %s
ORDER BY f.curah_hujan_mm DESC NULLS LAST, f.kecepatan_angin_kmh DESC NULLS LAST, l.nama, f.waktu_utc
"""

SQL_AKURASI = """
SELECT lokasi, kelompok_jarak, jumlah_pasangan, mae_suhu_c, bias_suhu_c, mae_hujan_mm
FROM mart_akurasi_prakiraan
ORDER BY lokasi, kelompok_jarak
"""

SQL_REVISI = """
SELECT r.waktu_utc, l.nama AS lokasi, r.kolom, r.nilai_lama, r.nilai_baru,
       ROUND(r.nilai_baru - r.nilai_lama, 1) AS selisih
FROM cuaca_revisi r
JOIN dim_lokasi l ON l.lokasi_id = r.lokasi_id
WHERE r.run_id = %s
ORDER BY r.waktu_utc, l.nama, r.kolom
"""

SQL_KARANTINA = """
SELECT jenis, lokasi, waktu_utc, alasan, nilai_mentah
FROM karantina
WHERE run_id = %s
ORDER BY jenis, lokasi, waktu_utc
"""


class RingkasanError(Exception):
    """Penulisan output gagal dan pipeline berhenti."""


def _tulis_csv(path, cur):
    kolom = [k[0] for k in cur.description]
    baris = cur.fetchall()
    with path.open("w", newline="", encoding="utf-8") as f:
        penulis = csv.writer(f)
        penulis.writerow(kolom)
        penulis.writerows(baris)
    logger.info("Output ditulis: %s (%d baris)", path, len(baris))


@timed
def buat_ringkasan(db_name, folder_output):
    conn = None
    try:
        folder = Path(folder_output)
        folder.mkdir(parents=True, exist_ok=True)
        conn = buka(db_name)
        with conn.cursor() as cur:
            for nama, sql, params in (
                ("cuaca_terbaru", SQL_TERBARU, None),
                ("ringkasan_lokasi", SQL_RINGKASAN, (HARI_HUJAN_MM, JENDELA_HARI)),
                (
                    "ekstrem_cuaca", SQL_EKSTREM,
                    (HUJAN_LEBAT_MM_JAM, ANGIN_KENCANG_KMH, HUJAN_LEBAT_MM_JAM,
                     HUJAN_LEBAT_MM_JAM, ANGIN_KENCANG_KMH, JENDELA_HARI),
                ),
                ("akurasi_prakiraan", SQL_AKURASI, None),
                ("revisi_cuaca", SQL_REVISI, (RUN_ID,)),
                ("karantina", SQL_KARANTINA, (RUN_ID,)),
            ):
                cur.execute(sql, params)
                _tulis_csv(folder / f"{nama}.csv", cur)
    except Exception:
        logger.exception("Gagal membuat file output di %s", folder_output)
        raise RingkasanError("Gagal membuat file output") from None
    finally:
        if conn is not None:
            conn.close()
