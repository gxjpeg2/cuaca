"""Tulis data cuaca ke PostgreSQL secara idempotent: menjalankan ulang tidak menggandakan data.

Model datanya terdiri dari tabel dimensi (dim_lokasi, dim_tanggal, dim_kode_cuaca) dan dua tabel fakta.

fact_cuaca_jam berisi data aktual, satu baris per kota per jam, dengan kunci (lokasi_id, waktu_utc).
Jam baru ditambahkan, jam yang angkanya direvisi sumber diperbarui dan dicatat di cuaca_revisi,
dan jam yang sama dibiarkan. Nilai kosong dari sumber tidak pernah menimpa nilai yang sudah tersimpan.

fact_prakiraan_jam berisi prakiraan, satu baris per kota per jam target per waktu prediksi.
Tiap run menambah satu potret baru dan tidak menimpa potret lama, supaya akurasi prakiraan
bisa dihitung. Run kedua di jam yang sama tidak menggandakan apa pun.
"""

import logging

from .koneksi import ErrorDB, buka
from .konfigurasi import KODE_CUACA, KOLOM_UKUR
from .logging_setup import RUN_ID, timed

logger = logging.getLogger(__name__)

NAMA_BULAN = "ARRAY['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember']"
NAMA_HARI = "ARRAY['Senin','Selasa','Rabu','Kamis','Jumat','Sabtu','Minggu']"

SKEMA = [
    """
    CREATE TABLE IF NOT EXISTS dim_kode_cuaca (
        kode_cuaca SMALLINT PRIMARY KEY,
        deskripsi TEXT NOT NULL,
        kategori TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_lokasi (
        lokasi_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        nama TEXT NOT NULL UNIQUE,
        provinsi TEXT NOT NULL,
        lintang NUMERIC(8,5) NOT NULL,
        bujur NUMERIC(8,5) NOT NULL,
        elevasi_m NUMERIC(7,1),
        zona_waktu TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_tanggal (
        tanggal DATE PRIMARY KEY,
        tahun SMALLINT NOT NULL,
        kuartal SMALLINT NOT NULL,
        bulan SMALLINT NOT NULL,
        nama_bulan TEXT NOT NULL,
        hari_dalam_minggu SMALLINT NOT NULL,
        nama_hari TEXT NOT NULL,
        akhir_pekan BOOLEAN NOT NULL
    )
    """,
    f"""
    INSERT INTO dim_tanggal (tanggal, tahun, kuartal, bulan, nama_bulan, hari_dalam_minggu, nama_hari, akhir_pekan)
    SELECT d,
           EXTRACT(YEAR FROM d)::int,
           EXTRACT(QUARTER FROM d)::int,
           EXTRACT(MONTH FROM d)::int,
           ({NAMA_BULAN})[EXTRACT(MONTH FROM d)::int],
           EXTRACT(ISODOW FROM d)::int,
           ({NAMA_HARI})[EXTRACT(ISODOW FROM d)::int],
           EXTRACT(ISODOW FROM d)::int >= 6
    FROM (SELECT DATE '2000-01-01' + n AS d FROM generate_series(0, 14609) AS g(n)) AS x
    WHERE NOT EXISTS (SELECT 1 FROM dim_tanggal)
    """,  # noqa: S608
    """
    CREATE TABLE IF NOT EXISTS fact_cuaca_jam (
        lokasi_id INTEGER NOT NULL REFERENCES dim_lokasi (lokasi_id),
        waktu_utc TIMESTAMPTZ NOT NULL,
        tanggal_lokal DATE NOT NULL REFERENCES dim_tanggal (tanggal),
        jam_lokal SMALLINT NOT NULL CHECK (jam_lokal BETWEEN 0 AND 23),
        kode_cuaca SMALLINT REFERENCES dim_kode_cuaca (kode_cuaca),
        suhu_c NUMERIC(4,1),
        kelembapan_persen SMALLINT,
        curah_hujan_mm NUMERIC(6,1),
        kecepatan_angin_kmh NUMERIC(5,1),
        tekanan_hpa NUMERIC(6,1),
        diperbarui_run TEXT NOT NULL,
        PRIMARY KEY (lokasi_id, waktu_utc)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_cuaca_tanggal ON fact_cuaca_jam (tanggal_lokal, lokasi_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_prakiraan_jam (
        lokasi_id INTEGER NOT NULL REFERENCES dim_lokasi (lokasi_id),
        waktu_target TIMESTAMPTZ NOT NULL,
        waktu_prediksi TIMESTAMPTZ NOT NULL,
        jarak_jam SMALLINT NOT NULL,
        kode_cuaca SMALLINT REFERENCES dim_kode_cuaca (kode_cuaca),
        suhu_c NUMERIC(4,1),
        kelembapan_persen SMALLINT,
        curah_hujan_mm NUMERIC(6,1),
        kecepatan_angin_kmh NUMERIC(5,1),
        tekanan_hpa NUMERIC(6,1),
        run_id TEXT NOT NULL,
        PRIMARY KEY (lokasi_id, waktu_target, waktu_prediksi)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cuaca_revisi (
        urutan BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        run_id TEXT NOT NULL,
        lokasi_id INTEGER NOT NULL,
        waktu_utc TIMESTAMPTZ NOT NULL,
        kolom TEXT NOT NULL,
        nilai_lama NUMERIC(8,1) NOT NULL,
        nilai_baru NUMERIC(8,1) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS karantina (
        run_id TEXT NOT NULL,
        jenis TEXT,
        lokasi TEXT,
        waktu_utc TEXT,
        nilai_mentah TEXT,
        alasan TEXT,
        dicatat_pada TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE OR REPLACE VIEW mart_cuaca_harian AS
    SELECT l.lokasi_id,
           l.nama AS lokasi,
           l.provinsi,
           f.tanggal_lokal AS tanggal,
           COUNT(*) AS jumlah_jam,
           (COUNT(*) = 24) AS hari_lengkap,
           MIN(f.suhu_c) AS suhu_min_c,
           MAX(f.suhu_c) AS suhu_maks_c,
           ROUND(AVG(f.suhu_c), 1) AS suhu_rata_c,
           ROUND(AVG(f.kelembapan_persen), 0) AS kelembapan_rata_persen,
           SUM(f.curah_hujan_mm) AS curah_hujan_total_mm,
           COUNT(*) FILTER (WHERE f.curah_hujan_mm >= 0.1) AS jam_hujan,
           MAX(f.kecepatan_angin_kmh) AS angin_maks_kmh
    FROM fact_cuaca_jam f
    JOIN dim_lokasi l ON l.lokasi_id = f.lokasi_id
    GROUP BY l.lokasi_id, l.nama, l.provinsi, f.tanggal_lokal
    """,
    """
    CREATE OR REPLACE VIEW mart_akurasi_prakiraan AS
    SELECT l.nama AS lokasi,
           CASE WHEN p.jarak_jam < 24 THEN '0-23 jam'
                WHEN p.jarak_jam < 48 THEN '24-47 jam'
                ELSE '48 jam ke atas' END AS kelompok_jarak,
           COUNT(*) AS jumlah_pasangan,
           ROUND(AVG(ABS(p.suhu_c - f.suhu_c)), 2) AS mae_suhu_c,
           ROUND(AVG(p.suhu_c - f.suhu_c), 2) AS bias_suhu_c,
           ROUND(AVG(ABS(p.curah_hujan_mm - f.curah_hujan_mm)), 2) AS mae_hujan_mm
    FROM fact_prakiraan_jam p
    JOIN fact_cuaca_jam f ON f.lokasi_id = p.lokasi_id AND f.waktu_utc = p.waktu_target
    JOIN dim_lokasi l ON l.lokasi_id = p.lokasi_id
    GROUP BY l.nama, 2
    """,
]

SELECT_AKTUAL = """
-- urutan kolom ukuran harus sama dengan KOLOM_UKUR
SELECT l.nama, f.waktu_utc, f.suhu_c, f.kelembapan_persen, f.curah_hujan_mm,
       f.kecepatan_angin_kmh, f.tekanan_hpa, f.kode_cuaca
FROM fact_cuaca_jam f
JOIN dim_lokasi l ON l.lokasi_id = f.lokasi_id
WHERE f.waktu_utc BETWEEN %s AND %s
"""

SELECT_PRAKIRAAN = """
SELECT l.nama, p.waktu_target
FROM fact_prakiraan_jam p
JOIN dim_lokasi l ON l.lokasi_id = p.lokasi_id
WHERE p.waktu_prediksi = %s
"""

UPSERT_KODE = """
INSERT INTO dim_kode_cuaca (kode_cuaca, deskripsi, kategori) VALUES %s
ON CONFLICT (kode_cuaca) DO UPDATE SET deskripsi = EXCLUDED.deskripsi, kategori = EXCLUDED.kategori
"""

UPSERT_LOKASI = """
INSERT INTO dim_lokasi (nama, provinsi, lintang, bujur, elevasi_m, zona_waktu) VALUES %s
ON CONFLICT (nama) DO UPDATE SET
    provinsi = EXCLUDED.provinsi,
    lintang = EXCLUDED.lintang,
    bujur = EXCLUDED.bujur,
    elevasi_m = COALESCE(EXCLUDED.elevasi_m, dim_lokasi.elevasi_m),
    zona_waktu = EXCLUDED.zona_waktu
"""

UPSERT_AKTUAL = """
INSERT INTO fact_cuaca_jam (
    lokasi_id, waktu_utc, tanggal_lokal, jam_lokal, kode_cuaca, suhu_c, kelembapan_persen,
    curah_hujan_mm, kecepatan_angin_kmh, tekanan_hpa, diperbarui_run
) VALUES %s
ON CONFLICT (lokasi_id, waktu_utc) DO UPDATE SET
    kode_cuaca = COALESCE(EXCLUDED.kode_cuaca, fact_cuaca_jam.kode_cuaca),
    suhu_c = COALESCE(EXCLUDED.suhu_c, fact_cuaca_jam.suhu_c),
    kelembapan_persen = COALESCE(EXCLUDED.kelembapan_persen, fact_cuaca_jam.kelembapan_persen),
    curah_hujan_mm = COALESCE(EXCLUDED.curah_hujan_mm, fact_cuaca_jam.curah_hujan_mm),
    kecepatan_angin_kmh = COALESCE(EXCLUDED.kecepatan_angin_kmh, fact_cuaca_jam.kecepatan_angin_kmh),
    tekanan_hpa = COALESCE(EXCLUDED.tekanan_hpa, fact_cuaca_jam.tekanan_hpa),
    diperbarui_run = EXCLUDED.diperbarui_run
"""

INSERT_PRAKIRAAN = """
INSERT INTO fact_prakiraan_jam (
    lokasi_id, waktu_target, waktu_prediksi, jarak_jam, kode_cuaca, suhu_c, kelembapan_persen,
    curah_hujan_mm, kecepatan_angin_kmh, tekanan_hpa, run_id
) VALUES %s
ON CONFLICT (lokasi_id, waktu_target, waktu_prediksi) DO NOTHING
"""

INSERT_REVISI = """
INSERT INTO cuaca_revisi (run_id, lokasi_id, waktu_utc, kolom, nilai_lama, nilai_baru) VALUES %s
"""

INSERT_KARANTINA = """
INSERT INTO karantina (run_id, jenis, lokasi, waktu_utc, nilai_mentah, alasan) VALUES %s
"""


class LoadError(Exception):
    """Load gagal dan pipeline berhenti."""


def kunci(r):
    return (r["lokasi"], r["waktu_utc"])


def bandingkan(aktual, lama):
    """Bagi jam data aktual jadi (baru, direvisi, terisi, sama) terhadap isi database.

    lama: dict (lokasi, waktu_utc) -> dict kolom -> nilai yang sudah tersimpan.
    direvisi berisi pasangan (baris, {kolom: (nilai_lama, nilai_baru)}) untuk jam yang
    sebelumnya sudah punya angka. terisi adalah jam yang hanya mengisi kolom yang tadinya
    kosong. Nilai kosong dari sumber diabaikan, jadi tidak pernah dianggap perubahan.
    """
    baru, direvisi, terisi, sama = [], [], [], []
    for r in aktual:
        k = kunci(r)
        if k not in lama:
            baru.append(r)
            continue
        beda = {
            kolom: (lama[k][kolom], r[kolom])
            for kolom in KOLOM_UKUR
            if r[kolom] is not None and lama[k][kolom] != r[kolom]
        }
        if not beda:
            sama.append(r)
        elif any(nilai_lama is not None for nilai_lama, _ in beda.values()):
            direvisi.append((r, beda))
        else:
            terisi.append(r)
    return baru, direvisi, terisi, sama


def _sisip(cur, sql, baris):
    if not baris:
        return
    from psycopg2.extras import execute_values

    execute_values(cur, sql, baris, page_size=1000)


@timed
def load(bersih, ditolak, db_name=None):
    """Tulis hasil ke database, lalu kembalikan dict jumlah per kategori.

    bersih: hasil transform (kunci aktual, prakiraan, lokasi). db_name kosong berarti PGDATABASE.
    Semua penulisan berjalan dalam satu transaksi: commit jika sukses, rollback jika error.
    """
    aktual = list({kunci(r): r for r in bersih["aktual"]}.values())
    prakiraan = bersih["prakiraan"]
    conn = None
    try:
        conn = buka(db_name)
        with conn:
            with conn.cursor() as cur:
                for pernyataan in SKEMA:
                    cur.execute(pernyataan)

                _sisip(cur, UPSERT_KODE, [(k, deskripsi, kategori) for k, (deskripsi, kategori) in KODE_CUACA.items()])
                _sisip(
                    cur,
                    UPSERT_LOKASI,
                    [
                        (v["lokasi"].nama, v["lokasi"].provinsi, v["lokasi"].lintang, v["lokasi"].bujur,
                         v["elevasi_m"], v["lokasi"].zona)
                        for v in bersih["lokasi"].values()
                    ],
                )
                cur.execute("SELECT nama, lokasi_id FROM dim_lokasi")
                id_lokasi = dict(cur.fetchall())

                lama = {}
                if aktual:
                    awal = min(r["waktu_utc"] for r in aktual)
                    akhir = max(r["waktu_utc"] for r in aktual)
                    cur.execute(SELECT_AKTUAL, (awal, akhir))
                    for nama, waktu, *ukuran in cur.fetchall():
                        lama[(nama, waktu)] = dict(zip(KOLOM_UKUR, ukuran, strict=True))
                baru, direvisi, terisi, sama = bandingkan(aktual, lama)

                tulis = baru + [r for r, _ in direvisi] + terisi
                _sisip(
                    cur,
                    UPSERT_AKTUAL,
                    [
                        (id_lokasi[r["lokasi"]], r["waktu_utc"], r["tanggal_lokal"], r["jam_lokal"], r["kode_cuaca"],
                         r["suhu_c"], r["kelembapan_persen"], r["curah_hujan_mm"], r["kecepatan_angin_kmh"],
                         r["tekanan_hpa"], RUN_ID)
                        for r in tulis
                    ],
                )
                _sisip(
                    cur,
                    INSERT_REVISI,
                    [
                        (RUN_ID, id_lokasi[r["lokasi"]], r["waktu_utc"], kolom, nilai_lama, nilai_baru)
                        for r, beda in direvisi
                        for kolom, (nilai_lama, nilai_baru) in beda.items()
                        if nilai_lama is not None
                    ],
                )

                prakiraan_baru = prakiraan
                if prakiraan:
                    cur.execute(SELECT_PRAKIRAAN, (prakiraan[0]["waktu_prediksi"],))
                    ada = set(cur.fetchall())
                    prakiraan_baru = [r for r in prakiraan if (r["lokasi"], r["waktu_utc"]) not in ada]
                _sisip(
                    cur,
                    INSERT_PRAKIRAAN,
                    [
                        (id_lokasi[r["lokasi"]], r["waktu_utc"], r["waktu_prediksi"], r["jarak_jam"], r["kode_cuaca"],
                         r["suhu_c"], r["kelembapan_persen"], r["curah_hujan_mm"], r["kecepatan_angin_kmh"],
                         r["tekanan_hpa"], RUN_ID)
                        for r in prakiraan_baru
                    ],
                )

                _sisip(
                    cur,
                    INSERT_KARANTINA,
                    [(RUN_ID, r["jenis"], r["lokasi"], r["waktu_utc"], r["nilai_mentah"], r["alasan"]) for r in ditolak],
                )

        logger.info(
            "Load selesai: aktual %d baru, %d direvisi, %d terisi, %d sama; prakiraan %d baru, %d sama; %d masuk karantina",
            len(baru), len(direvisi), len(terisi), len(sama),
            len(prakiraan_baru), len(prakiraan) - len(prakiraan_baru), len(ditolak),
        )
        if direvisi:
            logger.warning("%d jam data aktual yang sudah tersimpan direvisi oleh sumber", len(direvisi))
        return {
            "cuaca_baru": len(baru),
            "cuaca_direvisi": len(direvisi),
            "cuaca_terisi": len(terisi),
            "cuaca_sama": len(sama),
            "prakiraan_baru": len(prakiraan_baru),
            "prakiraan_sama": len(prakiraan) - len(prakiraan_baru),
        }
    except ErrorDB:
        logger.exception("Gagal menulis ke database")
        raise LoadError("Gagal menulis ke database") from None
    finally:
        if conn is not None:
            conn.close()
