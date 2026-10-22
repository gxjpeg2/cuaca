"""Tes unittest: python -m unittest discover -s tests -v

Extract dan pipeline dites lewat server palsu lokal (ServerPalsu) dan server simulasi, tanpa internet.
Tes database butuh PostgreSQL (lihat README.md) dan di-skip jika tidak terjangkau.
"""

import contextlib
import csv
import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlparse

from cuaca.extract import ExtractError, extract, jendela_tanggal
from cuaca.kesehatan import main as main_kesehatan
from cuaca.kesehatan import periksa
from cuaca.koneksi import ErrorDB, buka
from cuaca.konfigurasi import KODE_CUACA, LOKASI, TANGGAL_MIN, VARIABEL, Konfigurasi, Lokasi, hari_ini
from cuaca.load import LoadError, bandingkan, load
from cuaca.logging_setup import LevelCounter, RedactFilter
from cuaca.pipeline import run
from cuaca.ringkasan import buat_ringkasan
from cuaca.riwayat import FORMAT_WAKTU, baca_riwayat, catat_run
from cuaca.transform import MAX_CONTOH_LOG, TransformError, _ubah_nilai, transform, ubah_payload
from simulasi.server import buat_server

AKAR_PROJECT = Path(__file__).resolve().parent.parent
NAMA_DB_UJI = os.getenv("CUACA_TEST_DB", "cuaca_test")
HARI = date(2026, 10, 14)
SEKARANG = datetime(2026, 10, 14, 6, 30, tzinfo=timezone.utc)
SEKARANG_BULAT = SEKARANG.replace(minute=0)
UJI = Lokasi("Uji", "Provinsi Uji", -2.5, 140.7, "Asia/Jayapura")


def db_uji():
    return buka(NAMA_DB_UJI)


def bersihkan_db():
    conn = db_uji()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DROP VIEW IF EXISTS mart_akurasi_prakiraan, mart_cuaca_harian")
                cur.execute(
                    "DROP TABLE IF EXISTS cuaca_revisi, fact_prakiraan_jam, fact_cuaca_jam, "
                    "dim_tanggal, dim_lokasi, dim_kode_cuaca, karantina CASCADE"
                )
    finally:
        conn.close()


def ambil(sql, params=None):
    """Jalankan SELECT di database uji dan kembalikan semua baris."""
    conn = db_uji()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


class ButuhPostgres(unittest.TestCase):
    """Dasar tes yang menyentuh database: skip otomatis jika Postgres tidak jalan.

    Di CI, database yang tidak terjangkau membuat tes gagal, bukan di-skip.
    """

    def setUp(self):
        try:
            db_uji().close()
        except ErrorDB as e:
            pesan = f"database uji '{NAMA_DB_UJI}' tidak terjangkau: {e}".strip()
            if os.getenv("CI"):
                self.fail(pesan)
            self.skipTest(pesan)
        bersihkan_db()

    def tearDown(self):
        with contextlib.suppress(ErrorDB):
            bersihkan_db()


def badan_valid(waktu=("2026-10-14T00:00", "2026-10-14T01:00"), **timpa):
    """Badan respons Open-Meteo yang sah, tiap variabel berisi satu nilai per jam."""
    dasar = {
        "temperature_2m": 27.5, "relative_humidity_2m": 80, "precipitation": 0.0,
        "wind_speed_10m": 9.0, "pressure_msl": 1010.0, "weather_code": 1,
    }
    dasar.update(timpa)
    jam = {"time": list(waktu)}
    for variabel, nilai in dasar.items():
        jam[variabel] = list(nilai) if isinstance(nilai, list | tuple) else [nilai] * len(waktu)
    return {"latitude": -2.5, "longitude": 140.7, "elevation": 12.0, "hourly": jam}


def payload(jenis="aktual", lokasi=UJI, diambil_pada=SEKARANG_BULAT, **kwargs):
    return {"jenis": jenis, "lokasi": lokasi, "diambil_pada": diambil_pada, "isi": badan_valid(**kwargs)}


class ServerPalsu:
    """Server HTTP kecil yang jawabannya diatur per permintaan: penjawab(nomor, jalur) -> (status, isi)."""

    def __init__(self, penjawab):
        self.permintaan = []
        pemilik = self

        class Penangan(BaseHTTPRequestHandler):
            def do_GET(self):
                alamat = urlparse(self.path)
                pemilik.permintaan.append((alamat.path, {k: v[0] for k, v in parse_qs(alamat.query).items()}))
                status, isi = penjawab(len(pemilik.permintaan), alamat.path)
                badan = isi if isinstance(isi, bytes) else json.dumps(isi).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(badan)))
                self.end_headers()
                self.wfile.write(badan)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Penangan)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()


def konfig_untuk(url, **kwargs):
    dasar = dict(
        url_prakiraan=url + "/v1/forecast", url_arsip=url + "/v1/archive", folder_mentah=None,
        jeda_permintaan=0, jeda=0, lokasi=("Bandung",),
    )
    dasar.update(kwargs)
    return Konfigurasi(**dasar)


@contextlib.contextmanager
def simulasi(maju=0, **kwargs):
    server = buat_server(port=0, interval=0, hari=kwargs.pop("hari", HARI), **kwargs)
    for _ in range(maju):
        server.sumber.maju()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


class TestRentang(unittest.TestCase):
    def test_bawaan_tujuh_hari_berakhir_hari_ini(self):
        self.assertEqual(Konfigurasi().rentang(HARI), (HARI - timedelta(days=6), HARI))

    def test_mulai_dan_selesai_menggantikan_hari(self):
        k = Konfigurasi(mulai=date(2026, 1, 1), selesai=date(2026, 1, 31))
        self.assertEqual(k.rentang(HARI), (date(2026, 1, 1), date(2026, 1, 31)))

    def test_mulai_setelah_selesai_ditolak(self):
        with self.assertRaises(ValueError):
            Konfigurasi(mulai=date(2026, 2, 1), selesai=date(2026, 1, 1)).rentang(HARI)

    def test_sebelum_batas_dim_tanggal_ditolak(self):
        with self.assertRaises(ValueError):
            Konfigurasi(mulai=TANGGAL_MIN - timedelta(days=1), selesai=HARI).rentang(HARI)

    def test_hari_ini_memakai_wib(self):
        self.assertIsInstance(hari_ini(), date)


class TestLokasi(unittest.TestCase):
    def test_semua_lokasi_kalau_kosong(self):
        self.assertEqual(Konfigurasi().lokasi_terpilih(), LOKASI)

    def test_filter_potongan_nama_tanpa_peduli_huruf_besar(self):
        self.assertEqual([k.nama for k in Konfigurasi(lokasi=("bandu", "JAKARTA")).lokasi_terpilih()], ["Jakarta", "Bandung"])

    def test_tidak_cocok_ditolak(self):
        with self.assertRaises(ValueError):
            Konfigurasi(lokasi=("Atlantis",)).lokasi_terpilih()

    def test_koordinat_dan_zona_waktu_masuk_akal(self):
        nama = [k.nama for k in LOKASI]
        self.assertEqual(len(nama), len(set(nama)))
        for k in LOKASI:
            self.assertTrue(-11 <= k.lintang <= 6 and 95 <= k.bujur <= 141, k.nama)
            self.assertIn(k.selisih, (timedelta(hours=7), timedelta(hours=8), timedelta(hours=9)))


class TestJendelaTanggal(unittest.TestCase):
    def test_pecah_dan_tidak_tumpang_tindih(self):
        potongan = jendela_tanggal(date(2026, 1, 1), date(2026, 1, 10), 4)
        self.assertEqual(
            potongan,
            [(date(2026, 1, 1), date(2026, 1, 4)), (date(2026, 1, 5), date(2026, 1, 8)), (date(2026, 1, 9), date(2026, 1, 10))],
        )

    def test_satu_hari(self):
        self.assertEqual(jendela_tanggal(HARI, HARI, 90), [(HARI, HARI)])


class TestUbahNilai(unittest.TestCase):
    def test_suhu_dibulatkan_satu_desimal(self):
        self.assertEqual(_ubah_nilai("suhu_c", 27.46), (Decimal("27.5"), None))
        self.assertEqual(_ubah_nilai("suhu_c", "-3.04"), (Decimal("-3.0"), None))

    def test_none_berarti_tidak_ada_data_bukan_kesalahan(self):
        self.assertEqual(_ubah_nilai("suhu_c", None), (None, None))

    def test_bukan_angka(self):
        for nilai in ("abc", "nan", "inf", True, [1]):
            with self.subTest(nilai=nilai):
                hasil, alasan = _ubah_nilai("suhu_c", nilai)
                self.assertIsNone(hasil)
                self.assertIn("bukan angka", alasan)

    def test_di_luar_batas(self):
        for kolom, nilai in (
            ("kelembapan_persen", 150), ("kelembapan_persen", -1), ("suhu_c", 99), ("curah_hujan_mm", -0.1),
            ("kecepatan_angin_kmh", 1000), ("tekanan_hpa", 500),
        ):
            with self.subTest(kolom=kolom, nilai=nilai):
                hasil, alasan = _ubah_nilai(kolom, nilai)
                self.assertIsNone(hasil)
                self.assertIn("di luar batas", alasan)

    def test_kelembapan_jadi_bilangan_bulat(self):
        self.assertEqual(_ubah_nilai("kelembapan_persen", 55.4), (55, None))
        self.assertIsInstance(_ubah_nilai("kelembapan_persen", 55.4)[0], int)

    def test_kode_cuaca_harus_ada_di_tabel_wmo(self):
        self.assertEqual(_ubah_nilai("kode_cuaca", 61), (61, None))
        self.assertEqual(_ubah_nilai("kode_cuaca", 61.0), (61, None))
        for nilai in (62, 61.5, -1):
            with self.subTest(nilai=nilai):
                self.assertIn("tidak dikenal", _ubah_nilai("kode_cuaca", nilai)[1])

    def test_kode_cuaca_dan_variabel_konsisten(self):
        self.assertEqual(len(KODE_CUACA), 28)
        self.assertEqual(list(VARIABEL.values())[-1], "kode_cuaca")


class TestUbahPayload(unittest.TestCase):
    def test_waktu_lokal_mengikuti_zona_lokasi(self):
        bersih, ditolak, kosong, _ = ubah_payload(payload(waktu=["2026-10-09T20:00"]))
        self.assertEqual((ditolak, kosong), ([], 0))
        self.assertEqual(bersih[0]["tanggal_lokal"], date(2026, 10, 10))
        self.assertEqual(bersih[0]["jam_lokal"], 5)
        self.assertEqual(bersih[0]["waktu_utc"], datetime(2026, 10, 9, 20, tzinfo=timezone.utc))

    def test_nilai_bertipe_decimal_dan_int(self):
        r = ubah_payload(payload(waktu=["2026-10-14T03:00"]))[0][0]
        self.assertEqual(r["suhu_c"], Decimal("27.5"))
        self.assertEqual(r["kelembapan_persen"], 80)
        self.assertEqual(r["kode_cuaca"], 1)

    def test_jam_yang_semua_nilainya_kosong_dilewati(self):
        kosong_semua = dict(
            temperature_2m=None, relative_humidity_2m=None, precipitation=None,
            wind_speed_10m=None, pressure_msl=None, weather_code=None,
        )
        bersih, ditolak, kosong, _ = ubah_payload(payload(**kosong_semua))
        self.assertEqual((bersih, ditolak, kosong), ([], [], 2))

    def test_sebagian_kosong_tetap_disimpan_dengan_null(self):
        bersih, ditolak, _, _ = ubah_payload(payload(precipitation=None, pressure_msl=None))
        self.assertEqual(len(bersih), 2)
        self.assertEqual(ditolak, [])
        self.assertIsNone(bersih[0]["curah_hujan_mm"])
        self.assertEqual(bersih[0]["suhu_c"], Decimal("27.5"))

    def test_nilai_rusak_masuk_karantina_dengan_alasan(self):
        bersih, ditolak, _, _ = ubah_payload(payload(relative_humidity_2m=[150, 80]))
        self.assertEqual(len(bersih), 1)
        self.assertEqual(len(ditolak), 1)
        self.assertEqual(ditolak[0]["lokasi"], "Uji")
        self.assertEqual(ditolak[0]["jenis"], "aktual")
        self.assertEqual(ditolak[0]["waktu_utc"], "2026-10-14T00:00")
        self.assertIn("kelembapan_persen di luar batas", ditolak[0]["alasan"])

    def test_beberapa_kolom_rusak_alasannya_digabung(self):
        _, ditolak, _, _ = ubah_payload(payload(waktu=["2026-10-14T00:00"], relative_humidity_2m=150, weather_code=62))
        self.assertIn("kelembapan_persen", ditolak[0]["alasan"])
        self.assertIn("kode cuaca", ditolak[0]["alasan"])

    def test_waktu_tidak_valid(self):
        bersih, ditolak, _, _ = ubah_payload(payload(waktu=["kemarin", "2026-10-14T01:00"]))
        self.assertEqual(len(bersih), 1)
        self.assertEqual(ditolak[0]["alasan"], "waktu tidak valid")

    def test_prakiraan_membuang_jam_yang_sudah_lewat_dan_menghitung_jarak(self):
        waktu = ["2026-10-14T05:00", "2026-10-14T06:00", "2026-10-14T09:00"]
        bersih, ditolak, _, terlewat = ubah_payload(payload("prakiraan", waktu=waktu))
        self.assertEqual(terlewat, 1)
        self.assertEqual([r["jarak_jam"] for r in bersih], [0, 3])
        self.assertEqual(bersih[0]["waktu_prediksi"], datetime(2026, 10, 14, 6, tzinfo=timezone.utc))
        self.assertEqual(ditolak, [])

    def test_data_aktual_tidak_dipotong_oleh_waktu_pengambilan(self):
        bersih, _, _, terlewat = ubah_payload(payload("aktual", waktu=["2026-10-10T00:00"]))
        self.assertEqual((len(bersih), terlewat), (1, 0))


    def test_data_aktual_setelah_waktu_pengambilan_dibuang(self):
        waktu = ["2026-10-14T05:00", "2026-10-14T06:00", "2026-10-14T07:00"]
        bersih, ditolak, kosong, _ = ubah_payload(payload("aktual", waktu=waktu))
        self.assertEqual([r["waktu_utc"].hour for r in bersih], [5, 6])
        self.assertEqual((kosong, ditolak), (1, []))


class TestTransform(unittest.TestCase):
    def test_hasil_berisi_aktual_prakiraan_dan_lokasi(self):
        bersih, ditolak = transform([payload("aktual"), payload("prakiraan", waktu=["2026-10-14T08:00"])])
        self.assertEqual((len(bersih["aktual"]), len(bersih["prakiraan"]), ditolak), (2, 1, []))
        self.assertEqual(bersih["lokasi"]["Uji"]["elevasi_m"], Decimal("12.0"))
        self.assertIs(bersih["lokasi"]["Uji"]["lokasi"], UJI)

    def test_duplikat_ditolak(self):
        bersih, ditolak = transform([payload(waktu=["2026-10-14T00:00"]), payload(waktu=["2026-10-14T00:00"])])
        self.assertEqual(len(bersih["aktual"]), 1)
        self.assertEqual([d["alasan"] for d in ditolak], ["duplikat"])

    def test_tidak_ada_data_sama_sekali_menghentikan_pipeline(self):
        kosong = dict(
            temperature_2m=None, relative_humidity_2m=None, precipitation=None,
            wind_speed_10m=None, pressure_msl=None, weather_code=None,
        )
        with self.assertRaises(TransformError):
            transform([payload(**kosong)])

    def test_terlalu_banyak_ditolak_menghentikan_pipeline(self):
        with self.assertRaises(TransformError):
            transform([payload(relative_humidity_2m=[150, 150])])

    def test_ditolak_di_bawah_batas_tetap_lanjut(self):
        waktu = [f"2026-10-14T{j:02d}:00" for j in range(10)]
        hum = [150] + [80] * 9
        sore = datetime(2026, 10, 14, 12, tzinfo=timezone.utc)
        bersih, ditolak = transform([payload(waktu=waktu, relative_humidity_2m=hum, diambil_pada=sore)])
        self.assertEqual((len(bersih["aktual"]), len(ditolak)), (9, 1))

    def test_log_contoh_dibatasi(self):
        waktu = [f"2026-10-{d:02d}T{j:02d}:00" for d in (10, 11) for j in range(12)]
        hum = [150] * 12 + [80] * 12
        with self.assertLogs("cuaca.transform", level="WARNING") as log:
            transform([payload(waktu=waktu, relative_humidity_2m=hum)])
        self.assertEqual(len(log.records), MAX_CONTOH_LOG + 1)


class TestExtract(unittest.TestCase):
    def test_mode_semua_mengambil_aktual_dan_prakiraan_per_lokasi(self):
        with simulasi() as url:
            info = {}
            hasil = extract(konfig_untuk(url, lokasi=("Bandung", "Jakarta")), info, hari=HARI, sekarang=SEKARANG)
        self.assertEqual([(p["jenis"], p["lokasi"].nama) for p in hasil], [
            ("aktual", "Jakarta"), ("prakiraan", "Jakarta"), ("aktual", "Bandung"), ("prakiraan", "Bandung"),
        ])
        self.assertEqual(info["lokasi"], 2)
        self.assertEqual(info["permintaan"], 4)
        self.assertEqual((info["mulai"], info["selesai"]), ("2026-10-08", "2026-10-14"))
        self.assertEqual(info["waktu_prediksi"], "2026-10-14T06:00:00+00:00")
        self.assertEqual(hasil[0]["diambil_pada"], datetime(2026, 10, 14, 6, tzinfo=timezone.utc))

    def test_mode_aktual_tidak_memanggil_forecast(self):
        with ServerPalsu(lambda n, j: (200, badan_valid())) as palsu:
            info = {}
            extract(konfig_untuk(palsu.url, mode="aktual"), info, hari=HARI, sekarang=SEKARANG)
        self.assertEqual([j for j, _ in palsu.permintaan], ["/v1/archive"])
        self.assertIsNone(info["waktu_prediksi"])

    def test_mode_prakiraan_tidak_memanggil_archive(self):
        with ServerPalsu(lambda n, j: (200, badan_valid())) as palsu:
            info = {}
            extract(konfig_untuk(palsu.url, mode="prakiraan"), info, hari=HARI, sekarang=SEKARANG)
        self.assertEqual([j for j, _ in palsu.permintaan], ["/v1/forecast"])
        self.assertIsNone(info["mulai"])

    def test_parameter_permintaan(self):
        with ServerPalsu(lambda n, j: (200, badan_valid())) as palsu:
            extract(konfig_untuk(palsu.url, hari_prakiraan=5), hari=HARI, sekarang=SEKARANG)
        arsip = dict(palsu.permintaan)["/v1/archive"]
        prakiraan = dict(palsu.permintaan)["/v1/forecast"]
        for param in (arsip, prakiraan):
            self.assertEqual(param["timezone"], "UTC")
            self.assertEqual(set(param["hourly"].split(",")), set(VARIABEL))
            self.assertEqual(param["wind_speed_unit"], "kmh")
            self.assertAlmostEqual(float(param["latitude"]), -6.9175)
        self.assertEqual((arsip["start_date"], arsip["end_date"]), ("2026-10-08", "2026-10-14"))
        self.assertEqual((prakiraan["forecast_days"], prakiraan["past_days"]), ("5", "0"))

    def test_rentang_panjang_dipecah_per_jendela(self):
        with ServerPalsu(lambda n, j: (200, badan_valid())) as palsu:
            extract(konfig_untuk(palsu.url, mode="aktual", mulai=date(2026, 1, 1), selesai=date(2026, 7, 1), jendela_hari=90),
                    hari=HARI, sekarang=SEKARANG)
        self.assertEqual(len(palsu.permintaan), 3)

    def test_cache_dipakai_untuk_potongan_final_saja(self):
        with tempfile.TemporaryDirectory() as tmp, simulasi() as url:
            lama = dict(mode="aktual", mulai=HARI - timedelta(days=200), selesai=HARI - timedelta(days=150), folder_mentah=tmp)
            info1, info2 = {}, {}
            extract(konfig_untuk(url, **lama), info1, hari=HARI, sekarang=SEKARANG)
            extract(konfig_untuk(url, **lama), info2, hari=HARI, sekarang=SEKARANG)
            self.assertEqual((info1["dari_cache"], info2["dari_cache"]), (0, 1))
            self.assertEqual(len(list(Path(tmp).glob("arsip_bandung_*.json"))), 1)

            baru = dict(mode="aktual", folder_mentah=tmp)
            info3, info4 = {}, {}
            extract(konfig_untuk(url, **baru), info3, hari=HARI, sekarang=SEKARANG)
            extract(konfig_untuk(url, **baru), info4, hari=HARI, sekarang=SEKARANG)
            self.assertEqual((info3["dari_cache"], info4["dari_cache"]), (0, 0))
            self.assertEqual(len(list(Path(tmp).glob("arsip_bandung_*.json"))), 1)

    def test_cache_rusak_diambil_ulang(self):
        with tempfile.TemporaryDirectory() as tmp, simulasi() as url:
            lama = dict(mode="aktual", mulai=HARI - timedelta(days=200), selesai=HARI - timedelta(days=190), folder_mentah=tmp)
            extract(konfig_untuk(url, **lama), hari=HARI, sekarang=SEKARANG)
            berkas = next(Path(tmp).glob("*.json"))
            berkas.write_text("{rusak", encoding="utf-8")
            info = {}
            with self.assertLogs("cuaca.extract", level="WARNING"):
                extract(konfig_untuk(url, **lama), info, hari=HARI, sekarang=SEKARANG)
            self.assertEqual(info["dari_cache"], 0)

    def test_coba_ulang_gangguan_sementara(self):
        def penjawab(nomor, jalur):
            return (503, {}) if nomor <= 2 else (200, badan_valid())

        with ServerPalsu(penjawab) as palsu:
            with self.assertLogs("cuaca.extract", level="WARNING"):
                hasil = extract(konfig_untuk(palsu.url, mode="prakiraan", maks_coba=3), hari=HARI, sekarang=SEKARANG)
        self.assertEqual(len(hasil), 1)
        self.assertEqual(len(palsu.permintaan), 3)

    def test_rate_limit_429_dicoba_ulang(self):
        def penjawab(nomor, jalur):
            return (429, {"error": True, "reason": "terlalu banyak"}) if nomor == 1 else (200, badan_valid())

        with ServerPalsu(penjawab) as palsu:
            with self.assertLogs("cuaca.extract", level="WARNING"):
                extract(konfig_untuk(palsu.url, mode="prakiraan"), hari=HARI, sekarang=SEKARANG)
        self.assertEqual(len(palsu.permintaan), 2)

    def test_menyerah_setelah_percobaan_habis(self):
        with ServerPalsu(lambda n, j: (503, {})) as palsu:
            with self.assertLogs("cuaca.extract", level="WARNING"), self.assertRaises(ExtractError) as ctx:
                extract(konfig_untuk(palsu.url, mode="prakiraan", maks_coba=2), hari=HARI, sekarang=SEKARANG)
        self.assertEqual(len(palsu.permintaan), 2)
        self.assertIn("HTTP 503", str(ctx.exception))

    def test_http_400_tidak_dicoba_ulang_dan_membawa_alasan(self):
        with ServerPalsu(lambda n, j: (400, {"error": True, "reason": "latitude di luar rentang"})) as palsu:
            with self.assertLogs("cuaca.extract", level="ERROR"), self.assertRaises(ExtractError) as ctx:
                extract(konfig_untuk(palsu.url, mode="prakiraan"), hari=HARI, sekarang=SEKARANG)
        self.assertEqual(len(palsu.permintaan), 1)
        self.assertIn("latitude di luar rentang", str(ctx.exception))

    def test_server_tidak_terjangkau(self):
        konfig = konfig_untuk("http://127.0.0.1:1", mode="prakiraan", maks_coba=1, timeout=2)
        with self.assertLogs("cuaca.extract", level="WARNING"), self.assertRaises(ExtractError):
            extract(konfig, hari=HARI, sekarang=SEKARANG)

    def test_bentuk_respons_salah(self):
        kasus = {
            "bukan json": b"<html>",
            "bukan objek": [1, 2],
            "tanpa hourly": {"latitude": 1},
            "tanpa time": {"hourly": {"temperature_2m": [1]}},
            "error 200": {"error": True, "reason": "rusak"},
        }
        hilang = badan_valid()
        del hilang["hourly"]["pressure_msl"]
        kasus["variabel hilang"] = hilang
        pendek = badan_valid()
        pendek["hourly"]["temperature_2m"] = [1.0]
        kasus["panjang beda"] = pendek
        for nama, isi in kasus.items():
            with self.subTest(nama):
                with ServerPalsu(lambda n, j, isi=isi: (200, isi)) as palsu:
                    with self.assertLogs("cuaca.extract", level="ERROR"), self.assertRaises(ExtractError):
                        extract(konfig_untuk(palsu.url, mode="prakiraan"), hari=HARI, sekarang=SEKARANG)

    def test_pengaturan_salah(self):
        with simulasi() as url:
            for kwargs in (dict(mode="ngawur"), dict(hari_prakiraan=17), dict(lokasi=("Atlantis",)),
                           dict(mulai=date(2026, 2, 1), selesai=date(2026, 1, 1))):
                with self.subTest(kwargs):
                    with self.assertRaises(ExtractError):
                        extract(konfig_untuk(url, **kwargs), hari=HARI, sekarang=SEKARANG)


class TestBandingkan(unittest.TestCase):
    @staticmethod
    def baris(**timpa):
        r = {"lokasi": "Uji", "waktu_utc": datetime(2026, 10, 14, tzinfo=timezone.utc), "suhu_c": Decimal("27.5"),
             "kelembapan_persen": 80, "curah_hujan_mm": Decimal("0.0"), "kecepatan_angin_kmh": Decimal("9.0"),
             "tekanan_hpa": Decimal("1010.0"), "kode_cuaca": 1}
        r.update(timpa)
        return r

    def lama(self, **timpa):
        r = self.baris(**timpa)
        return {(r["lokasi"], r["waktu_utc"]): {k: r[k] for k in VARIABEL.values()}}

    def test_baru(self):
        baru, direvisi, terisi, sama = bandingkan([self.baris()], {})
        self.assertEqual((len(baru), len(direvisi), len(terisi), len(sama)), (1, 0, 0, 0))

    def test_sama(self):
        hasil = bandingkan([self.baris()], self.lama())
        self.assertEqual([len(h) for h in hasil], [0, 0, 0, 1])

    def test_direvisi_mencatat_kolom_yang_berubah(self):
        _, direvisi, _, _ = bandingkan([self.baris(suhu_c=Decimal("28.1"), kode_cuaca=3)], self.lama())
        self.assertEqual(direvisi[0][1], {"suhu_c": (Decimal("27.5"), Decimal("28.1")), "kode_cuaca": (1, 3)})

    def test_terisi_kalau_hanya_mengisi_kolom_kosong(self):
        hasil = bandingkan([self.baris()], self.lama(suhu_c=None, curah_hujan_mm=None))
        self.assertEqual([len(h) for h in hasil], [0, 0, 1, 0])

    def test_nilai_kosong_dari_sumber_bukan_perubahan(self):
        hasil = bandingkan([self.baris(suhu_c=None, kode_cuaca=None)], self.lama())
        self.assertEqual([len(h) for h in hasil], [0, 0, 0, 1])

    def test_revisi_dan_pengisian_sekaligus_dihitung_revisi(self):
        hasil = bandingkan([self.baris(suhu_c=Decimal("30.0"), tekanan_hpa=Decimal("1011.0"))], self.lama(tekanan_hpa=None))
        self.assertEqual([len(h) for h in hasil], [0, 1, 0, 0])


class TestLoadTanpaDatabase(unittest.TestCase):
    def test_database_tidak_terjangkau_jadi_load_error(self):
        bersih = {"aktual": [], "prakiraan": [], "lokasi": {}}
        with mock.patch("cuaca.load.buka", side_effect=ErrorDB("tidak ada koneksi")):
            with self.assertLogs("cuaca.load", level="ERROR"), self.assertRaises(LoadError):
                load(bersih, [])


def muat(url, sekarang=SEKARANG, **kwargs):
    """Extract dari simulasi, transform, lalu load. Hasilnya (bersih, ditolak, hasil_load)."""
    mentah = extract(konfig_untuk(url, **kwargs), hari=HARI, sekarang=sekarang)
    bersih, ditolak = transform(mentah)
    return bersih, ditolak, load(bersih, ditolak, NAMA_DB_UJI)


class TestLoad(ButuhPostgres):
    def test_load_pertama_mengisi_dimensi_dan_fakta(self):
        with simulasi() as url:
            bersih, _, hasil = muat(url, lokasi=("Bandung", "Jayapura"))
        self.assertEqual(hasil["cuaca_baru"], len(bersih["aktual"]))
        self.assertEqual(hasil["prakiraan_baru"], len(bersih["prakiraan"]))
        self.assertEqual(ambil("SELECT COUNT(*) FROM fact_cuaca_jam")[0][0], 96)
        self.assertEqual(ambil("SELECT COUNT(*) FROM fact_prakiraan_jam")[0][0], 132)
        self.assertEqual(ambil("SELECT COUNT(*) FROM dim_kode_cuaca")[0][0], len(KODE_CUACA))
        self.assertEqual(ambil("SELECT COUNT(*) FROM dim_tanggal")[0][0], 14610)
        self.assertEqual(ambil("SELECT MIN(tanggal), MAX(tanggal) FROM dim_tanggal")[0], (date(2000, 1, 1), date(2039, 12, 31)))
        lokasi = ambil("SELECT nama, provinsi, zona_waktu, elevasi_m FROM dim_lokasi ORDER BY nama")
        self.assertEqual([r[:3] for r in lokasi], [
            ("Bandung", "Jawa Barat", "Asia/Jakarta"), ("Jayapura", "Papua", "Asia/Jayapura"),
        ])
        self.assertTrue(all(r[3] is not None for r in lokasi))

    def test_dim_tanggal_berisi_nama_dan_akhir_pekan(self):
        with simulasi() as url:
            muat(url)
        rabu = ambil("SELECT nama_hari, nama_bulan, kuartal, akhir_pekan FROM dim_tanggal WHERE tanggal = '2026-10-14'")[0]
        sabtu = ambil("SELECT nama_hari, akhir_pekan FROM dim_tanggal WHERE tanggal = '2026-10-17'")[0]
        self.assertEqual(rabu, ("Rabu", "Oktober", 4, False))
        self.assertEqual(sabtu, ("Sabtu", True))

    def test_waktu_lokal_tersimpan_sesuai_zona(self):
        with simulasi() as url:
            muat(url, lokasi=("Jayapura",))
        baris = ambil(
            "SELECT tanggal_lokal, jam_lokal FROM fact_cuaca_jam "
            "WHERE waktu_utc = '2026-10-09 20:00:00+00'"
        )
        self.assertEqual(baris, [(date(2026, 10, 10), 5)])

    def test_load_dua_kali_tidak_menggandakan(self):
        with simulasi() as url:
            _, _, pertama = muat(url, lokasi=("Bandung",))
            _, _, kedua = muat(url, lokasi=("Bandung",))
        self.assertEqual((kedua["cuaca_baru"], kedua["cuaca_direvisi"], kedua["cuaca_sama"]), (0, 0, pertama["cuaca_baru"]))
        self.assertEqual((kedua["prakiraan_baru"], kedua["prakiraan_sama"]), (0, pertama["prakiraan_baru"]))
        self.assertEqual(ambil("SELECT COUNT(*) FROM fact_cuaca_jam")[0][0], 48)
        self.assertEqual(ambil("SELECT COUNT(*) FROM fact_prakiraan_jam")[0][0], 66)
        self.assertEqual(ambil("SELECT COUNT(*) FROM cuaca_revisi")[0][0], 0)
        self.assertEqual(ambil("SELECT COUNT(*) FROM dim_lokasi")[0][0], 1)

    def test_angka_yang_direvisi_diperbarui_dan_dicatat(self):
        with simulasi() as url:
            bersih, ditolak, _ = muat(url, lokasi=("Bandung",))
            r = bersih["aktual"][0]
            r2 = dict(r, suhu_c=r["suhu_c"] + Decimal("1.5"))
            hasil = load({**bersih, "aktual": [r2]}, [], NAMA_DB_UJI)
        self.assertEqual((hasil["cuaca_baru"], hasil["cuaca_direvisi"]), (0, 1))
        suhu = ambil("SELECT suhu_c FROM fact_cuaca_jam WHERE waktu_utc = %s", (r["waktu_utc"],))[0][0]
        self.assertEqual(suhu, r2["suhu_c"])
        revisi = ambil("SELECT kolom, nilai_lama, nilai_baru FROM cuaca_revisi")
        self.assertEqual(revisi, [("suhu_c", r["suhu_c"], r2["suhu_c"])])

    def test_nilai_kosong_dari_sumber_tidak_menghapus_data(self):
        with simulasi() as url:
            bersih, _, _ = muat(url, lokasi=("Bandung",))
            r = bersih["aktual"][0]
            kosong = dict(r, suhu_c=None, kode_cuaca=None)
            hasil = load({**bersih, "aktual": [kosong]}, [], NAMA_DB_UJI)
        self.assertEqual((hasil["cuaca_sama"], hasil["cuaca_direvisi"]), (1, 0))
        suhu, kode = ambil("SELECT suhu_c, kode_cuaca FROM fact_cuaca_jam WHERE waktu_utc = %s", (r["waktu_utc"],))[0]
        self.assertEqual((suhu, kode), (r["suhu_c"], r["kode_cuaca"]))

    def test_kolom_yang_tadinya_kosong_terisi_tanpa_dicatat_sebagai_revisi(self):
        with simulasi() as url:
            bersih, _, _ = muat(url, lokasi=("Bandung",))
            r = bersih["aktual"][0]
            conn = db_uji()
            try:
                with conn, conn.cursor() as cur:
                    cur.execute("UPDATE fact_cuaca_jam SET tekanan_hpa = NULL WHERE waktu_utc = %s", (r["waktu_utc"],))
            finally:
                conn.close()
            hasil = load({**bersih, "aktual": [r]}, [], NAMA_DB_UJI)
        self.assertEqual((hasil["cuaca_terisi"], hasil["cuaca_direvisi"]), (1, 0))
        self.assertEqual(ambil("SELECT tekanan_hpa FROM fact_cuaca_jam WHERE waktu_utc = %s", (r["waktu_utc"],))[0][0], r["tekanan_hpa"])
        self.assertEqual(ambil("SELECT COUNT(*) FROM cuaca_revisi")[0][0], 0)

    def test_prakiraan_di_jam_berbeda_menambah_potret_baru(self):
        with simulasi() as url:
            muat(url, lokasi=("Bandung",), sekarang=SEKARANG)
            _, _, hasil = muat(url, lokasi=("Bandung",), sekarang=SEKARANG + timedelta(hours=2))
        self.assertGreater(hasil["prakiraan_baru"], 0)
        potret = ambil("SELECT COUNT(DISTINCT waktu_prediksi) FROM fact_prakiraan_jam")[0][0]
        self.assertEqual(potret, 2)
        jarak = ambil(
            "SELECT jarak_jam FROM fact_prakiraan_jam WHERE waktu_target = '2026-10-14 10:00:00+00' ORDER BY 1"
        )
        self.assertEqual([r[0] for r in jarak], [2, 4])

    def test_karantina_menyimpan_jam_rusak(self):
        ditolak = [{"jenis": "aktual", "lokasi": "Uji", "waktu_utc": "2026-10-14T00:00", "nilai_mentah": "{}", "alasan": "x"}]
        with simulasi() as url:
            bersih, _, _ = muat(url, lokasi=("Bandung",))
        load({"aktual": [], "prakiraan": [], "lokasi": bersih["lokasi"]}, ditolak, NAMA_DB_UJI)
        baris = ambil("SELECT jenis, lokasi, waktu_utc, alasan FROM karantina")
        self.assertEqual(baris, [("aktual", "Uji", "2026-10-14T00:00", "x")])

    def test_transaksi_dibatalkan_kalau_gagal(self):
        with simulasi() as url:
            bersih, _, _ = muat(url, lokasi=("Bandung",))
        rusak = dict(bersih["aktual"][0], kode_cuaca=62)
        baru = dict(rusak, waktu_utc=rusak["waktu_utc"] + timedelta(days=30))
        with self.assertLogs("cuaca.load", level="ERROR"), self.assertRaises(LoadError):
            load({**bersih, "aktual": [baru]}, [], NAMA_DB_UJI)
        self.assertEqual(ambil("SELECT COUNT(*) FROM fact_cuaca_jam")[0][0], 48)


class TestMart(ButuhPostgres):
    def test_mart_cuaca_harian_menandai_hari_tidak_lengkap(self):
        with simulasi() as url:
            muat(url, lokasi=("Bandung",))
        baris = ambil("SELECT tanggal, jumlah_jam, hari_lengkap FROM mart_cuaca_harian ORDER BY tanggal")
        self.assertEqual(baris, [(date(2026, 10, 8), 17, False), (date(2026, 10, 9), 24, True), (date(2026, 10, 10), 7, False)])

    def test_mart_cuaca_harian_agregasi(self):
        with simulasi() as url:
            muat(url, lokasi=("Bandung",))
        suhu_min, suhu_maks, suhu_rata, hujan = ambil(
            "SELECT suhu_min_c, suhu_maks_c, suhu_rata_c, curah_hujan_total_mm FROM mart_cuaca_harian WHERE tanggal = '2026-10-09'"
        )[0]
        mentah = ambil(
            "SELECT MIN(suhu_c), MAX(suhu_c), SUM(curah_hujan_mm) FROM fact_cuaca_jam WHERE tanggal_lokal = '2026-10-09'"
        )[0]
        self.assertEqual((suhu_min, suhu_maks, hujan), mentah)
        self.assertTrue(suhu_min <= suhu_rata <= suhu_maks)

    def test_mart_akurasi_memasangkan_prakiraan_dengan_data_aktual(self):
        with simulasi(maju=5) as url:
            # Prakiraan diambil tengah malam. Data aktual hari itu baru lengkap di run berikutnya.
            muat(url, lokasi=("Bandung", "Jakarta"), sekarang=datetime(2026, 10, 14, 0, 0, tzinfo=timezone.utc))
            muat(url, lokasi=("Bandung", "Jakarta"), sekarang=datetime(2026, 10, 15, 0, 0, tzinfo=timezone.utc))
        baris = ambil("SELECT lokasi, kelompok_jarak, jumlah_pasangan, mae_suhu_c FROM mart_akurasi_prakiraan ORDER BY lokasi")
        self.assertEqual([(r[0], r[1], r[2]) for r in baris], [("Bandung", "0-23 jam", 24), ("Jakarta", "0-23 jam", 24)])
        self.assertTrue(all(r[3] is not None and r[3] >= 0 for r in baris))


class TestRingkasan(ButuhPostgres):
    def test_menulis_enam_csv(self):
        with simulasi() as url, tempfile.TemporaryDirectory() as tmp:
            muat(url, lokasi=("Bandung", "Jayapura"))
            buat_ringkasan(NAMA_DB_UJI, tmp)
            nama = {"cuaca_terbaru", "ringkasan_lokasi", "ekstrem_cuaca", "akurasi_prakiraan", "revisi_cuaca", "karantina"}
            self.assertEqual({p.stem for p in Path(tmp).glob("*.csv")}, nama)

            def baca(n):
                with (Path(tmp) / f"{n}.csv").open(encoding="utf-8", newline="") as f:
                    return list(csv.DictReader(f))

            terbaru = baca("cuaca_terbaru")
            self.assertEqual(sorted(r["lokasi"] for r in terbaru), ["Bandung", "Jayapura"])
            self.assertTrue(all(r["kondisi"] for r in terbaru))
            ringkas = baca("ringkasan_lokasi")
            self.assertEqual(sorted(r["lokasi"] for r in ringkas), ["Bandung", "Jayapura"])
            self.assertTrue(all(r["jumlah_hari"] == "1" for r in ringkas))
            self.assertEqual(baca("karantina"), [])
            self.assertEqual(baca("revisi_cuaca"), [])
            self.assertEqual(baca("akurasi_prakiraan"), [])

    def test_ekstrem_hanya_memuat_jam_yang_melewati_ambang(self):
        with simulasi() as url, tempfile.TemporaryDirectory() as tmp:
            muat(url, lokasi=("Bandung",))
            conn = db_uji()
            try:
                with conn, conn.cursor() as cur:
                    cur.execute(
                        "UPDATE fact_cuaca_jam SET curah_hujan_mm = 25, kecepatan_angin_kmh = 60 "
                        "WHERE waktu_utc = '2026-10-09 05:00:00+00'"
                    )
                    cur.execute(
                        "UPDATE fact_cuaca_jam SET curah_hujan_mm = 0, kecepatan_angin_kmh = 55 "
                        "WHERE waktu_utc = '2026-10-09 06:00:00+00'"
                    )
                    cur.execute(
                        "UPDATE fact_cuaca_jam SET curah_hujan_mm = 0, kecepatan_angin_kmh = 5 "
                        "WHERE waktu_utc NOT IN ('2026-10-09 05:00:00+00', '2026-10-09 06:00:00+00')"
                    )
            finally:
                conn.close()
            buat_ringkasan(NAMA_DB_UJI, tmp)
            with (Path(tmp) / "ekstrem_cuaca.csv").open(encoding="utf-8", newline="") as f:
                baris = list(csv.DictReader(f))
        self.assertEqual([r["jenis"] for r in baris], ["hujan lebat dan angin kencang", "angin kencang"])

    def test_revisi_dan_karantina_masuk_csv(self):
        with simulasi() as url, tempfile.TemporaryDirectory() as tmp:
            bersih, _, _ = muat(url, lokasi=("Bandung",))
            r = bersih["aktual"][0]
            load({**bersih, "aktual": [dict(r, suhu_c=r["suhu_c"] + 2)]}, [
                {"jenis": "aktual", "lokasi": "Bandung", "waktu_utc": "x", "nilai_mentah": "{}", "alasan": "rusak"},
            ], NAMA_DB_UJI)
            buat_ringkasan(NAMA_DB_UJI, tmp)
            with (Path(tmp) / "revisi_cuaca.csv").open(encoding="utf-8", newline="") as f:
                revisi = list(csv.DictReader(f))
            with (Path(tmp) / "karantina.csv").open(encoding="utf-8", newline="") as f:
                karantina = list(csv.DictReader(f))
        self.assertEqual([(x["kolom"], x["selisih"]) for x in revisi], [("suhu_c", "2.0")])
        self.assertEqual([x["alasan"] for x in karantina], ["rusak"])


def jalankan_run(url, tmp, **kwargs):
    konfig = konfig_untuk(
        url, db=NAMA_DB_UJI, folder_output=str(Path(tmp) / "out"), folder_mentah=str(Path(tmp) / "mentah"), **kwargs,
    )
    statistik = {}
    with contextlib.redirect_stderr(io.StringIO()):
        kode = run(konfig, statistik)
    return kode, statistik


class TestPipeline(ButuhPostgres):
    def test_run_sukses_dan_statistik_terisi(self):
        with simulasi(hari=hari_ini()) as url, tempfile.TemporaryDirectory() as tmp:
            kode, st = jalankan_run(url, tmp, lokasi=("Bandung", "Medan"))
            self.assertEqual(kode, 0)
            self.assertEqual(st["lokasi"], 2)
            self.assertGreater(st["aktual_valid"], 0)
            self.assertGreater(st["prakiraan_valid"], 0)
            self.assertIn("tanggal_terbaru", st)
            self.assertEqual(len(list((Path(tmp) / "out").glob("*.csv"))), 6)

    def test_run_kedua_idempotent(self):
        with simulasi(hari=hari_ini()) as url, tempfile.TemporaryDirectory() as tmp:
            _, pertama = jalankan_run(url, tmp)
            kode, kedua = jalankan_run(url, tmp)
        self.assertEqual(kode, 0)
        self.assertEqual(kedua["cuaca_baru"], 0)
        self.assertEqual(kedua["cuaca_sama"], pertama["cuaca_baru"])


class TestPipelineTanpaDatabase(unittest.TestCase):
    def test_sumber_mati_kode_keluar_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertLogs("cuaca", level="ERROR"):
                kode, _ = jalankan_run("http://127.0.0.1:1", tmp, maks_coba=1, timeout=2)
        self.assertEqual(kode, 1)

    def test_error_tak_terduga_kode_keluar_2(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch("cuaca.pipeline.extract", side_effect=RuntimeError("boom")):
            with self.assertLogs("cuaca", level="ERROR"):
                kode, _ = jalankan_run("http://127.0.0.1:1", tmp)
        self.assertEqual(kode, 2)

    def test_data_rusak_semua_kode_keluar_1(self):
        rusak = badan_valid(relative_humidity_2m=150)
        with ServerPalsu(lambda n, j: (200, rusak)) as palsu, tempfile.TemporaryDirectory() as tmp:
            with self.assertLogs("cuaca", level="ERROR"):
                kode, _ = jalankan_run(palsu.url, tmp, mode="prakiraan")
        self.assertEqual(kode, 1)


class TestCli(unittest.TestCase):
    def test_opsi_salah_ditolak_dengan_pesan_ramah(self):
        for opsi in (["--mode", "ngawur"], ["--hari", "0"], ["--hari-prakiraan", "20"], ["--mulai", "kemarin"]):
            with self.subTest(opsi):
                hasil = subprocess.run(
                    [sys.executable, "-m", "cuaca", *opsi], cwd=AKAR_PROJECT, capture_output=True, text=True, timeout=60,
                )
                self.assertEqual(hasil.returncode, 2)
                self.assertNotIn("Traceback", hasil.stderr)

    def test_help_menampilkan_semua_opsi_utama(self):
        hasil = subprocess.run(
            [sys.executable, "-m", "cuaca", "--help"], cwd=AKAR_PROJECT, capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(hasil.returncode, 0)
        for opsi in ("--mode", "--lokasi", "--url-simulasi", "--hari-prakiraan", "--mulai", "--tanpa-mentah"):
            self.assertIn(opsi, hasil.stdout)


class TestProgramUtuh(ButuhPostgres):
    def test_python_m_cuaca_dari_awal_sampai_akhir(self):
        with simulasi(hari=hari_ini()) as url, tempfile.TemporaryDirectory() as tmp:
            env = {**os.environ, "PGDATABASE": NAMA_DB_UJI, "LOG_DIR": str(Path(tmp) / "logs")}
            perintah = [
                sys.executable, "-m", "cuaca", "--url-simulasi", url, "--lokasi", "Bandung,Makassar",
                "--output", str(Path(tmp) / "out"), "--mentah", str(Path(tmp) / "mentah"),
                "--jeda-permintaan", "0", "--level", "ERROR",
            ]
            hasil = subprocess.run(perintah, cwd=AKAR_PROJECT, env=env, capture_output=True, text=True, timeout=120)
            self.assertEqual(hasil.returncode, 0, hasil.stderr)
            self.assertEqual(len(list((Path(tmp) / "out").glob("*.csv"))), 6)
            riwayat = baca_riwayat(Path(tmp) / "logs")
            self.assertEqual(len(riwayat), 1)
            self.assertEqual(riwayat[0]["kode_keluar"], 0)
            self.assertEqual(riwayat[0]["mode"], "semua")
            self.assertEqual(riwayat[0]["lokasi"], 2)

            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main_kesehatan(["--log-dir", str(Path(tmp) / "logs")]), 0)


class TestRedactFilter(unittest.TestCase):
    def catat(self, pesan):
        record = logging.LogRecord("t", logging.INFO, __file__, 1, pesan, (), None)
        RedactFilter().filter(record)
        return record.getMessage()

    def test_menyamarkan_data_sensitif(self):
        self.assertEqual(self.catat("kirim ke a.b@contoh.id"), "kirim ke [EMAIL]")
        self.assertEqual(self.catat("password=rahasia123 lanjut"), "password=[REDACTED] lanjut")
        self.assertEqual(self.catat("hubungi 081234567890"), "hubungi [TELEPON]")

    def test_pesan_biasa_tidak_berubah(self):
        self.assertEqual(self.catat("Bandung 27.5 derajat"), "Bandung 27.5 derajat")


class TestLevelCounter(unittest.TestCase):
    def test_menghitung_per_level(self):
        penghitung = LevelCounter()
        for level in (logging.INFO, logging.ERROR, logging.ERROR):
            penghitung.emit(logging.LogRecord("t", level, __file__, 1, "x", (), None))
        self.assertEqual(dict(penghitung.hitung), {"INFO": 1, "ERROR": 2})


class TestRiwayat(unittest.TestCase):
    def test_catat_dan_baca(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(baca_riwayat(tmp), [])
            catat_run(tmp, {"run_id": "a", "kode_keluar": 0})
            catat_run(tmp, {"run_id": "b", "kode_keluar": 1})
            self.assertEqual([r["run_id"] for r in baca_riwayat(tmp)], ["a", "b"])

    def test_baris_rusak_dilewati(self):
        with tempfile.TemporaryDirectory() as tmp:
            catat_run(tmp, {"run_id": "a"})
            with (Path(tmp) / "riwayat_run.jsonl").open("a", encoding="utf-8") as f:
                f.write("{rusak\n[1, 2]\n")
            catat_run(tmp, {"run_id": "b"})
            self.assertEqual([r["run_id"] for r in baca_riwayat(tmp)], ["a", "b"])


def run_sukses(waktu, **kwargs):
    dasar = {
        "run_id": "r", "waktu_selesai": waktu.strftime(FORMAT_WAKTU), "kode_keluar": 0, "mode": "semua",
        "aktual_valid": 1000, "prakiraan_valid": 500, "baris_ditolak": 0, "tanggal_terbaru": "2026-10-09",
        "jumlah_log": {"INFO": 20},
    }
    dasar.update(kwargs)
    return dasar


class TestKesehatan(unittest.TestCase):
    SEKARANG = datetime(2026, 10, 14, 8, 0, tzinfo=timezone.utc)

    def level(self, riwayat, **kwargs):
        hasil = periksa(riwayat, self.SEKARANG, **kwargs)
        return {level for level, _ in hasil}, [pesan for _, pesan in hasil]

    def test_riwayat_kosong_kritis(self):
        self.assertEqual(self.level([])[0], {"CRITICAL"})

    def test_semua_baik(self):
        level, _ = self.level([run_sukses(self.SEKARANG - timedelta(hours=2))])
        self.assertEqual(level, {"OK"})

    def test_run_terakhir_gagal_kritis(self):
        level, pesan = self.level([run_sukses(self.SEKARANG - timedelta(hours=30)), run_sukses(self.SEKARANG, kode_keluar=1)])
        self.assertIn("CRITICAL", level)
        self.assertTrue(any("GAGAL" in p for p in pesan))

    def test_data_basi_kritis(self):
        level, pesan = self.level([run_sukses(self.SEKARANG - timedelta(hours=40))])
        self.assertIn("CRITICAL", level)
        self.assertTrue(any("BASI" in p for p in pesan))

    def test_rasio_ditolak_tinggi_warning(self):
        level, _ = self.level([run_sukses(self.SEKARANG, baris_ditolak=1000)])
        self.assertIn("WARNING", level)

    def test_data_aktual_tertinggal_wajar_tidak_warning(self):
        level, _ = self.level([run_sukses(self.SEKARANG, tanggal_terbaru="2026-10-07")])
        self.assertEqual(level, {"OK"})

    def test_data_aktual_terlalu_lama_warning(self):
        level, pesan = self.level([run_sukses(self.SEKARANG, tanggal_terbaru="2026-09-20")])
        self.assertIn("WARNING", level)
        self.assertTrue(any("sumber belum memperbarui" in p for p in pesan))

    def test_tanggal_terbaru_tidak_terbaca_warning(self):
        level, _ = self.level([run_sukses(self.SEKARANG, tanggal_terbaru="bukan-tanggal")])
        self.assertIn("WARNING", level)

    def test_tanpa_prakiraan_di_mode_semua_warning(self):
        level, pesan = self.level([run_sukses(self.SEKARANG, prakiraan_valid=0)])
        self.assertIn("WARNING", level)
        self.assertTrue(any("prakiraan" in p for p in pesan))

    def test_mode_aktual_tidak_butuh_prakiraan(self):
        level, _ = self.level([run_sukses(self.SEKARANG, mode="aktual", prakiraan_valid=0)])
        self.assertEqual(level, {"OK"})

    def test_penurunan_volume_dibandingkan_dengan_mode_yang_sama(self):
        riwayat = [
            run_sukses(self.SEKARANG - timedelta(hours=26), aktual_valid=1000, prakiraan_valid=500),
            run_sukses(self.SEKARANG, aktual_valid=400, prakiraan_valid=100),
        ]
        level, pesan = self.level(riwayat)
        self.assertIn("WARNING", level)
        self.assertTrue(any("turun" in p for p in pesan))

    def test_backfill_mode_aktual_tidak_dibandingkan_dengan_run_harian(self):
        riwayat = [
            run_sukses(self.SEKARANG - timedelta(hours=26), aktual_valid=1000, prakiraan_valid=500),
            run_sukses(self.SEKARANG - timedelta(hours=1), mode="aktual", aktual_valid=20, prakiraan_valid=None),
        ]
        level, _ = self.level(riwayat)
        self.assertNotIn("WARNING", level)

    def test_log_error_warning(self):
        level, _ = self.level([run_sukses(self.SEKARANG, jumlah_log={"ERROR": 2})])
        self.assertIn("WARNING", level)

    def test_main_kode_keluar_dan_keluaran(self):
        with tempfile.TemporaryDirectory() as tmp:
            keluaran = io.StringIO()
            with contextlib.redirect_stdout(keluaran):
                kode = main_kesehatan(["--log-dir", tmp, "--sekarang", "2026-10-14T08:00:00Z"])
            self.assertEqual(kode, 2)
            self.assertIn("STATUS: CRITICAL", keluaran.getvalue())
            catat_run(tmp, run_sukses(self.SEKARANG - timedelta(hours=1)))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main_kesehatan(["--log-dir", tmp, "--sekarang", "2026-10-14T08:00:00Z"]), 0)

    def test_main_sekarang_salah_format(self):
        with self.assertRaises(SystemExit) as ctx, contextlib.redirect_stderr(io.StringIO()):
            main_kesehatan(["--sekarang", "kemarin"])
        self.assertEqual(ctx.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
