"""Tes server simulasi Open-Meteo: python -m unittest discover -s tests -v"""

import json
import threading
import unittest
from datetime import date, datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from simulasi.atmosfer import VARIABEL, Atmosfer
from simulasi.server import buat_server

HARI = date(2026, 10, 14)
SEMUA = ",".join(VARIABEL)


def ambil(url):
    with urlopen(url, timeout=10) as r:
        return json.loads(r.read()), r.headers


class TestAtmosfer(unittest.TestCase):
    def test_deterministik_untuk_seed_yang_sama(self):
        a = Atmosfer(seed=1, hari=HARI).aktual(-6.9, 107.6, date(2026, 10, 1), date(2026, 10, 3))
        b = Atmosfer(seed=1, hari=HARI).aktual(-6.9, 107.6, date(2026, 10, 1), date(2026, 10, 3))
        c = Atmosfer(seed=2, hari=HARI).aktual(-6.9, 107.6, date(2026, 10, 1), date(2026, 10, 3))
        self.assertEqual(a, b)
        self.assertNotEqual(a["hourly"]["temperature_2m"], c["hourly"]["temperature_2m"])

    def test_bentuk_respons_seperti_open_meteo(self):
        isi = Atmosfer(hari=HARI).aktual(-6.9, 107.6, date(2026, 10, 1), date(2026, 10, 2))
        jam = isi["hourly"]
        self.assertEqual(len(jam["time"]), 48)
        self.assertEqual(jam["time"][0], "2026-10-01T00:00")
        for v in VARIABEL:
            self.assertEqual(len(jam[v]), 48)
        self.assertEqual(isi["hourly_units"]["temperature_2m"], "°C")
        self.assertIn("elevation", isi)

    def test_tidak_ada_data_aktual_setelah_sekarang(self):
        sekarang = datetime(2026, 10, 14, 4, 30, tzinfo=timezone.utc)
        a = Atmosfer(hari=HARI, hari_awal=0, sekarang=sekarang)
        jam = a.aktual(-6.9, 107.6, HARI, HARI)["hourly"]
        for waktu, suhu in zip(jam["time"], jam["temperature_2m"], strict=True):
            ada = datetime.strptime(waktu, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc) <= sekarang
            self.assertEqual(suhu is not None, ada, waktu)

    def test_data_aktual_tertinggal_lalu_mengejar(self):
        a = Atmosfer(hari=HARI, hari_awal=5)
        self.assertEqual(a.tanggal_data, date(2026, 10, 9))
        isi = a.aktual(-6.9, 107.6, date(2026, 10, 8), date(2026, 10, 12))
        suhu = isi["hourly"]["temperature_2m"]
        self.assertTrue(all(v is not None for v in suhu[:48]))
        self.assertTrue(all(v is None for v in suhu[48:]))
        for _ in range(3):
            a.maju()
        self.assertEqual(a.tanggal_data, date(2026, 10, 12))
        for _ in range(10):
            a.maju()
        self.assertEqual(a.tanggal_data, HARI)

    def test_angka_terbaru_bisa_direvisi_di_langkah_berikutnya(self):
        ditemukan = False
        for i in range(40):
            lintang, bujur = -1.0 - i * 0.37, 100.0 + i * 0.41
            a = Atmosfer(hari=HARI, hari_awal=5)
            sebelum = a.aktual(lintang, bujur, date(2026, 10, 9), date(2026, 10, 9))["hourly"]["temperature_2m"]
            a.maju()
            sesudah = a.aktual(lintang, bujur, date(2026, 10, 9), date(2026, 10, 9))["hourly"]["temperature_2m"]
            if sebelum != sesudah:
                ditemukan = True
                self.assertTrue(all(abs(s - b) == 0.6 or abs(abs(s - b) - 0.6) < 0.01 for s, b in zip(sesudah, sebelum, strict=True)))
                break
        self.assertTrue(ditemukan, "tidak ada satu pun lokasi yang direvisi dalam 40 percobaan")

    def test_jam_rusak_muncul_tiap_empat_langkah(self):
        def ada_rusak(langkah):
            a = Atmosfer(hari=HARI)
            for _ in range(langkah):
                a.maju()
            for i in range(60):
                lintang, bujur = -1.0 - i * 0.37, 100.0 + i * 0.41
                kelembapan = a.aktual(lintang, bujur, a.tanggal_data, a.tanggal_data)["hourly"]["relative_humidity_2m"]
                if 150 in kelembapan:
                    return True
            return False

        self.assertTrue(ada_rusak(3))
        self.assertTrue(ada_rusak(7))
        self.assertFalse(ada_rusak(2))
        self.assertFalse(ada_rusak(4))

    def test_prakiraan_mulai_tengah_malam_utc_dan_berubah_tiap_langkah(self):
        a = Atmosfer(hari=HARI)
        p1 = a.prakiraan(-6.9, 107.6, 2)
        self.assertEqual(p1["hourly"]["time"][0], "2026-10-14T00:00")
        self.assertEqual(len(p1["hourly"]["time"]), 48)
        a.maju()
        p2 = a.prakiraan(-6.9, 107.6, 2)
        self.assertNotEqual(p1["hourly"]["temperature_2m"], p2["hourly"]["temperature_2m"])

    def test_prakiraan_makin_jauh_makin_meleset(self):
        a = Atmosfer(seed=7, hari=HARI)
        dekat, jauh = [], []
        for i in range(30):
            lintang, bujur = -1.0 - i * 0.37, 100.0 + i * 0.41
            p = a.prakiraan(lintang, bujur, 3)["hourly"]
            for j, t in enumerate(p["time"]):
                waktu = datetime.strptime(t, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
                benar = a._nilai(lintang, bujur, waktu)["temperature_2m"]
                galat = abs(p["temperature_2m"][j] - benar)
                (dekat if j < 12 else jauh if j >= 60 else []).append(galat)
        self.assertLess(sum(dekat) / len(dekat), sum(jauh) / len(jauh))


class TestServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = buat_server(port=0, interval=0, hari=HARI)
        cls.url = f"http://127.0.0.1:{cls.server.server_address[1]}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_archive(self):
        isi, header = ambil(
            f"{self.url}/v1/archive?latitude=-6.9&longitude=107.6&start_date=2026-10-08&end_date=2026-10-09"
            f"&hourly={SEMUA}&timezone=UTC"
        )
        self.assertEqual(len(isi["hourly"]["time"]), 48)
        self.assertIn("X-Langkah", header)

    def test_forecast(self):
        isi, _ = ambil(f"{self.url}/v1/forecast?latitude=-6.9&longitude=107.6&hourly={SEMUA}&timezone=UTC&forecast_days=3")
        self.assertEqual(len(isi["hourly"]["time"]), 72)

    def test_parameter_salah_dijawab_400_dengan_alasan(self):
        for jalur in (
            f"/v1/forecast?latitude=abc&longitude=107.6&hourly={SEMUA}",
            "/v1/forecast?latitude=-6.9&longitude=107.6&hourly=temperature_2m",
            f"/v1/forecast?latitude=-6.9&longitude=107.6&hourly={SEMUA}&forecast_days=99",
            f"/v1/forecast?latitude=-6.9&longitude=107.6&hourly={SEMUA}&timezone=Asia/Jakarta",
            f"/v1/archive?latitude=-6.9&longitude=107.6&hourly={SEMUA}&start_date=2026-10-09&end_date=2026-10-01",
            f"/v1/archive?latitude=-6.9&longitude=107.6&hourly={SEMUA}&start_date=bukan&end_date=2026-10-01",
        ):
            with self.subTest(jalur=jalur):
                with self.assertRaises(HTTPError) as ctx:
                    urlopen(self.url + jalur, timeout=10)
                self.assertEqual(ctx.exception.code, 400)
                badan = json.loads(ctx.exception.read())
                self.assertTrue(badan["error"])
                self.assertTrue(badan["reason"])

    def test_jalur_tak_dikenal_404(self):
        with self.assertRaises(HTTPError) as ctx:
            urlopen(self.url + "/tidak-ada", timeout=10)
        self.assertEqual(ctx.exception.code, 404)

    def test_maju_lewat_post(self):
        langkah_awal = self.server.sumber.atmosfer.langkah
        with urlopen(Request(self.url + "/maju", method="POST"), timeout=10) as r:
            self.assertEqual(json.loads(r.read())["langkah"], langkah_awal + 1)


if __name__ == "__main__":
    unittest.main()
