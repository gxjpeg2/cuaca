"""Atmosfer simulasi: cuaca per jam dengan bentuk respons seperti Open-Meteo.

Angkanya karangan, hanya untuk demo dan tes. Data aktual (archive) tertinggal beberapa hari
dari hari ini seperti sumber aslinya, dan maju satu hari per langkah. Angka hari aktual
terbaru masih sementara dan sebagian direvisi di langkah berikutnya. Tiap empat langkah ada
satu jam dengan kelembapan rusak untuk karantina. Jam yang belum terjadi tidak punya data aktual. Prakiraan menyimpang dari kenyataan,
makin jauh makin besar, dan berubah tiap langkah. Untuk seed yang sama, hasil langkah
ke-n selalu sama.
"""

import math
import random
from datetime import datetime, timedelta, timezone

VARIABEL = (
    "temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "pressure_msl", "weather_code",
)
SATUAN = {
    "temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm",
    "wind_speed_10m": "km/h", "pressure_msl": "hPa", "weather_code": "wmo code",
}
FORMAT_WAKTU = "%Y-%m-%dT%H:%M"
UTC = timezone.utc
RUSAK_SETIAP = 4
PELAPORAN_SEMENTARA = 0.3


def _kode(hujan, awan, acak):
    if hujan <= 0:
        return 0 if awan < 0.3 else 1 if awan < 0.6 else 2 if awan < 0.85 else 3
    if hujan >= 10 and acak < 0.3:
        return 95
    return 61 if hujan < 2.5 else 63 if hujan < 7.5 else 65


class Atmosfer:
    def __init__(self, seed=2024, hari=None, hari_awal=5, sekarang=None):
        self.seed = seed
        self.langkah = 0
        self.hari_awal = hari_awal
        if sekarang is None:
            sekarang = datetime.now(UTC) if hari is None else datetime(hari.year, hari.month, hari.day, 23, tzinfo=UTC)
        # Jam setelah waktu ini belum terjadi, jadi data aktualnya null seperti sumber asli.
        self.sekarang = sekarang
        self.akhir = hari or sekarang.date()
        self._iklim_cache = {}

    @property
    def tanggal_data(self):
        """Tanggal terakhir yang sudah ada di data aktual."""
        return min(self.akhir - timedelta(days=self.hari_awal) + timedelta(days=self.langkah), self.akhir)

    def maju(self):
        self.langkah += 1

    @staticmethod
    def _kunci(lintang, bujur):
        return f"{lintang:.2f}:{bujur:.2f}"

    def _iklim(self, lintang, bujur):
        k = self._kunci(lintang, bujur)
        if k not in self._iklim_cache:
            rng = random.Random(f"{self.seed}:iklim:{k}")
            self._iklim_cache[k] = {"rata": rng.uniform(22.0, 29.0), "elevasi": round(rng.uniform(2.0, 800.0), 1)}
        return self._iklim_cache[k]

    def _hari(self, lintang, bujur, tanggal):
        rng = random.Random(f"{self.seed}:hari:{self._kunci(lintang, bujur)}:{tanggal}")
        return {
            "selisih_suhu": rng.gauss(0, 1.2),
            "hujan": rng.random() < 0.4,
            "mulai": rng.randint(11, 19),
            "durasi": rng.randint(1, 4),
            "intensitas": rng.uniform(1.5, 14.0),
            "awan": rng.random(),
        }

    def _nilai(self, lintang, bujur, waktu):
        """Cuaca 'sebenarnya' untuk satu jam UTC: dict dengan kunci VARIABEL."""
        iklim = self._iklim(lintang, bujur)
        hari = self._hari(lintang, bujur, waktu.date())
        rng = random.Random(f"{self.seed}:jam:{self._kunci(lintang, bujur)}:{waktu.isoformat()}")
        jam_lokal = (waktu.hour + round(bujur / 15)) % 24

        hujan = 0.0
        if hari["hujan"] and hari["mulai"] <= jam_lokal < hari["mulai"] + hari["durasi"]:
            hujan = round(hari["intensitas"] * rng.uniform(0.5, 1.2), 1)
        suhu = iklim["rata"] + hari["selisih_suhu"] + 4.0 * math.sin(2 * math.pi * (jam_lokal - 9) / 24)
        if hujan:
            suhu -= 2.0
        kelembapan = 75 - 1.8 * (suhu - iklim["rata"]) + (12 if hujan else 0) + rng.gauss(0, 3)
        angin = max(0.0, 8 + rng.gauss(0, 3) + (6 if hujan > 5 else 0))
        tekanan = 1010 + 2 * math.sin(2 * math.pi * (jam_lokal - 10) / 12) + rng.gauss(0, 0.8)
        return {
            "temperature_2m": round(suhu, 1),
            "relative_humidity_2m": int(min(100, max(40, round(kelembapan)))),
            "precipitation": hujan,
            "wind_speed_10m": round(angin, 1),
            "pressure_msl": round(tekanan, 1),
            "weather_code": _kode(hujan, hari["awan"], rng.random()),
        }

    def _badan(self, lintang, bujur, waktu, deret):
        return {
            "latitude": lintang,
            "longitude": bujur,
            "generationtime_ms": 0.2,
            "utc_offset_seconds": 0,
            "timezone": "GMT",
            "timezone_abbreviation": "GMT",
            "elevation": self._iklim(lintang, bujur)["elevasi"],
            "hourly_units": {"time": "iso8601", **SATUAN},
            "hourly": {"time": [t.strftime(FORMAT_WAKTU) for t in waktu], **deret},
        }

    def _rusak(self, lintang, bujur):
        """True jika lokasi ini mendapat satu jam kelembapan rusak pada langkah sekarang."""
        if self.langkah % RUSAK_SETIAP != RUSAK_SETIAP - 1:
            return False
        rng = random.Random(f"{self.seed}:rusak:{self.langkah}:{self._kunci(lintang, bujur)}")
        return rng.random() < 1 / 6

    def aktual(self, lintang, bujur, mulai, akhir):
        """Isi endpoint archive: jam setelah tanggal_data atau setelah waktu sekarang belum ada (null)."""
        awal = datetime(mulai.year, mulai.month, mulai.day, tzinfo=UTC)
        jumlah = ((akhir - mulai).days + 1) * 24
        waktu = [awal + timedelta(hours=i) for i in range(jumlah)]
        terbaru = self.tanggal_data
        sementara = (
            terbaru < self.akhir
            and random.Random(f"{self.seed}:sementara:{self._kunci(lintang, bujur)}:{terbaru}").random()
            < PELAPORAN_SEMENTARA
        )
        rusak = self._rusak(lintang, bujur)

        deret = {v: [] for v in VARIABEL}
        for t in waktu:
            if t.date() > terbaru or t > self.sekarang:
                nilai = {v: None for v in VARIABEL}
            else:
                nilai = self._nilai(lintang, bujur, t)
                if t.date() == terbaru and sementara and nilai["temperature_2m"] is not None:
                    nilai["temperature_2m"] = round(nilai["temperature_2m"] + 0.6, 1)
                if rusak and t.date() == terbaru and t.hour == 12:
                    nilai["relative_humidity_2m"] = 150
            for v in VARIABEL:
                deret[v].append(nilai[v])
        return self._badan(lintang, bujur, waktu, deret)

    def prakiraan(self, lintang, bujur, hari_prakiraan):
        """Isi endpoint forecast: mulai 00:00 UTC pada tanggal akhir, sepanjang hari_prakiraan hari."""
        awal = datetime(self.akhir.year, self.akhir.month, self.akhir.day, tzinfo=UTC)
        waktu = [awal + timedelta(hours=i) for i in range(hari_prakiraan * 24)]
        deret = {v: [] for v in VARIABEL}
        for i, t in enumerate(waktu):
            benar = self._nilai(lintang, bujur, t)
            rng = random.Random(f"{self.seed}:prakiraan:{self._kunci(lintang, bujur)}:{t.isoformat()}:{self.langkah}")
            hujan = max(0.0, benar["precipitation"] * (1 + rng.gauss(0, 0.3 + 0.01 * i)))
            if benar["precipitation"] == 0 and rng.random() < 0.04:
                hujan = rng.uniform(0.2, 3.0)
            nilai = {
                "temperature_2m": round(benar["temperature_2m"] + rng.gauss(0, 0.5 + 0.04 * i), 1),
                "relative_humidity_2m": int(min(100, max(40, round(benar["relative_humidity_2m"] + rng.gauss(0, 4))))),
                "precipitation": round(hujan, 1),
                "wind_speed_10m": round(max(0.0, benar["wind_speed_10m"] + rng.gauss(0, 2)), 1),
                "pressure_msl": round(benar["pressure_msl"] + rng.gauss(0, 1.0), 1),
                "weather_code": _kode(hujan, 0.5, rng.random()) if hujan else benar["weather_code"] % 4,
            }
            for v in VARIABEL:
                deret[v].append(nilai[v])
        return self._badan(lintang, bujur, waktu, deret)
