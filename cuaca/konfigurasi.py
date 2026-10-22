"""Pengaturan pipeline dalam satu objek yang dikirim ke tiap tahap."""

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

URL_PRAKIRAAN_BAWAAN = "https://api.open-meteo.com/v1/forecast"
URL_ARSIP_BAWAAN = "https://archive-api.open-meteo.com/v1/archive"
WIB = timezone(timedelta(hours=7))
TANGGAL_MIN = date(2000, 1, 1)
MODE = ("semua", "aktual", "prakiraan")

VARIABEL = {
    "temperature_2m": "suhu_c",
    "relative_humidity_2m": "kelembapan_persen",
    "precipitation": "curah_hujan_mm",
    "wind_speed_10m": "kecepatan_angin_kmh",
    "pressure_msl": "tekanan_hpa",
    "weather_code": "kode_cuaca",
}
KOLOM_UKUR = tuple(VARIABEL.values())

ZONA = {"Asia/Jakarta": 7, "Asia/Makassar": 8, "Asia/Jayapura": 9}

KODE_CUACA = {
    0: ("Cerah", "cerah"),
    1: ("Sebagian besar cerah", "cerah"),
    2: ("Berawan sebagian", "berawan"),
    3: ("Mendung", "berawan"),
    45: ("Kabut", "kabut"),
    48: ("Kabut embun beku", "kabut"),
    51: ("Gerimis ringan", "gerimis"),
    53: ("Gerimis sedang", "gerimis"),
    55: ("Gerimis lebat", "gerimis"),
    56: ("Gerimis beku ringan", "gerimis"),
    57: ("Gerimis beku lebat", "gerimis"),
    61: ("Hujan ringan", "hujan"),
    63: ("Hujan sedang", "hujan"),
    65: ("Hujan lebat", "hujan"),
    66: ("Hujan beku ringan", "hujan"),
    67: ("Hujan beku lebat", "hujan"),
    71: ("Salju ringan", "salju"),
    73: ("Salju sedang", "salju"),
    75: ("Salju lebat", "salju"),
    77: ("Butiran salju", "salju"),
    80: ("Hujan lokal ringan", "hujan"),
    81: ("Hujan lokal sedang", "hujan"),
    82: ("Hujan lokal deras", "hujan"),
    85: ("Salju lokal ringan", "salju"),
    86: ("Salju lokal lebat", "salju"),
    95: ("Badai petir", "badai"),
    96: ("Badai petir dengan hujan es ringan", "badai"),
    99: ("Badai petir dengan hujan es lebat", "badai"),
}


@dataclass(frozen=True)
class Lokasi:
    nama: str
    provinsi: str
    lintang: float
    bujur: float
    zona: str

    @property
    def selisih(self):
        """Selisih waktu lokal terhadap UTC."""
        return timedelta(hours=ZONA[self.zona])


LOKASI = (
    Lokasi("Jakarta", "DKI Jakarta", -6.2088, 106.8456, "Asia/Jakarta"),
    Lokasi("Bandung", "Jawa Barat", -6.9175, 107.6191, "Asia/Jakarta"),
    Lokasi("Semarang", "Jawa Tengah", -6.9667, 110.4167, "Asia/Jakarta"),
    Lokasi("Yogyakarta", "DI Yogyakarta", -7.7956, 110.3695, "Asia/Jakarta"),
    Lokasi("Surabaya", "Jawa Timur", -7.2575, 112.7521, "Asia/Jakarta"),
    Lokasi("Medan", "Sumatera Utara", 3.5952, 98.6722, "Asia/Jakarta"),
    Lokasi("Palembang", "Sumatera Selatan", -2.9761, 104.7754, "Asia/Jakarta"),
    Lokasi("Pontianak", "Kalimantan Barat", -0.0263, 109.3425, "Asia/Jakarta"),
    Lokasi("Balikpapan", "Kalimantan Timur", -1.2379, 116.8529, "Asia/Makassar"),
    Lokasi("Makassar", "Sulawesi Selatan", -5.1477, 119.4327, "Asia/Makassar"),
    Lokasi("Denpasar", "Bali", -8.6705, 115.2126, "Asia/Makassar"),
    Lokasi("Jayapura", "Papua", -2.5337, 140.7181, "Asia/Jayapura"),
)


def hari_ini():
    """Tanggal hari ini di WIB, zona waktu tempat DAG dijadwalkan."""
    return datetime.now(WIB).date()


@dataclass(frozen=True)
class Konfigurasi:
    """Pengaturan untuk satu kali jalan pipeline.

    Semua satuan waktu dalam detik. db None berarti PGDATABASE, folder_mentah None berarti
    cache tidak dipakai, dan lokasi kosong berarti semua kota. Data aktual diambil untuk rentang
    mulai sampai selesai, atau hari_terakhir hari terakhir jika mulai kosong. hari_prakiraan
    bernilai 1 sampai 16. Permintaan yang gagal dicoba sampai maks_coba kali, dengan jeda yang
    digandakan tiap percobaan (backoff eksponensial). jeda_permintaan adalah jeda antar
    permintaan supaya server sumber tidak terbebani.
    """

    url_prakiraan: str = field(default_factory=lambda: os.getenv("OPEN_METEO_FORECAST_URL") or URL_PRAKIRAAN_BAWAAN)
    url_arsip: str = field(default_factory=lambda: os.getenv("OPEN_METEO_ARCHIVE_URL") or URL_ARSIP_BAWAAN)
    db: str | None = None
    folder_output: str = "output"
    folder_mentah: str | None = "data/mentah"
    mode: str = "semua"
    mulai: date | None = None
    selesai: date | None = None
    hari_terakhir: int = 7
    hari_prakiraan: int = 3
    jendela_hari: int = 90
    lokasi: tuple = ()
    timeout: float = 60.0
    maks_coba: int = 3
    jeda: float = 1.0
    jeda_permintaan: float = 1.0

    def rentang(self, hari=None):
        """Rentang (mulai, selesai) data aktual yang akan diambil."""
        selesai = self.selesai or hari or hari_ini()
        mulai = self.mulai or selesai - timedelta(days=self.hari_terakhir - 1)
        if mulai > selesai:
            raise ValueError(f"mulai ({mulai}) setelah selesai ({selesai})")
        if mulai < TANGGAL_MIN:
            raise ValueError(f"mulai ({mulai}) sebelum {TANGGAL_MIN}, batas awal dim_tanggal")
        return mulai, selesai

    def lokasi_terpilih(self):
        """Daftar Lokasi yang akan diambil. Kosong berarti semua."""
        if not self.lokasi:
            return LOKASI
        pilihan = [p.lower() for p in self.lokasi]
        terpilih = tuple(k for k in LOKASI if any(p in k.nama.lower() for p in pilihan))
        if not terpilih:
            raise ValueError(f"tidak ada lokasi yang cocok dengan {list(self.lokasi)}")
        return terpilih
