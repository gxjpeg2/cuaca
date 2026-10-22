"""Mengambil data cuaca per jam dari Open-Meteo: data aktual (archive) dan prakiraan (forecast), per kota."""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from .konfigurasi import MODE, VARIABEL, hari_ini
from .logging_setup import timed

logger = logging.getLogger(__name__)

STATUS_COBA_ULANG = {429, 500, 502, 503, 504}
USER_AGENT = "cuaca-etl/1.0 (proyek portofolio data engineering)"
HARI_FINAL = 90


class ExtractError(Exception):
    """Extract gagal dan pipeline berhenti."""


def jendela_tanggal(mulai, selesai, hari):
    """Pecah rentang tanggal menjadi potongan berurutan sepanjang `hari` hari."""
    hasil = []
    awal = mulai
    while awal <= selesai:
        akhir = min(awal + timedelta(days=hari - 1), selesai)
        hasil.append((awal, akhir))
        awal = akhir + timedelta(days=1)
    return hasil


def _alasan_dari(respons):
    """Pesan error dari badan respons Open-Meteo ({"error": true, "reason": "..."}), jika ada."""
    try:
        data = respons.json()
    except ValueError:
        return ""
    if isinstance(data, dict) and data.get("reason"):
        return str(data["reason"])[:200]
    return ""


def _baca_badan(respons, label):
    """Pastikan respons berbentuk {"hourly": {"time": [...], <variabel>: [...]}} dengan panjang yang sama."""
    try:
        data = respons.json()
    except ValueError:
        logger.error("Isi respons (%s) bukan JSON yang valid", label)
        raise ExtractError("Respons API bukan JSON") from None

    if isinstance(data, dict) and data.get("error"):
        logger.error("Sumber melaporkan error (%s): %s", label, data.get("reason"))
        raise ExtractError(f"Sumber melaporkan error: {data.get('reason')}")

    jam = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(jam, dict) or not isinstance(jam.get("time"), list):
        logger.error("Bentuk respons tidak sesuai (%s): butuh 'hourly' dengan daftar 'time'", label)
        raise ExtractError("Bentuk respons API tidak sesuai")
    for nama in VARIABEL:
        nilai = jam.get(nama)
        if not isinstance(nilai, list) or len(nilai) != len(jam["time"]):
            logger.error("Bentuk respons tidak sesuai (%s): '%s' hilang atau panjangnya beda dari 'time'", label, nama)
            raise ExtractError("Bentuk respons API tidak sesuai")
    return data


class _Klien:
    """Sesi HTTP yang memberi jeda antar permintaan dan mencoba ulang gangguan sementara."""

    def __init__(self, konfig):
        self.konfig = konfig
        self.permintaan = 0
        self.sesi = requests.Session()
        self.sesi.headers.update({"User-Agent": USER_AGENT})

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.sesi.close()

    def ambil(self, alamat, params, label):
        """Kirim satu permintaan. Gangguan sementara dicoba ulang, error lain langsung berhenti."""
        konfig = self.konfig
        if self.permintaan:
            time.sleep(konfig.jeda_permintaan)
        self.permintaan += 1

        penyebab = "tidak diketahui"
        for percobaan in range(1, konfig.maks_coba + 1):
            try:
                respons = self.sesi.get(alamat, params=params, timeout=konfig.timeout)
            except (requests.Timeout, requests.ConnectionError) as e:
                penyebab = type(e).__name__
                logger.warning("Percobaan %d/%d gagal (%s): %s", percobaan, konfig.maks_coba, label, penyebab)
                logger.debug("Detail kegagalan koneksi", exc_info=True)
            except requests.RequestException as e:
                logger.error("Permintaan tidak bisa dikirim: %s", type(e).__name__)
                raise ExtractError(f"Permintaan tidak valid: {type(e).__name__}") from e
            else:
                if respons.status_code in STATUS_COBA_ULANG:
                    penyebab = f"HTTP {respons.status_code}"
                    logger.warning("Percobaan %d/%d gagal (%s): %s", percobaan, konfig.maks_coba, label, penyebab)
                elif respons.status_code >= 400:
                    alasan = _alasan_dari(respons)
                    logger.error(
                        "Sumber menolak permintaan dengan HTTP %d (%s), tidak dicoba ulang. %s",
                        respons.status_code, label, alasan,
                    )
                    detail = f": {alasan}" if alasan else ""
                    raise ExtractError(f"Sumber mengembalikan HTTP {respons.status_code}{detail}")
                else:
                    return _baca_badan(respons, label)

            if percobaan < konfig.maks_coba:
                tunggu = konfig.jeda * 2 ** (percobaan - 1)
                logger.info("Menunggu %.1f detik sebelum percobaan berikutnya", tunggu)
                time.sleep(tunggu)

        logger.error("%s gagal setelah %d percobaan (%s)", label, konfig.maks_coba, penyebab)
        raise ExtractError(f"Sumber tidak bisa dijangkau setelah {konfig.maks_coba} percobaan ({penyebab})")


def _params_dasar(lokasi):
    return {
        "latitude": lokasi.lintang,
        "longitude": lokasi.bujur,
        "hourly": ",".join(VARIABEL),
        "timezone": "UTC",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }


def _slug(nama):
    return nama.lower().replace(" ", "_")


def _path_cache(konfig, lokasi, awal, akhir):
    if not konfig.folder_mentah:
        return None
    return Path(konfig.folder_mentah) / f"arsip_{_slug(lokasi.nama)}_{awal}_{akhir}.json"


def _ambil_arsip(klien, konfig, lokasi, awal, akhir, hari, info):
    """Data aktual satu kota untuk satu potongan tanggal. Potongan lama dibaca dari cache jika ada."""
    final = akhir < hari - timedelta(days=HARI_FINAL)
    cache = _path_cache(konfig, lokasi, awal, akhir) if final else None

    if cache is not None and cache.exists():
        try:
            isi = json.loads(cache.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            logger.warning("Cache rusak, diambil ulang: %s", cache)
        else:
            if isinstance(isi, dict) and isinstance(isi.get("hourly"), dict):
                info["dari_cache"] += 1
                return isi

    params = _params_dasar(lokasi)
    params.update(start_date=awal.isoformat(), end_date=akhir.isoformat())
    isi = klien.ambil(konfig.url_arsip, params, f"aktual {lokasi.nama} {awal}..{akhir}")

    if cache is not None:
        try:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(isi, ensure_ascii=False), encoding="utf-8")
        except OSError:
            logger.warning("Cache gagal ditulis ke %s, pipeline lanjut", cache, exc_info=True)
    return isi


def _ambil_prakiraan(klien, konfig, lokasi):
    """Prakiraan satu kota. Tidak pernah dicache karena selalu berubah."""
    params = _params_dasar(lokasi)
    params.update(forecast_days=konfig.hari_prakiraan, past_days=0)
    return klien.ambil(konfig.url_prakiraan, params, f"prakiraan {lokasi.nama}")


@timed
def extract(konfig, info=None, hari=None, sekarang=None):
    """Ambil respons mentah per kota. Hasilnya daftar payload.

    Tiap payload berisi jenis ("aktual" atau "prakiraan"), lokasi, diambil_pada (UTC, dibulatkan
    ke jam), dan isi (respons Open-Meteo apa adanya). info (opsional) diisi lokasi, permintaan,
    dari_cache, mulai, selesai, dan waktu_prediksi.
    """
    if konfig.mode not in MODE:
        raise ExtractError(f"mode harus salah satu dari {MODE}, bukan {konfig.mode!r}")
    if not 1 <= konfig.hari_prakiraan <= 16:
        raise ExtractError(f"hari_prakiraan harus 1 sampai 16, bukan {konfig.hari_prakiraan}")

    hari = hari or hari_ini()
    sekarang = sekarang or datetime.now(timezone.utc)
    diambil_pada = sekarang.replace(minute=0, second=0, microsecond=0)

    try:
        daftar_lokasi = konfig.lokasi_terpilih()
        mulai, selesai = konfig.rentang(hari)
    except ValueError as e:
        logger.error("Pengaturan tidak valid: %s", e)
        raise ExtractError(str(e)) from None

    ambil_aktual = konfig.mode in ("semua", "aktual")
    ambil_prakiraan = konfig.mode in ("semua", "prakiraan")
    potongan = jendela_tanggal(mulai, selesai, konfig.jendela_hari) if ambil_aktual else []

    statistik = {"dari_cache": 0}
    payload = []
    with _Klien(konfig) as klien:
        logger.info(
            "%d lokasi, mode %s%s",
            len(daftar_lokasi), konfig.mode,
            f", data aktual {mulai} sampai {selesai} ({len(potongan)} potongan per lokasi)" if ambil_aktual else "",
        )
        for lokasi in daftar_lokasi:
            for awal, akhir in potongan:
                isi = _ambil_arsip(klien, konfig, lokasi, awal, akhir, hari, statistik)
                logger.info("aktual %s %s..%s: %d jam", lokasi.nama, awal, akhir, len(isi["hourly"]["time"]))
                payload.append({"jenis": "aktual", "lokasi": lokasi, "diambil_pada": diambil_pada, "isi": isi})
            if ambil_prakiraan:
                isi = _ambil_prakiraan(klien, konfig, lokasi)
                logger.info("prakiraan %s: %d jam", lokasi.nama, len(isi["hourly"]["time"]))
                payload.append({"jenis": "prakiraan", "lokasi": lokasi, "diambil_pada": diambil_pada, "isi": isi})
        permintaan = klien.permintaan

    if info is not None:
        info.update(
            lokasi=len(daftar_lokasi), permintaan=permintaan, dari_cache=statistik["dari_cache"],
            mulai=mulai.isoformat() if ambil_aktual else None,
            selesai=selesai.isoformat() if ambil_aktual else None,
            waktu_prediksi=diambil_pada.isoformat() if ambil_prakiraan else None,
        )
    logger.info("Total %d respons dari %d permintaan (%d dari cache)", len(payload), permintaan, statistik["dari_cache"])
    return payload
