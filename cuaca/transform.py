"""Validasi dan pembersihan: array per jam dari Open-Meteo jadi satu baris per kota per jam."""

import json
import logging
from collections import Counter
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from .konfigurasi import KODE_CUACA, VARIABEL
from .logging_setup import timed

logger = logging.getLogger(__name__)

MAX_REJECT_RATIO = 0.5
MAX_CONTOH_LOG = 10
FORMAT_WAKTU = "%Y-%m-%dT%H:%M"
SATU_DESIMAL = Decimal("0.1")

BATAS = {
    "suhu_c": (Decimal(-90), Decimal(60)),
    "kelembapan_persen": (Decimal(0), Decimal(100)),
    "curah_hujan_mm": (Decimal(0), Decimal(500)),
    "kecepatan_angin_kmh": (Decimal(0), Decimal(400)),
    "tekanan_hpa": (Decimal(850), Decimal(1100)),
}


class TransformError(Exception):
    """Transform gagal dan pipeline berhenti."""


def _desimal(nilai):
    """Ubah ke Decimal yang berhingga, atau None jika bukan angka."""
    if isinstance(nilai, bool):
        return None
    try:
        angka = Decimal(str(nilai).strip())
    except InvalidOperation:
        return None
    return angka if angka.is_finite() else None


def _ubah_nilai(kolom, nilai):
    """Hasilnya (nilai_bersih, alasan_tolak). nilai None berarti tidak ada data, bukan kesalahan."""
    if nilai is None:
        return None, None
    angka = _desimal(nilai)
    if angka is None:
        return None, f"{kolom} bukan angka ({nilai!r})"

    if kolom == "kode_cuaca":
        if angka != angka.to_integral_value() or int(angka) not in KODE_CUACA:
            return None, f"kode cuaca tidak dikenal ({nilai!r})"
        return int(angka), None

    bawah, atas = BATAS[kolom]
    if not bawah <= angka <= atas:
        return None, f"{kolom} di luar batas ({angka:g})"
    if kolom == "kelembapan_persen":
        return int(angka.quantize(Decimal(1), rounding=ROUND_HALF_UP)), None
    return angka.quantize(SATU_DESIMAL, rounding=ROUND_HALF_UP), None


def _tolak(jenis, lokasi, waktu, alasan, nilai):
    return {
        "jenis": jenis,
        "lokasi": lokasi,
        "waktu_utc": str(waktu),
        "nilai_mentah": str(nilai)[:500],
        "alasan": alasan,
    }


def ubah_payload(payload):
    """Pecah satu respons jadi (bersih, ditolak, jumlah_jam_kosong, jumlah_terlewat).

    Jam terlewat adalah jam prakiraan yang sudah lewat saat data diambil.
    """
    lokasi = payload["lokasi"]
    jenis = payload["jenis"]
    prediksi = payload["diambil_pada"]
    jam = payload["isi"]["hourly"]

    bersih, ditolak, kosong, terlewat = [], [], 0, 0
    for i, teks in enumerate(jam["time"]):
        mentah = {kolom: jam[variabel][i] for variabel, kolom in VARIABEL.items()}
        try:
            waktu = datetime.strptime(str(teks), FORMAT_WAKTU).replace(tzinfo=timezone.utc)
        except ValueError:
            ditolak.append(_tolak(jenis, lokasi.nama, teks, "waktu tidak valid", json.dumps(mentah)))
            continue

        if all(v is None for v in mentah.values()):
            kosong += 1
            continue
        if jenis == "prakiraan" and waktu < prediksi:
            terlewat += 1
            continue

        nilai, alasan = {}, []
        for kolom, v in mentah.items():
            hasil, sebab = _ubah_nilai(kolom, v)
            nilai[kolom] = hasil
            if sebab:
                alasan.append(sebab)
        if alasan:
            ditolak.append(_tolak(jenis, lokasi.nama, teks, "; ".join(alasan), json.dumps(mentah)))
            continue

        lokal = waktu + lokasi.selisih
        baris = {
            "jenis": jenis,
            "lokasi": lokasi.nama,
            "waktu_utc": waktu,
            "tanggal_lokal": lokal.date(),
            "jam_lokal": lokal.hour,
            **nilai,
        }
        if jenis == "prakiraan":
            baris["waktu_prediksi"] = prediksi
            baris["jarak_jam"] = int((waktu - prediksi).total_seconds() // 3600)
        bersih.append(baris)
    return bersih, ditolak, kosong, terlewat


@timed
def transform(payload):
    """Hasilnya (bersih, ditolak). bersih berisi kunci aktual, prakiraan, dan lokasi.

    Pipeline berhenti jika rasio jam ditolak melewati MAX_REJECT_RATIO.
    """
    aktual, prakiraan, ditolak = [], [], []
    lokasi_info = {}
    terlihat = set()
    alasan_hitung = Counter()
    kosong_total = terlewat_total = 0

    def catat_tolak(item):
        ditolak.append(item)
        alasan_hitung[item["alasan"].split(";")[0].split(" (")[0]] += 1
        if len(ditolak) <= MAX_CONTOH_LOG:
            logger.warning(
                "Jam %s/%s/%s ditolak: %s", item["jenis"], item["lokasi"], item["waktu_utc"], item["alasan"],
            )

    for p in payload:
        lokasi = p["lokasi"]
        elevasi = _desimal(p["isi"].get("elevation"))
        lokasi_info[lokasi.nama] = {
            "lokasi": lokasi,
            "elevasi_m": elevasi.quantize(SATU_DESIMAL, rounding=ROUND_HALF_UP) if elevasi is not None else None,
        }

        valid, tolak, kosong, terlewat = ubah_payload(p)
        kosong_total += kosong
        terlewat_total += terlewat
        for item in tolak:
            catat_tolak(item)
        for baris in valid:
            kunci = (baris["jenis"], baris["lokasi"], baris["waktu_utc"])
            if kunci in terlihat:
                catat_tolak(_tolak(baris["jenis"], baris["lokasi"], baris["waktu_utc"], "duplikat", baris["suhu_c"]))
                continue
            terlihat.add(kunci)
            (aktual if baris["jenis"] == "aktual" else prakiraan).append(baris)

    valid_total = len(aktual) + len(prakiraan)
    total = valid_total + len(ditolak)
    rasio = len(ditolak) / total if total else 0.0
    logger.info(
        "Hasil: %d aktual valid, %d prakiraan valid, %d ditolak, %d jam kosong, %d jam prakiraan terlewat (%.0f%% ditolak)",
        len(aktual), len(prakiraan), len(ditolak), kosong_total, terlewat_total, rasio * 100,
    )
    if len(ditolak) > MAX_CONTOH_LOG:
        logger.info(
            "Hanya %d contoh pertama dicatat satu per satu; semua jam ditolak ada di tabel karantina",
            MAX_CONTOH_LOG,
        )
        for sebab, jumlah in alasan_hitung.most_common():
            logger.warning("Ditolak karena %s: %d jam", sebab, jumlah)

    if total == 0:
        logger.error("Tidak ada satu jam pun yang berisi data (%d jam kosong)", kosong_total)
        raise TransformError("Tidak ada data cuaca di rentang ini")
    if rasio > MAX_REJECT_RATIO:
        logger.error(
            "Rasio ditolak %.0f%% melewati batas %.0f%%, pipeline dihentikan", rasio * 100, MAX_REJECT_RATIO * 100,
        )
        raise TransformError("Terlalu banyak jam ditolak")

    return {"aktual": aktual, "prakiraan": prakiraan, "lokasi": lokasi_info}, ditolak
