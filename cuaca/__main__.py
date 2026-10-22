"""Titik masuk: python -m cuaca --help"""

import argparse
import json
import logging
import sys
import time
from datetime import date

from .konfigurasi import MODE, Konfigurasi
from .logging_setup import PENGHITUNG, folder_log, setup_logging
from .pipeline import run
from .riwayat import catat_run, waktu_sekarang

logger = logging.getLogger("cuaca")


def _tipe(jenis, minimal):
    """Buat tipe argumen untuk argparse: angka dengan batas bawah dan pesan error yang ramah."""

    def ubah(teks):
        try:
            nilai = jenis(teks)
        except ValueError:
            raise argparse.ArgumentTypeError(f"bukan angka: {teks!r}") from None
        if nilai < minimal:
            raise argparse.ArgumentTypeError(f"minimal {minimal}, bukan {teks}")
        return nilai

    return ubah


def _tanggal(teks):
    try:
        return date.fromisoformat(teks)
    except ValueError:
        raise argparse.ArgumentTypeError(f"format YYYY-MM-DD, bukan {teks!r}") from None


def main(argv=None):
    asal = Konfigurasi()
    parser = argparse.ArgumentParser(
        prog="cuaca", description="Pipeline ETL cuaca per jam dari Open-Meteo ke PostgreSQL"
    )
    parser.add_argument(
        "--url-prakiraan", default=asal.url_prakiraan,
        help="alamat lengkap endpoint forecast (bawaan: OPEN_METEO_FORECAST_URL atau api.open-meteo.com)",
    )
    parser.add_argument(
        "--url-arsip", default=asal.url_arsip,
        help="alamat lengkap endpoint archive (bawaan: OPEN_METEO_ARCHIVE_URL atau archive-api.open-meteo.com)",
    )
    parser.add_argument(
        "--url-simulasi", default=None,
        help="alamat dasar server simulasi, mengatur kedua endpoint sekaligus, misalnya http://127.0.0.1:8000",
    )
    parser.add_argument("--db", default=asal.db, help="nama database PostgreSQL (bawaan: PGDATABASE atau cuaca)")
    parser.add_argument("--output", default=asal.folder_output, help="folder file CSV hasil")
    parser.add_argument("--mentah", default=asal.folder_mentah, help="folder cache respons mentah")
    parser.add_argument("--tanpa-mentah", action="store_true", help="jangan pakai cache respons mentah")
    parser.add_argument(
        "--mode", choices=MODE, default=asal.mode,
        help="semua = aktual dan prakiraan, aktual = hanya data aktual (untuk backfill), prakiraan = hanya prakiraan",
    )
    parser.add_argument("--mulai", type=_tanggal, default=None, help="tanggal awal data aktual YYYY-MM-DD (bawaan: --hari terakhir)")
    parser.add_argument("--selesai", type=_tanggal, default=None, help="tanggal akhir data aktual YYYY-MM-DD (bawaan: hari ini WIB)")
    parser.add_argument("--hari", type=_tipe(int, 1), default=asal.hari_terakhir, help="jumlah hari terakhir data aktual kalau --mulai kosong")
    parser.add_argument("--hari-prakiraan", type=int, choices=range(1, 17), metavar="1-16", default=asal.hari_prakiraan, help="panjang prakiraan, dalam hari")
    parser.add_argument("--jendela", type=_tipe(int, 1), default=asal.jendela_hari, help="hari data aktual per permintaan")
    parser.add_argument("--lokasi", default="", help="potongan nama kota, pisahkan dengan koma (bawaan: semua)")
    parser.add_argument("--timeout", type=_tipe(float, 0.1), default=asal.timeout, help="detik tunggu per permintaan")
    parser.add_argument("--maks-coba", type=_tipe(int, 1), default=asal.maks_coba, help="percobaan per permintaan")
    parser.add_argument("--jeda", type=_tipe(float, 0), default=asal.jeda, help="detik tunggu sebelum coba ulang")
    parser.add_argument(
        "--jeda-permintaan", type=_tipe(float, 0), default=asal.jeda_permintaan,
        help="detik antar permintaan ke sumber",
    )
    parser.add_argument("--level", default=None, help="level log di layar (default INFO)")
    parser.add_argument("--json", action="store_true", help="tulis log dalam format JSON")
    parser.add_argument(
        "--log-stdout", action="store_true",
        help="log hanya ke stdout, tanpa file log (untuk container)",
    )
    args = parser.parse_args(argv)
    if args.mulai and args.selesai and args.mulai > args.selesai:
        parser.error("--mulai tidak boleh setelah --selesai")

    url_prakiraan, url_arsip = args.url_prakiraan, args.url_arsip
    if args.url_simulasi:
        dasar = args.url_simulasi.rstrip("/")
        url_prakiraan, url_arsip = f"{dasar}/v1/forecast", f"{dasar}/v1/archive"

    konfig = Konfigurasi(
        url_prakiraan=url_prakiraan,
        url_arsip=url_arsip,
        db=args.db,
        folder_output=args.output,
        folder_mentah=None if args.tanpa_mentah else args.mentah,
        mode=args.mode,
        mulai=args.mulai,
        selesai=args.selesai,
        hari_terakhir=args.hari,
        hari_prakiraan=args.hari_prakiraan,
        jendela_hari=args.jendela,
        lokasi=tuple(k.strip() for k in args.lokasi.split(",") if k.strip()),
        timeout=args.timeout,
        maks_coba=args.maks_coba,
        jeda=args.jeda,
        jeda_permintaan=args.jeda_permintaan,
    )

    try:
        run_id = setup_logging(
            console_level=args.level,
            json_logs=True if args.json else None,
            log_stdout=True if args.log_stdout else None,
        )
    except ValueError as e:
        parser.error(str(e))

    statistik = {}
    mulai = time.perf_counter()
    kode = run(konfig, statistik)

    ringkasan = {
        "run_id": run_id,
        "waktu_selesai": waktu_sekarang(),
        "kode_keluar": kode,
        "status": "sukses" if kode == 0 else "gagal",
        "durasi_detik": round(time.perf_counter() - mulai, 3),
        "mode": konfig.mode,
        "lokasi": statistik.get("lokasi"),
        "mulai": statistik.get("mulai"),
        "selesai": statistik.get("selesai"),
        "waktu_prediksi": statistik.get("waktu_prediksi"),
        "aktual_valid": statistik.get("aktual_valid"),
        "prakiraan_valid": statistik.get("prakiraan_valid"),
        "baris_ditolak": statistik.get("baris_ditolak"),
        "tanggal_terbaru": statistik.get("tanggal_terbaru"),
        "cuaca_baru": statistik.get("cuaca_baru"),
        "cuaca_direvisi": statistik.get("cuaca_direvisi"),
        "cuaca_terisi": statistik.get("cuaca_terisi"),
        "cuaca_sama": statistik.get("cuaca_sama"),
        "prakiraan_baru": statistik.get("prakiraan_baru"),
        "prakiraan_sama": statistik.get("prakiraan_sama"),
        "jumlah_log": dict(PENGHITUNG.hitung),
    }
    logger.info("Ringkasan run: %s", json.dumps(ringkasan, ensure_ascii=False))
    if folder_log() is not None:
        try:
            catat_run(folder_log(), ringkasan)
        except OSError as e:
            logger.warning("Riwayat run gagal ditulis: %s", e)
    return kode


if __name__ == "__main__":
    sys.exit(main())
