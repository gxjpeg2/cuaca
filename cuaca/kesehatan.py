"""Cek kesehatan pipeline dari riwayat run (python -m cuaca.kesehatan).

Kode keluar: 0 OK, 1 WARNING, 2 CRITICAL.
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

from .riwayat import FORMAT_WAKTU, baca_riwayat

URUTAN = {"OK": 0, "WARNING": 1, "CRITICAL": 2}


def _waktu(teks):
    return datetime.strptime(teks, FORMAT_WAKTU).replace(tzinfo=timezone.utc)


def _jumlah_valid(run):
    """Jumlah jam valid (aktual ditambah prakiraan) di satu run, atau None jika tidak tercatat."""
    aktual, prakiraan = run.get("aktual_valid"), run.get("prakiraan_valid")
    if aktual is None and prakiraan is None:
        return None
    return (aktual or 0) + (prakiraan or 0)


def periksa(riwayat, sekarang, maks_umur_jam=26, maks_rasio=0.2, maks_turun=0.3, maks_umur_data_hari=8):
    """Hasilnya daftar (level, pesan) dengan level OK, WARNING, atau CRITICAL.

    maks_umur_data_hari bawaannya 8, karena data aktual (archive) di sumbernya tertinggal
    beberapa hari dari hari ini.
    """
    if not riwayat:
        return [("CRITICAL", "Belum ada riwayat run: pipeline belum pernah jalan?")]

    hasil = []
    terakhir = riwayat[-1]
    run_id = terakhir.get("run_id", "?")
    mode = terakhir.get("mode") or "semua"

    if terakhir.get("kode_keluar") != 0:
        hasil.append(("CRITICAL", f"Run terakhir ({run_id}) GAGAL, kode keluar {terakhir.get('kode_keluar')}"))
    else:
        hasil.append(("OK", f"Run terakhir ({run_id}) sukses"))

    sukses = [r for r in riwayat if r.get("kode_keluar") == 0]
    if not sukses:
        hasil.append(("CRITICAL", "Belum pernah ada run yang sukses"))
    else:
        try:
            umur = (sekarang - _waktu(sukses[-1]["waktu_selesai"])).total_seconds() / 3600
        except (KeyError, ValueError):
            hasil.append(("CRITICAL", "Waktu run sukses terakhir tidak terbaca di riwayat"))
        else:
            if umur > maks_umur_jam:
                hasil.append(("CRITICAL", f"Data BASI: sukses terakhir {umur:.1f} jam lalu (batas {maks_umur_jam:g} jam)"))
            else:
                hasil.append(("OK", f"Data segar: sukses terakhir {umur:.1f} jam lalu (batas {maks_umur_jam:g} jam)"))

    valid = _jumlah_valid(terakhir)
    ditolak = terakhir.get("baris_ditolak")
    if valid is not None and ditolak is not None and valid + ditolak > 0:
        rasio = ditolak / (valid + ditolak)
        if rasio > maks_rasio:
            hasil.append(("WARNING", f"Jam ditolak {rasio:.0%} (batas {maks_rasio:.0%}): {ditolak} dari {valid + ditolak}"))
        else:
            hasil.append(("OK", f"Jam ditolak {rasio:.0%} (batas {maks_rasio:.0%})"))

    tanggal_data = terakhir.get("tanggal_terbaru")
    if tanggal_data and terakhir.get("kode_keluar") == 0:
        try:
            umur_hari = ((sekarang + timedelta(hours=7)).date() - date.fromisoformat(tanggal_data)).days
        except ValueError:
            hasil.append(("WARNING", f"Tanggal data aktual terbaru tidak terbaca: {tanggal_data!r}"))
        else:
            if umur_hari > maks_umur_data_hari:
                hasil.append(
                    (
                        "WARNING",
                        f"Data aktual terbaru bertanggal {tanggal_data}, {umur_hari} hari lalu "
                        f"(batas {maks_umur_data_hari} hari): sumber belum memperbarui?",
                    )
                )
            else:
                hasil.append(("OK", f"Data aktual terbaru bertanggal {tanggal_data} ({umur_hari} hari lalu)"))

    if terakhir.get("kode_keluar") == 0 and mode in ("semua", "prakiraan"):
        if not terakhir.get("prakiraan_valid"):
            hasil.append(("WARNING", "Run terakhir tidak menghasilkan satu pun prakiraan valid"))
        else:
            hasil.append(("OK", f"Prakiraan valid: {terakhir['prakiraan_valid']} jam"))

    if terakhir.get("kode_keluar") == 0:
        sebanding = [r for r in sukses if (r.get("mode") or "semua") == mode]
        if len(sebanding) >= 2:
            sebelumnya = _jumlah_valid(sebanding[-2])
            sekarang_valid = _jumlah_valid(sebanding[-1])
            if sebelumnya and sekarang_valid is not None:
                turun = (sebelumnya - sekarang_valid) / sebelumnya
                if turun > maks_turun:
                    hasil.append(
                        (
                            "WARNING",
                            f"Jam valid turun {turun:.0%} dibanding run sukses sebelumnya "
                            f"({sebelumnya} -> {sekarang_valid}, batas {maks_turun:.0%})",
                        )
                    )
                else:
                    hasil.append(("OK", f"Jumlah jam valid stabil ({sebelumnya} -> {sekarang_valid})"))

    jumlah_log = terakhir.get("jumlah_log") or {}
    n_error = jumlah_log.get("ERROR", 0) + jumlah_log.get("CRITICAL", 0)
    if n_error:
        hasil.append(("WARNING", f"{n_error} log ERROR/CRITICAL di run terakhir"))

    return hasil


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="cuaca.kesehatan",
        description="Cek kesehatan pipeline dari riwayat run (kode keluar 0 OK, 1 WARNING, 2 CRITICAL)",
    )
    parser.add_argument("--log-dir", default=os.getenv("LOG_DIR", "logs"), help="folder log (isi riwayat_run.jsonl)")
    parser.add_argument("--maks-umur-jam", type=float, default=26, help="batas umur run sukses terakhir, dalam jam")
    parser.add_argument("--maks-rasio", type=float, default=0.2, help="batas rasio jam ditolak (0.2 = 20 persen)")
    parser.add_argument("--maks-turun", type=float, default=0.3, help="batas penurunan jam valid antar run (0.3 = 30 persen)")
    parser.add_argument("--maks-umur-data-hari", type=int, default=8, help="batas umur tanggal data aktual terbaru, dalam hari")
    parser.add_argument("--sekarang", default=None, help="waktu UTC palsu untuk uji, format 2026-10-06T08:00:00Z")
    args = parser.parse_args(argv)

    if args.sekarang:
        try:
            sekarang = datetime.strptime(args.sekarang, FORMAT_WAKTU).replace(tzinfo=timezone.utc)
        except ValueError:
            parser.error(f"--sekarang harus berformat 2026-10-06T08:00:00Z, bukan {args.sekarang!r}")
    else:
        sekarang = datetime.now(timezone.utc)

    hasil = periksa(
        baca_riwayat(args.log_dir), sekarang, args.maks_umur_jam, args.maks_rasio, args.maks_turun,
        args.maks_umur_data_hari,
    )
    for level, pesan in hasil:
        print(f"[{level}] {pesan}")
    terburuk = max((level for level, _ in hasil), key=URUTAN.get)
    print(f"STATUS: {terburuk}")
    return URUTAN[terburuk]


if __name__ == "__main__":
    sys.exit(main())
