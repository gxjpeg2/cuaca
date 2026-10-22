"""Jalankan extract, transform, load, lalu tulis output.

Kode keluar: 0 sukses, 1 gagal yang dikenali, 2 gagal tak terduga.
"""

import logging

from .extract import ExtractError, extract
from .load import LoadError, load
from .ringkasan import RingkasanError, buat_ringkasan
from .transform import TransformError, transform

logger = logging.getLogger(__name__)


def run(konfig, statistik=None):
    logger.info(
        "Pipeline mulai (mode=%s, forecast=%s, archive=%s, db=%s)",
        konfig.mode, konfig.url_prakiraan, konfig.url_arsip, konfig.db,
    )
    try:
        info = {}
        mentah = extract(konfig, info)
        if statistik is not None:
            statistik.update(
                lokasi=info.get("lokasi"), mulai=info.get("mulai"), selesai=info.get("selesai"),
                waktu_prediksi=info.get("waktu_prediksi"),
            )
        bersih, ditolak = transform(mentah)

        hasil_load = load(bersih, ditolak, konfig.db)
        if statistik is not None:
            statistik.update(
                aktual_valid=len(bersih["aktual"]), prakiraan_valid=len(bersih["prakiraan"]),
                baris_ditolak=len(ditolak), **hasil_load,
            )
            if bersih["aktual"]:
                statistik["tanggal_terbaru"] = max(r["tanggal_lokal"] for r in bersih["aktual"]).isoformat()
        buat_ringkasan(konfig.db, konfig.folder_output)
    except (ExtractError, TransformError, LoadError, RingkasanError) as e:
        logger.error("Pipeline dihentikan: %s", e)
        return 1
    except Exception:
        logger.exception("Pipeline gagal karena error tak terduga")
        return 2

    logger.info(
        "Pipeline sukses: %d jam aktual dan %d jam prakiraan dimuat, %d ditolak",
        len(bersih["aktual"]), len(bersih["prakiraan"]), len(ditolak),
    )
    return 0
