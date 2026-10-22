"""Server HTTP yang meniru dua endpoint Open-Meteo: /v1/forecast dan /v1/archive.

Langkah simulasi bertambah tiap `interval` detik. Dengan interval 0, simulasi hanya maju
lewat POST /maju. Kesalahan parameter dijawab HTTP 400 dengan badan
{"error": true, "reason": "..."}, seperti sumber aslinya.
"""

import argparse
import json
import logging
import threading
import time
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .atmosfer import VARIABEL, Atmosfer

logger = logging.getLogger(__name__)

JALUR_PRAKIRAAN = "/v1/forecast"
JALUR_ARSIP = "/v1/archive"
MAKS_HARI = 400


class Sumber:
    """Atmosfer yang langkahnya mengikuti waktu."""

    def __init__(self, interval, atmosfer):
        self.atmosfer = atmosfer
        self.interval = interval
        self.mulai = time.monotonic()
        self._kunci = threading.Lock()

    def akses(self, fungsi):
        with self._kunci:
            if self.interval > 0:
                target = int((time.monotonic() - self.mulai) // self.interval)
                while self.atmosfer.langkah < target:
                    self.atmosfer.maju()
            return self.atmosfer.langkah, fungsi(self.atmosfer)

    def maju(self):
        with self._kunci:
            self.atmosfer.maju()
            return self.atmosfer.langkah


def _baca_umum(query):
    """Ambil (lintang, bujur) dan pastikan hourly dan timezone yang diminta didukung."""
    try:
        lintang = float(query["latitude"][0])
        bujur = float(query["longitude"][0])
    except (KeyError, ValueError):
        raise ValueError("latitude dan longitude harus angka") from None
    if not (-90 <= lintang <= 90 and -180 <= bujur <= 180):
        raise ValueError("latitude atau longitude di luar rentang")
    diminta = [v for v in query.get("hourly", [""])[0].split(",") if v]
    salah = [v for v in diminta if v not in VARIABEL]
    if not diminta or salah:
        raise ValueError(f"nilai hourly tidak valid: {salah or 'kosong'}")
    if set(diminta) != set(VARIABEL):
        raise ValueError("simulasi hanya melayani keenam variabel hourly sekaligus")
    if query.get("timezone", ["GMT"])[0] not in ("UTC", "GMT"):
        raise ValueError("simulasi hanya mendukung timezone=UTC")
    return lintang, bujur


def buat_server(host="127.0.0.1", port=8000, interval=60.0, seed=2024, hari_awal=5, hari=None):
    sumber = Sumber(interval, Atmosfer(seed=seed, hari=hari, hari_awal=hari_awal))

    class Penangan(BaseHTTPRequestHandler):
        def do_GET(self):
            alamat = urlparse(self.path)
            query = parse_qs(alamat.query)
            if alamat.path == JALUR_PRAKIRAAN:
                try:
                    lintang, bujur = _baca_umum(query)
                    hari_ke_depan = int(query.get("forecast_days", ["7"])[0])
                    if not 1 <= hari_ke_depan <= 16:
                        raise ValueError("forecast_days harus 1 sampai 16")
                except ValueError as e:
                    return self._kirim(400, {"error": True, "reason": str(e)})
                langkah, isi = sumber.akses(lambda a: a.prakiraan(lintang, bujur, hari_ke_depan))
                return self._kirim(200, isi, langkah)
            if alamat.path == JALUR_ARSIP:
                try:
                    lintang, bujur = _baca_umum(query)
                    mulai = date.fromisoformat(query.get("start_date", [""])[0])
                    akhir = date.fromisoformat(query.get("end_date", [""])[0])
                    if akhir < mulai or (akhir - mulai).days >= MAKS_HARI:
                        raise ValueError("rentang tanggal tidak valid")
                except ValueError as e:
                    return self._kirim(400, {"error": True, "reason": str(e)})
                langkah, isi = sumber.akses(lambda a: a.aktual(lintang, bujur, mulai, akhir))
                return self._kirim(200, isi, langkah)
            self._kirim(404, {"error": True, "reason": "tidak ditemukan"})

        def do_POST(self):
            if urlparse(self.path).path != "/maju":
                return self._kirim(404, {"error": True, "reason": "tidak ditemukan"})
            self._kirim(200, {"langkah": sumber.maju()})

        def _kirim(self, status, isi, langkah=None):
            badan = json.dumps(isi).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(badan)))
            if langkah is not None:
                self.send_header("X-Langkah", str(langkah))
            self.end_headers()
            self.wfile.write(badan)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer((host, port), Penangan)
    server.sumber = sumber
    return server


def main(argv=None):
    parser = argparse.ArgumentParser(prog="simulasi", description="Open-Meteo simulasi yang datanya bergerak")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--interval", type=float, default=60.0, help="detik per langkah, 0 = hanya lewat POST /maju")
    parser.add_argument("--seed", type=int, default=2024)
    parser.add_argument("--hari-awal", type=int, default=5, help="data aktual awal tertinggal sekian hari dari hari ini")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    server = buat_server(args.host, args.port, args.interval, args.seed, args.hari_awal)
    logger.info("Simulasi jalan di http://%s:%d (interval %gs)", args.host, args.port, args.interval)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
