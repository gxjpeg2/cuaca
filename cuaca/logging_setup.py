"""Logging terpusat: file, JSON, run_id, penyamaran data sensitif."""

import json
import logging
import logging.config
import os
import re
import sys
import time
import uuid
from collections import Counter
from functools import wraps
from pathlib import Path

RUN_ID = uuid.uuid4().hex[:8]

LEVEL_VALID = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

_STATE = {"log_dir": None}


class RunIdFilter(logging.Filter):
    """Tempelkan run_id ke setiap record."""

    def filter(self, record):
        record.run_id = RUN_ID
        return True


class RedactFilter(logging.Filter):
    """Samarkan data sensitif di pesan log. Isi traceback tidak disentuh."""

    PATTERNS = [
        (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "[EMAIL]"),
        (re.compile(r"(?i)(password|token|api[_-]?key)\s*[=:]\s*\S+"), r"\1=[REDACTED]"),
        (re.compile(r"(?<!\d)(?:\+62|0)8\d{8,11}(?!\d)"), "[TELEPON]"),
    ]

    def filter(self, record):
        pesan = record.getMessage()
        for pola, pengganti in self.PATTERNS:
            pesan = pola.sub(pengganti, pesan)
        record.msg = pesan
        record.args = ()
        return True


class JsonFormatter(logging.Formatter):
    """Satu baris JSON per log."""

    def format(self, record):
        data = {
            "waktu": self.formatTime(record, "%Y-%m-%dT%H:%M:%S") + "Z",
            "level": record.levelname,
            "run_id": getattr(record, "run_id", None),
            "logger": record.name,
            "fungsi": record.funcName,
            "baris": record.lineno,
            "pesan": record.getMessage(),
        }
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False)


class LevelCounter(logging.Handler):
    """Hitung jumlah log per level, termasuk yang tidak tampil di layar."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.hitung = Counter()

    def emit(self, record):
        self.hitung[record.levelname] += 1


PENGHITUNG = LevelCounter()


def folder_log():
    """Folder file log yang aktif, atau None jika mode stdout."""
    return _STATE["log_dir"]


def setup_logging(console_level=None, log_dir=None, json_logs=None, log_stdout=None):
    """Atur logging untuk seluruh program dan kembalikan run_id.

    log_stdout=True mengirim log hanya ke stdout (untuk container), tanpa file.
    """
    console_level = (console_level or os.getenv("LOG_LEVEL", "INFO")).upper()
    if console_level not in LEVEL_VALID:
        raise ValueError(f"LOG_LEVEL tidak valid: {console_level!r}, pilih {sorted(LEVEL_VALID)}")

    log_dir = Path(log_dir or os.getenv("LOG_DIR", "logs"))
    if log_stdout is None:
        log_stdout = os.getenv("LOG_STDOUT", "0") == "1"
    if not log_stdout:
        log_dir.mkdir(parents=True, exist_ok=True)

    if json_logs is None:
        json_logs = os.getenv("LOG_JSON", "0") == "1"
    nama_format = "json" if json_logs else "detail"

    logging.Formatter.converter = time.gmtime

    handler_filters = ["run_id", "redact"]
    config = (
        {
            "version": 1,
            "disable_existing_loggers": False,
            "filters": {
                "run_id": {"()": RunIdFilter},
                "redact": {"()": RedactFilter},
            },
            "formatters": {
                "detail": {
                    "format": (
                        "%(asctime)sZ | %(levelname)-8s | %(run_id)s | "
                        "%(name)s:%(funcName)s:%(lineno)d | %(message)s"
                    ),
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                },
                "json": {"()": JsonFormatter},
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": console_level,
                    "formatter": nama_format,
                    "filters": handler_filters,
                },
                "app_file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": "DEBUG",
                    "formatter": nama_format,
                    "filters": handler_filters,
                    "filename": str(log_dir / "app.log"),
                    "maxBytes": 1_000_000,
                    "backupCount": 3,
                    "encoding": "utf-8",
                },
                "error_file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": "ERROR",
                    "formatter": nama_format,
                    "filters": handler_filters,
                    "filename": str(log_dir / "error.log"),
                    "maxBytes": 1_000_000,
                    "backupCount": 3,
                    "encoding": "utf-8",
                },
            },
            "root": {
                "level": "DEBUG",
                "handlers": ["console", "app_file", "error_file"],
            },
        }
    )
    if log_stdout:
        config["handlers"] = {"console": {**config["handlers"]["console"], "stream": "ext://sys.stdout"}}
        config["root"]["handlers"] = ["console"]
    logging.config.dictConfig(config)

    PENGHITUNG.hitung.clear()
    logging.getLogger().addHandler(PENGHITUNG)
    _STATE["log_dir"] = None if log_stdout else log_dir

    sys.excepthook = _catat_error_tak_tertangani
    return RUN_ID


def _catat_error_tak_tertangani(exc_type, exc, tb):
    logging.getLogger("cuaca").critical(
        "Error tidak tertangani", exc_info=(exc_type, exc, tb)
    )


def timed(func):
    """Dekorator untuk mencatat mulai dan durasi sebuah tahap."""
    log = logging.getLogger(func.__module__)

    @wraps(func)
    def wrapper(*args, **kwargs):
        mulai = time.perf_counter()
        log.info("Tahap '%s' mulai", func.__name__, stacklevel=2)
        try:
            return func(*args, **kwargs)
        finally:
            durasi = time.perf_counter() - mulai
            log.info(
                "Tahap '%s' selesai dalam %.3f detik",
                func.__name__,
                durasi,
                stacklevel=2,
            )

    return wrapper
