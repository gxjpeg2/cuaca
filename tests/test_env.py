"""Tes pemuatan .env: python -m unittest discover -s tests -v"""

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cuaca import env
from cuaca.env import load_dotenv, muat_env

NAMA = "CUACA_UJI_ENV"


@unittest.skipIf(load_dotenv is None, "python-dotenv belum terpasang")
class TesMuatEnv(unittest.TestCase):
    def _file_env(self, tmp, isi):
        path = Path(tmp) / ".env"
        path.write_text(isi, encoding="utf-8")
        return path

    def test_mengisi_variabel_yang_belum_ada(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ):
            os.environ.pop(NAMA, None)
            path = self._file_env(tmp, f"# komentar\n{NAMA}=dari_file\n")
            self.assertTrue(muat_env(path))
            self.assertEqual(os.environ[NAMA], "dari_file")

    def test_tidak_menimpa_variabel_yang_sudah_ada(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {NAMA: "dari_shell"}):
            path = self._file_env(tmp, f"{NAMA}=dari_file\n")
            muat_env(path)
            self.assertEqual(os.environ[NAMA], "dari_shell")

    def test_file_tidak_ada_tidak_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(muat_env(Path(tmp) / "tidak_ada.env"))


class TesTanpaDotenv(unittest.TestCase):
    def test_tanpa_paket_dotenv_hanya_dilewati(self):
        with mock.patch.object(env, "load_dotenv", None):
            self.assertFalse(env.muat_env())


if __name__ == "__main__":
    unittest.main()
