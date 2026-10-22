"""Tes statis DAG Airflow: Airflow tidak dipasang di CI, jadi file dicek lewat ast, tanpa diimpor.

Yang ditangkap di sini: salah ketik atau salah sintaks di dags/, import gaya Airflow 2 yang
sudah deprecated, dan jadwal atau id task yang berubah tanpa sengaja.
"""

import ast
import unittest
from pathlib import Path

DAG_PATH = Path(__file__).resolve().parent.parent / "dags" / "cuaca_dag.py"


def _literal(node):
    try:
        return ast.literal_eval(node)
    except ValueError:
        return None


class TesDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pohon = ast.parse(DAG_PATH.read_text(encoding="utf-8"))

    def _panggilan(self, nama):
        return [
            n for n in ast.walk(self.pohon)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == nama
        ]

    def test_memakai_import_airflow_3(self):
        modul = {n.module for n in ast.walk(self.pohon) if isinstance(n, ast.ImportFrom)}
        self.assertLessEqual(
            {"airflow.sdk", "airflow.providers.standard.operators.bash", "airflow.providers.standard.operators.python"},
            modul,
        )
        for lama in ("airflow", "airflow.operators.bash", "airflow.operators.python"):
            self.assertNotIn(lama, modul, f"import gaya Airflow 2 masih dipakai: {lama}")

    def test_dag_id_jadwal_dan_catchup(self):
        panggilan = self._panggilan("DAG")
        self.assertEqual(len(panggilan), 1)
        opsi = {k.arg: _literal(k.value) for k in panggilan[0].keywords}
        self.assertEqual(opsi["dag_id"], "cuaca_harian")
        self.assertEqual(opsi["schedule"], "0 6 * * *")
        self.assertIs(opsi["catchup"], False)
        self.assertEqual(opsi["max_active_runs"], 1)

    def test_task_etl_dan_cek_kesehatan_ada(self):
        id_task = {
            _literal(k.value)
            for nama in ("BashOperator", "PythonOperator")
            for p in self._panggilan(nama)
            for k in p.keywords
            if k.arg == "task_id"
        }
        self.assertEqual(id_task, {"etl", "cek_kesehatan"})


if __name__ == "__main__":
    unittest.main()
