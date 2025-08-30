import os

os.environ["_AIRFLOW_SKIP_DB_TESTS"] = "true"

import importlib
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DAG_DIR = ROOT / "dags"


@pytest.mark.parametrize("dag_file", [f for f in DAG_DIR.glob("*.py") if f.stem != "__init__"])
def test_dag_imports(dag_file):
    """
    Ensure each DAG file imports without error.
    """
    module_name = f"dags.{dag_file.stem}"
    mod = importlib.import_module(module_name)
    # Must have at least one attribute called 'dag'
    assert hasattr(mod, "dag"), f"{module_name} missing 'dag'"
