import importlib.util
from pathlib import Path
from unittest.mock import Mock


DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "nairobi_air_quality.py"
SPEC = importlib.util.spec_from_file_location("nairobi_air_quality_dag", DAG_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

nairobi_air_quality_pipeline = MODULE.nairobi_air_quality_pipeline


def test_dag_has_expected_metadata():
    dag = nairobi_air_quality_pipeline()
    assert dag.dag_id == "nairobi_air_quality"
    assert dag.schedule == "@daily"
    assert dag.tags == {"air_quality", "nairobi"}


def test_dag_builds_pipeline_structure():
    dag = nairobi_air_quality_pipeline()

    task_ids = [task.task_id for task in dag.task_dict.values()]

    assert "extract_locations" in task_ids
    assert "extract_sensors" in task_ids
    assert "flatten_sensors" in task_ids
    assert "extract_measurements" in task_ids
    assert "flatten_measurements" in task_ids
    assert "validate_measurements" in task_ids
    assert "load_measurements" in task_ids
    assert "refresh_marts" in task_ids
    assert "upsert_dimensions" in task_ids
