"""DAG integrity tests: every DAG imports cleanly and follows the team's rules."""

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_exist(dagbag):
    assert set(dagbag.dag_ids) == {"grid_pulse_extract", "grid_pulse_transform"}


@pytest.mark.parametrize("dag_id", ["grid_pulse_extract", "grid_pulse_transform"])
def test_every_task_retries_and_alerts(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    for t in dag.tasks:
        assert t.retries >= 1, f"{t.task_id} has no retries"
        assert t.on_failure_callback, f"{t.task_id} has no failure alert"
        assert t.execution_timeout is not None, f"{t.task_id} can run forever"


def test_extract_feeds_transform_through_an_asset(dagbag):
    extract = dagbag.get_dag("grid_pulse_extract")
    transform = dagbag.get_dag("grid_pulse_transform")
    outlets = {str(o.uri) for t in extract.tasks for o in getattr(t, "outlets", [])}
    assert any(uri.rstrip("/") == "gridpulse://bronze" for uri in outlets)
    assert type(transform.timetable).__name__ == "AssetTriggeredTimetable"


def test_quality_gate_runs_before_publishing(dagbag):
    transform = dagbag.get_dag("grid_pulse_transform")
    assert transform.get_task("publish_site").upstream_task_ids == {"quality_report"}
    assert transform.get_task("quality_report").upstream_task_ids == {"dbt_build"}
