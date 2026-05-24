import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytest.importorskip("airflow", reason="airflow 미설치 시 스킵")

from airflow.models import DagBag


DAG_ID = "ecommerce_log_pipeline"
EXPECTED_TASKS = ["generate_logs", "spark_transform", "load_to_postgres", "validate_load"]
DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_dag_loaded_without_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import 오류: {dagbag.import_errors}"


def test_dag_exists(dagbag):
    assert DAG_ID in dagbag.dags, f"{DAG_ID} DAG를 찾을 수 없음"


def test_dag_has_all_tasks(dagbag):
    dag = dagbag.dags[DAG_ID]
    task_ids = {t.task_id for t in dag.tasks}
    for expected in EXPECTED_TASKS:
        assert expected in task_ids, f"태스크 누락: {expected}"


def test_dag_task_count(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert len(dag.tasks) == len(EXPECTED_TASKS)


def test_dag_dependency_order(dagbag):
    """generate_logs → spark_transform → load_to_postgres → validate_load 순서 검증."""
    dag = dagbag.dags[DAG_ID]
    tasks = {t.task_id: t for t in dag.tasks}

    assert "spark_transform" in {t.task_id for t in tasks["generate_logs"].downstream_list}
    assert "load_to_postgres" in {t.task_id for t in tasks["spark_transform"].downstream_list}
    assert "validate_load" in {t.task_id for t in tasks["load_to_postgres"].downstream_list}


def test_dag_schedule(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert dag.schedule_interval == "0 2 * * *"


def test_dag_catchup_disabled(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert dag.catchup is False
