# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

from airflow.api.common.mark_tasks import set_dag_run_state_to_failed, set_dag_run_state_to_success
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, State, TaskInstanceState

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.definitions.dag import SerializedDAG

    from tests_common.pytest_plugin import DagMaker

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


def test_set_dag_run_state_to_failed(dag_maker: DagMaker[SerializedDAG]):
    with dag_maker("TEST_DAG_1") as dag:
        with EmptyOperator(task_id="teardown").as_teardown():
            EmptyOperator(task_id="running")
            EmptyOperator(task_id="pending")
    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "running":
            ti.set_state(TaskInstanceState.RUNNING)
    dag_maker.session.flush()

    updated_tis: list[TaskInstance] = set_dag_run_state_to_failed(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.FAILED
    assert task_dict["pending"].state == TaskInstanceState.UPSTREAM_FAILED
    assert "teardown" not in task_dict


def test_dag_run_state_persists_after_update_state(dag_maker: DagMaker[SerializedDAG]):
    """
    Test for issue #57061: DAG state should remain FAILED after manual failure.

    When a user manually marks a running DAG as failed:
    1. Running tasks are set to FAILED
    2. Pending leaf tasks are set to UPSTREAM_FAILED (not SKIPPED)
    3. When scheduler calls update_state(), DAG must stay FAILED

    If pending leaf tasks were SKIPPED (which is in success_states), the scheduler's
    update_state() would see all leaf tasks in success_states and mark DAG as SUCCESS,
    overriding the user's manual failure. Using UPSTREAM_FAILED (which is in failed_states)
    ensures the DAG stays FAILED.
    """
    # Create a simple linear DAG: start -> middle (running) -> end (pending leaf)
    with dag_maker("TEST_DAG_PERSIST_FAILED") as dag:
        start = EmptyOperator(task_id="start")
        middle = EmptyOperator(task_id="middle")
        end = EmptyOperator(task_id="end")  # This is the leaf task
        start >> middle >> end

    dr = dag_maker.create_dagrun()

    # Set up initial state: start completed, middle running, end pending
    for ti in dr.get_task_instances():
        if ti.task_id == "start":
            ti.set_state(TaskInstanceState.SUCCESS)
        elif ti.task_id == "middle":
            ti.set_state(TaskInstanceState.RUNNING)
        # "end" stays in None/pending state
    dag_maker.session.flush()

    # User manually marks DAG as failed
    set_dag_run_state_to_failed(dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session)
    dag_maker.session.flush()

    # Verify DAG is FAILED after manual marking
    dr_after_manual_fail = dag_maker.session.scalar(
        select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id)
    )
    assert dr_after_manual_fail.state == DagRunState.FAILED, "DAG should be FAILED after manual marking"

    # Verify the leaf task (end) is UPSTREAM_FAILED, not SKIPPED
    tis_after = {ti.task_id: ti for ti in dr_after_manual_fail.get_task_instances()}
    assert tis_after["end"].state == TaskInstanceState.UPSTREAM_FAILED, (
        "Leaf task should be UPSTREAM_FAILED (in failed_states), not SKIPPED (in success_states)"
    )

    # Now simulate what the scheduler does: call update_state()
    # This is the critical test - with SKIPPED, this would change DAG to SUCCESS
    dr_after_manual_fail.update_state(session=dag_maker.session)
    dag_maker.session.flush()

    # Verify DAG is STILL FAILED after update_state()
    dr_final = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert dr_final.state == DagRunState.FAILED, (
        "DAG should remain FAILED after update_state(). "
        "If this fails, it means leaf tasks were set to SKIPPED (success_states) "
        "instead of UPSTREAM_FAILED (failed_states), causing scheduler to override "
        "the manual failure with SUCCESS."
    )


@pytest.mark.parametrize(
    "unfinished_state", sorted([state for state in State.unfinished if state is not None])
)
def test_set_dag_run_state_to_success_unfinished_teardown(
    dag_maker: DagMaker[SerializedDAG],
    unfinished_state,
):
    with dag_maker("TEST_DAG_1") as dag:
        with EmptyOperator(task_id="teardown").as_teardown():
            EmptyOperator(task_id="running")
            EmptyOperator(task_id="pending")

    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "running":
            ti.set_state(TaskInstanceState.RUNNING)
        if ti.task_id == "teardown":
            ti.set_state(unfinished_state)

    dag_maker.session.flush()
    assert dr.state == DagRunState.RUNNING

    updated_tis: list[TaskInstance] = set_dag_run_state_to_success(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    run = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert run is not None
    assert run.state != DagRunState.SUCCESS
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.SUCCESS
    assert task_dict["pending"].state == TaskInstanceState.SUCCESS
    assert "teardown" not in task_dict


@pytest.mark.parametrize("finished_state", sorted(list(State.finished)))
def test_set_dag_run_state_to_success_finished_teardown(dag_maker: DagMaker[SerializedDAG], finished_state):
    with dag_maker("TEST_DAG_1") as dag:
        with EmptyOperator(task_id="teardown").as_teardown():
            EmptyOperator(task_id="failed")
    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "failed":
            ti.set_state(TaskInstanceState.FAILED)
        if ti.task_id == "teardown":
            ti.set_state(finished_state)
    dag_maker.session.flush()
    dr.set_state(DagRunState.FAILED)

    updated_tis: list[TaskInstance] = set_dag_run_state_to_success(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    run = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert run is not None
    assert run.state == DagRunState.SUCCESS
    if finished_state == TaskInstanceState.SUCCESS:
        assert len(updated_tis) == 1
    else:
        assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["failed"].state == TaskInstanceState.SUCCESS
    if finished_state != TaskInstanceState.SUCCESS:
        assert task_dict["teardown"].state == TaskInstanceState.SUCCESS
