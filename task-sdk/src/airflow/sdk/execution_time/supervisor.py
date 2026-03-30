#
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
"""Backwards-compatibility re-export module. Import from base_supervisor or task_supervisor directly."""

from __future__ import annotations

from airflow.sdk.execution_time.base_supervisor import (
    BUFFER_SIZE as BUFFER_SIZE,
    SIGSEGV_MESSAGE as SIGSEGV_MESSAGE,
    BlockedDBSession as BlockedDBSession,
    WatchedSubprocess as WatchedSubprocess,
    _configure_logs_over_json_channel as _configure_logs_over_json_channel,
    _fork_main as _fork_main,
    _get_last_chance_stderr as _get_last_chance_stderr,
    _make_process_nondumpable as _make_process_nondumpable,
    _reopen_std_io_handles as _reopen_std_io_handles,
    _reset_signals as _reset_signals,
    _subprocess_main as _subprocess_main,
    block_orm_access as block_orm_access,
    ensure_secrets_backend_loaded as ensure_secrets_backend_loaded,
    forward_to_log as forward_to_log,
    length_prefixed_frame_reader as length_prefixed_frame_reader,
    make_buffered_socket_reader as make_buffered_socket_reader,
    process_log_messages_from_subprocess as process_log_messages_from_subprocess,
)
from airflow.sdk.execution_time.task_supervisor import (
    ActivitySubprocess as ActivitySubprocess,
    InProcessSupervisorComms as InProcessSupervisorComms,
    InProcessTestSupervisor as InProcessTestSupervisor,
    TaskRunResult as TaskRunResult,
    _configure_logging as _configure_logging,
    _fetch_remote_logging_conn as _fetch_remote_logging_conn,
    _remote_logging_conn as _remote_logging_conn,
    in_process_api_server as in_process_api_server,
    run_task_in_process as run_task_in_process,
    set_supervisor_comms as set_supervisor_comms,
    supervise_task as supervise_task,
    supervise_workload as supervise_workload,
)

# Backwards compatibility alias
supervise = supervise_task

__all__ = ["ActivitySubprocess", "WatchedSubprocess", "supervise"]
