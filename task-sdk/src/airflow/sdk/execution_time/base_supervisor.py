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
"""Base subprocess management for Airflow's TaskSDK."""

from __future__ import annotations

import atexit
import io
import logging
import os
import selectors
import signal
import sys
import time
import weakref
from collections.abc import Callable, Generator
from contextlib import suppress
from datetime import datetime
from socket import socket, socketpair
from typing import TYPE_CHECKING, ClassVar, NoReturn, TextIO, cast
from uuid import UUID

import attrs
import msgspec
import psutil
import structlog

from airflow.sdk._shared.logging.structlog import reconfigure_logger
from airflow.sdk.api.client import ServerResponseError
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time import comms
from airflow.sdk.execution_time.comms import ErrorResponse, _RequestFrame, _ResponseFrame

if TYPE_CHECKING:
    from pydantic import BaseModel, TypeAdapter
    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.bases.secrets_backend import BaseSecretsBackend

__all__ = [
    "WatchedSubprocess",
    "ensure_secrets_backend_loaded",
    "make_buffered_socket_reader",
    "length_prefixed_frame_reader",
]

log: FilteringBoundLogger = structlog.get_logger(logger_name="supervisor")

SIGSEGV_MESSAGE = """
******************************************* Received SIGSEGV *******************************************
SIGSEGV (Segmentation Violation) signal indicates Segmentation Fault error which refers to
an attempt by a program/library to write or read outside its allocated memory.

In Python environment usually this signal refers to libraries which use low level C API.
Make sure that you use right libraries/Docker Images
for your architecture (Intel/ARM) and/or Operational System (Linux/macOS).

Suggested way to debug
======================
  - Set environment variable 'PYTHONFAULTHANDLER' to 'true'.
  - Start airflow services.
  - Restart failed airflow task.
  - Check 'scheduler' and 'worker' services logs for additional traceback
    which might contain information about module/library where actual error happen.

Known Issues
============

Note: Only Linux-based distros supported as "Production" execution environment for Airflow.

macOS
-----
 1. Due to limitations in Apple's libraries not every process might 'fork' safe.
    One of the general error is unable to query the macOS system configuration for network proxies.
    If your are not using a proxy you could disable it by set environment variable 'no_proxy' to '*'.
    See: https://github.com/python/cpython/issues/58037 and https://bugs.python.org/issue30385#msg293958
********************************************************************************************************"""

# From <linux/prctl.h>
_PR_SET_DUMPABLE = 4
_PR_GET_DUMPABLE = 3

# Setting a fair buffer size here to handle most message sizes. Intention is to enforce a buffer size
# that is big enough to handle small to medium messages while not enforcing hard latency issues
BUFFER_SIZE = 4096


def _subprocess_main():
    from airflow.sdk.execution_time.task_runner import main

    main()


def _reset_signals():
    # Uninstall the rich etc. exception handler
    sys.excepthook = sys.__excepthook__
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGUSR2, signal.SIG_DFL)


def _configure_logs_over_json_channel(log_fd: int):
    # A channel that the task can send JSON-formatted logs over.
    #
    # JSON logs sent this way will be handled nicely
    from airflow.sdk.log import configure_logging, reset_logging

    log_io = os.fdopen(log_fd, "wb", buffering=0)
    reset_logging()
    configure_logging(json_output=True, output=log_io, sending_to_supervisor=True)


def _reopen_std_io_handles(child_stdin, child_stdout, child_stderr):
    # Ensure that sys.stdout et al (and the underlying filehandles for C libraries etc) are connected to the
    # pipes from the supervisor
    for handle_name, fd, sock, mode in (
        # Yes, we want to re-open stdin in write mode! This is cause it is a bi-directional socket, so we can
        # read and write to it.
        ("stdin", 0, child_stdin, "w"),
        ("stdout", 1, child_stdout, "w"),
        ("stderr", 2, child_stderr, "w"),
    ):
        os.dup2(sock.fileno(), fd)
        del sock

        # We open the socket/fd as binary, and then pass it to a TextIOWrapper so that it looks more like a
        # normal sys.stdout etc.
        binary = os.fdopen(fd, mode + "b")
        handle = io.TextIOWrapper(binary, line_buffering=True)
        setattr(sys, handle_name, handle)


def _get_last_chance_stderr() -> TextIO:
    stream = sys.__stderr__ or sys.stderr

    try:
        # We want to open another copy of the underlying filedescriptor if we can, to ensure it stays open!
        return os.fdopen(os.dup(stream.fileno()), "w", buffering=1)
    except Exception:
        # If that didn't work, do the best we can
        return stream


def _make_process_nondumpable() -> None:
    """Mark the current process as non-dumpable to prevent same-UID memory access."""
    if sys.platform != "linux":
        return
    try:
        import ctypes

        # CDLL(None) is dlopen(NULL) — a handle to the current process, which always
        # includes libc symbols since CPython is linked against it.
        libc = ctypes.CDLL(None, use_errno=True)
        rc = libc.prctl(_PR_SET_DUMPABLE, 0, 0, 0, 0)
        if rc != 0:
            log.warning("Failed to set PR_SET_DUMPABLE=0", errno=ctypes.get_errno())
    except Exception:
        log.warning("Unable to set PR_SET_DUMPABLE=0", exc_info=True)


def _fork_main(
    requests: socket,
    child_stdout: socket,
    child_stderr: socket,
    log_fd: int,
    target: Callable[[], None],
) -> NoReturn:
    """
    "Entrypoint" of the child process.

    Ultimately this process will be running the user's code in the operators ``execute()`` function.

    The responsibility of this function is to:

    - Reset any signals handlers we inherited from the parent process (so they don't fire twice - once in
      parent, and once in child)
    - Set up the out/err handles to the streams created in the parent (to capture stdout and stderr for
      logging)
    - Configure the loggers in the child (both stdlib logging and Structlog) to send JSON logs back to the
      supervisor for processing/output.
    - Catch un-handled exceptions and attempt to show _something_ in case of error
    - Finally, run the actual task runner code (``target`` argument, defaults to ``.task_runner:main`)
    """
    # TODO: Make this process a session leader

    # Store original stderr for last-chance exception handling
    last_chance_stderr = _get_last_chance_stderr()

    _reset_signals()
    if log_fd:
        _configure_logs_over_json_channel(log_fd)
    _reopen_std_io_handles(requests, child_stdout, child_stderr)

    def exit(n: int) -> NoReturn:
        with suppress(ValueError, OSError):
            sys.stdout.flush()
        with suppress(ValueError, OSError):
            sys.stderr.flush()
        with suppress(ValueError, OSError):
            last_chance_stderr.flush()

        # Explicitly close the child-end of our supervisor sockets so
        # the parent sees EOF on "logs" channel.
        with suppress(OSError):
            os.close(log_fd)
        with suppress(OSError):
            os.close(requests.fileno())
        os._exit(n)

    if hasattr(atexit, "_clear"):
        # Since we're in a fork we want to try and clear them. If we can't do it cleanly, then we won't try
        # and run new atexit handlers.
        with suppress(Exception):
            atexit._clear()
            base_exit = exit

            def exit(n: int) -> NoReturn:
                # This will only run any atexit funcs registered after we've forked.
                atexit._run_exitfuncs()
                base_exit(n)

    try:
        block_orm_access()

        target()
        exit(0)
    except SystemExit as e:
        code = 1
        if isinstance(e.code, int):
            code = e.code
        elif e.code:
            print(e.code, file=sys.stderr)
        exit(code)
    except Exception:
        # Last ditch log attempt
        exc, v, tb = sys.exc_info()

        import traceback

        try:
            last_chance_stderr.write("--- Supervised process Last chance exception handler ---\n")
            traceback.print_exception(exc, value=v, tb=tb, file=last_chance_stderr)
            # Exit code 126 and 125 don't have any "special" meaning, they are only meant to serve as an
            # identifier that the task process died in a really odd way.
            exit(126)
        except Exception as e:
            with suppress(Exception):
                print(
                    f"--- Last chance exception handler failed --- {repr(str(e))}\n", file=last_chance_stderr
                )
            exit(125)


class BlockedDBSession:
    """:meta private:"""  # noqa: D400

    def __init__(self):
        raise RuntimeError("Direct database access via the ORM is not allowed in Airflow 3.0")

    def remove(*args, **kwargs):
        pass

    def get_bind(
        self,
        mapper=None,
        clause=None,
        bind=None,
        _sa_skip_events=None,
        _sa_skip_for_implicit_returning=False,
    ):
        pass


def block_orm_access():
    """
    Disable direct DB access as best as possible from task code.

    While we still don't have 100% code separation between TaskSDK and "core" Airflow, it is still possible to
    import the models and use them. This does what it can to disable that if it is not blocked at the network
    level
    """
    # A fake URL schema that might give users some clue what's going on. Hopefully
    conn = "airflow-db-not-allowed:///"
    if "airflow.settings" in sys.modules:
        from airflow import settings

        # This one needs to be from core, because we are checking if settings is loaded to disallow ORM
        # If settings is loaded, airflow.configuration will be too
        from airflow.configuration import conf

        to_block = frozenset(("engine", "async_engine", "Session", "AsyncSession", "NonScopedSession"))
        for attr in to_block:
            if hasattr(settings, attr):
                delattr(settings, attr)

        def configure_orm(*args, **kwargs):
            raise RuntimeError("Database access is disabled from Dags and Triggers")

        settings.configure_orm = configure_orm
        settings.Session = BlockedDBSession
        if conf.has_section("database"):
            conf.set("database", "sql_alchemy_conn", conn)
            conf.set("database", "sql_alchemy_conn_cmd", "/bin/false")
            conf.set("database", "sql_alchemy_conn_secret", "db-access-blocked")

        # This only gets called when the module does not already have an attribute, and for these values
        # lets give a custom error message
        def __getattr__(name: str):
            if name in to_block:
                raise AttributeError("Access to the Airflow Metadatabase from dags is not allowed!")
            raise AttributeError(f"module {settings.__name__!r} has no attribute {name!r}")

        settings.__getattr__ = __getattr__

        settings.SQL_ALCHEMY_CONN = conn
        settings.SQL_ALCHEMY_CONN_ASYNC = conn

    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = conn
    os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = conn


@attrs.define(kw_only=True)
class WatchedSubprocess:
    """
    Base class for managing subprocesses in Airflow's TaskSDK.

    This class handles common functionalities required for subprocess management, such as
    socket handling, process monitoring, and request handling.
    """

    id: UUID

    pid: int
    """The process ID of the child process"""

    stdin: socket
    """The handle connected to stdin of the child process"""

    decoder: ClassVar[TypeAdapter]
    """The decoder to use for incoming messages from the child process."""

    _process: psutil.Process = attrs.field(repr=False)
    """File descriptor for request handling."""

    _exit_code: int | None = attrs.field(default=None, init=False)
    _process_exit_monotonic: float | None = attrs.field(default=None, init=False)
    _open_sockets: weakref.WeakKeyDictionary[socket, str] = attrs.field(
        factory=weakref.WeakKeyDictionary, init=False
    )

    selector: selectors.BaseSelector = attrs.field(factory=selectors.DefaultSelector, repr=False)

    _frame_encoder: msgspec.msgpack.Encoder = attrs.field(factory=comms._new_encoder, repr=False)

    process_log: FilteringBoundLogger = attrs.field(repr=False)

    subprocess_logs_to_stdout: bool = False
    """Duplicate log messages to stdout, or only send them to ``self.process_log``."""

    start_time: float = attrs.field(factory=time.monotonic)
    """The start time of the child process."""

    @classmethod
    def start(
        cls,
        *,
        target: Callable[[], None] = _subprocess_main,
        logger: FilteringBoundLogger | None = None,
        **constructor_kwargs,
    ) -> Self:
        """Fork and start a new subprocess with the specified target function."""
        # Create socketpairs/"pipes" to connect to the stdin and out from the subprocess
        child_stdout, read_stdout = socketpair()
        child_stderr, read_stderr = socketpair()

        # Place for child to send requests/read responses, and the server side to read/respond
        child_requests, read_requests = socketpair()

        # Open the socketpair before forking off the child, so that it is open when we fork.
        child_logs, read_logs = socketpair()

        pid = os.fork()
        if pid == 0:
            # Close and delete of the parent end of the sockets.
            cls._close_unused_sockets(read_requests, read_stdout, read_stderr, read_logs)

            # Python GC should delete these for us, but lets make double sure that we don't keep anything
            # around in the forked processes, especially things that might involve open files or sockets!
            del constructor_kwargs
            del logger

            try:
                # Run the child entrypoint
                _fork_main(child_requests, child_stdout, child_stderr, child_logs.fileno(), target)
            except BaseException as e:
                import traceback

                with suppress(BaseException):
                    # We can't use log here, as if we except out of _fork_main something _weird_ went on.
                    print("Exception in _fork_main, exiting with code 124", file=sys.stderr)
                    traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)

            # It's really super super important we never exit this block. We are in the forked child, and if we
            # do then _THINGS GET WEIRD_.. (Normally `_fork_main` itself will `_exit()` so we never get here)
            os._exit(124)

        # Close the remaining parent-end of the sockets we've passed to the child via fork. We still have the
        # other end of the pair open
        cls._close_unused_sockets(child_stdout, child_stderr, child_logs)

        logger = logger or cast("FilteringBoundLogger", structlog.get_logger(logger_name="task").bind())
        proc = cls(
            pid=pid,
            stdin=read_requests,
            process=psutil.Process(pid),
            process_log=logger,
            start_time=time.monotonic(),
            **constructor_kwargs,
        )

        proc._register_pipe_readers(
            stdout=read_stdout,
            stderr=read_stderr,
            requests=read_requests,
            logs=read_logs,
        )

        return proc

    def _register_pipe_readers(self, stdout: socket, stderr: socket, requests: socket, logs: socket):
        """Register handlers for subprocess communication channels."""
        # self.selector is a way of registering a handler/callback to be called when the given IO channel has
        # activity to read on (https://www.man7.org/linux/man-pages/man2/select.2.html etc, but better
        # alternatives are used automatically) -- this is a way of having "event-based" code, but without
        # needing full async, to read and process output from each socket as it is received.

        # Track the open sockets, and for debugging what type each one is
        self._open_sockets.update(
            (
                (stdout, "stdout"),
                (stderr, "stderr"),
                (logs, "logs"),
                (requests, "requests"),
            )
        )

        target_loggers: tuple[FilteringBoundLogger, ...] = (self.process_log,)

        if self.subprocess_logs_to_stdout:
            target_loggers += (log,)

        self.selector.register(
            stdout, selectors.EVENT_READ, self._create_log_forwarder(target_loggers, "task.stdout")
        )
        self.selector.register(
            stderr,
            selectors.EVENT_READ,
            self._create_log_forwarder(target_loggers, "task.stderr", log_level=logging.ERROR),
        )
        self.selector.register(
            logs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(
                process_log_messages_from_subprocess(target_loggers), on_close=self._on_socket_closed
            ),
        )
        self.selector.register(
            requests,
            selectors.EVENT_READ,
            length_prefixed_frame_reader(self.handle_requests(log), on_close=self._on_socket_closed),
        )

    def _create_log_forwarder(self, loggers, name, log_level=logging.INFO) -> Callable[[socket], bool]:
        """Create a socket handler that forwards logs to a logger."""
        loggers = tuple(
            reconfigure_logger(
                log,
                structlog.processors.CallsiteParameterAdder,
            )
            for log in loggers
        )
        return make_buffered_socket_reader(
            forward_to_log(loggers, logger=name, level=log_level), on_close=self._on_socket_closed
        )

    def _on_socket_closed(self, sock: socket):
        # We want to keep servicing this process until we've read up to EOF from all the sockets.

        with suppress(KeyError):
            self.selector.unregister(sock)
            del self._open_sockets[sock]

    def send_msg(
        self, msg: BaseModel | None, request_id: int, error: ErrorResponse | None = None, **dump_opts
    ):
        """
        Send the msg as a length-prefixed response frame.

        ``request_id`` is the ID that the client sent in it's request, and has no meaning to the server

        """
        if msg:
            frame = _ResponseFrame(id=request_id, body=msg.model_dump(**dump_opts))
        else:
            err_resp = error.model_dump() if error else None
            frame = _ResponseFrame(id=request_id, error=err_resp)

        self.stdin.sendall(frame.as_bytes())

    def handle_requests(self, log: FilteringBoundLogger) -> Generator[None, _RequestFrame, None]:
        """Handle incoming requests from the task process, respond with the appropriate data."""
        while True:
            request = yield

            try:
                msg = self.decoder.validate_python(request.body)
            except Exception:
                log.exception("Unable to decode message", body=request.body)
                continue

            try:
                self._handle_request(msg, log, request.id)
            except ServerResponseError as e:
                error_details = e.response.json() if e.response else None
                log.error(
                    "API server error",
                    status_code=e.response.status_code,
                    detail=error_details,
                    message=str(e),
                )

                # Send error response back to task so that the error appears in the task logs
                self.send_msg(
                    msg=None,
                    error=ErrorResponse(
                        error=ErrorType.API_SERVER_ERROR,
                        detail={
                            "status_code": e.response.status_code,
                            "message": str(e),
                            "detail": error_details,
                        },
                    ),
                    request_id=request.id,
                )

    def _handle_request(self, msg, log: FilteringBoundLogger, req_id: int) -> None:
        raise NotImplementedError()

    @staticmethod
    def _close_unused_sockets(*sockets):
        """Close unused ends of sockets after fork."""
        for sock in sockets:
            sock.close()

    def _cleanup_open_sockets(self):
        """Force-close any sockets that never reported EOF."""
        # In extremely busy environments the selector can fail to deliver a
        # final read event before the subprocess exits. Without closing these
        # sockets the supervisor would wait forever thinking they are still
        # active. This cleanup ensures we always release resources and exit.
        stuck_sockets = []
        for sock, socket_type in self._open_sockets.items():
            fileno = "unknown"
            with suppress(Exception):
                fileno = sock.fileno()
                sock.close()
            stuck_sockets.append(f"{socket_type}(fd={fileno})")

        if stuck_sockets:
            log.warning("Force-closed stuck sockets", pid=self.pid, sockets=stuck_sockets)

        self.selector.close()
        self.stdin.close()

    def kill(
        self,
        signal_to_send: signal.Signals = signal.SIGINT,
        escalation_delay: float = 5.0,
        force: bool = False,
    ):
        """
        Attempt to terminate the subprocess with a given signal.

        If the process does not exit within `escalation_delay` seconds, escalate to SIGTERM and eventually SIGKILL if necessary.

        :param signal_to_send: The signal to send initially (default is SIGINT).
        :param escalation_delay: Time in seconds to wait before escalating to a stronger signal.
        :param force: If True, ensure escalation through all signals without skipping.
        """
        if self._exit_code is not None:
            return

        # Escalation sequence: SIGINT -> SIGTERM -> SIGKILL
        escalation_path: list[signal.Signals] = [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]

        if force and signal_to_send in escalation_path:
            # Start from `signal_to_send` and escalate to the end of the escalation path
            escalation_path = escalation_path[escalation_path.index(signal_to_send) :]
        else:
            escalation_path = [signal_to_send]

        for sig in escalation_path:
            try:
                self._process.send_signal(sig)

                start = time.monotonic()
                end = start + escalation_delay
                now = start

                while now < end:
                    # Service subprocess events during the escalation delay. This will return as soon as it's
                    # read from any of the sockets, so we need to re-run it if the process is still alive
                    if (
                        exit_code := self._service_subprocess(
                            max_wait_time=end - now, raise_on_timeout=False, expect_signal=sig
                        )
                    ) is not None:
                        log.info("Process exited", pid=self.pid, exit_code=exit_code, signal_sent=sig.name)
                        return

                    now = time.monotonic()

                msg = "Process did not terminate in time"
                if sig != escalation_path[-1]:
                    msg += "; escalating"
                log.warning(msg, pid=self.pid, signal=sig.name)
            except psutil.NoSuchProcess:
                log.debug("Process already terminated", pid=self.pid)
                self._exit_code = -1
                return

        log.error("Failed to terminate process after full escalation", pid=self.pid)

    def wait(self) -> int:
        raise NotImplementedError()

    def __rich_repr__(self):
        yield "id", self.id
        yield "pid", self.pid
        # only include this if it's not the default (third argument)
        yield "exit_code", self._exit_code, None

    __rich_repr__.angular = True  # type: ignore[attr-defined]

    def __repr__(self) -> str:
        rep = f"<{type(self).__name__} id={self.id} pid={self.pid}"
        if self._exit_code is not None:
            rep += f" exit_code={self._exit_code}"
        return rep + " >"

    def _service_subprocess(
        self, max_wait_time: float, raise_on_timeout: bool = False, expect_signal: None | int = None
    ):
        """
        Service subprocess events by processing socket activity and checking for process exit.

        This method:
        - Waits for activity on the registered file objects (via `self.selector.select`).
        - Processes any events triggered on these file objects.
        - Checks if the subprocess has exited during the wait.

        :param max_wait_time: Maximum time to block while waiting for events, in seconds.
        :param raise_on_timeout: If True, raise an exception if the subprocess does not exit within the timeout.
        :param expect_signal: Signal not to log if the task exits with this code.
        :returns: The process exit code, or None if it's still alive
        """
        # Ensure minimum timeout to prevent CPU spike with tight loop when timeout is 0 or negative
        timeout = max(0.01, max_wait_time)
        events = self.selector.select(timeout=timeout)
        for key, _ in events:
            # Retrieve the handler responsible for processing this file object (e.g., stdout, stderr)
            socket_handler, on_close = key.data

            # Example of handler behavior:
            # If the subprocess writes "Hello, World!" to stdout:
            # - `socket_handler` reads and processes the message.
            # - If EOF is reached, the handler returns False to signal no more reads are expected.
            # - BrokenPipeError should be caught and treated as if the handler returned false, similar
            # to EOF case
            try:
                need_more = socket_handler(key.fileobj)
            except (BrokenPipeError, ConnectionResetError):
                need_more = False

            # If the handler signals that the file object is no longer needed (EOF, closed, etc.)
            # unregister it from the selector to stop monitoring; `wait()` blocks until all selectors
            # are removed.
            if not need_more:
                sock: socket = key.fileobj  # type: ignore[assignment]
                on_close(sock)
                sock.close()

        # Check if the subprocess has exited
        return self._check_subprocess_exit(raise_on_timeout=raise_on_timeout, expect_signal=expect_signal)

    def _check_subprocess_exit(
        self, raise_on_timeout: bool = False, expect_signal: None | int = None
    ) -> int | None:
        """Check if the subprocess has exited."""
        if self._exit_code is not None:
            return self._exit_code

        try:
            self._exit_code = self._process.wait(timeout=0)
        except psutil.TimeoutExpired:
            if raise_on_timeout:
                raise
        else:
            self._process_exit_monotonic = time.monotonic()

            if expect_signal is not None and self._exit_code == -expect_signal:
                # Bypass logging, the caller expected us to exit with this
                return self._exit_code

            # Put a message in the viewable task logs

            if self._exit_code == -signal.SIGSEGV:
                self.process_log.critical(SIGSEGV_MESSAGE)
            # psutil turns signal exit codes into an enum for us. Handy. (Otherwise it's a plain integer) if exit_code and (name := getattr(exit_code, "name")):
            elif name := getattr(self._exit_code, "name", None):
                message = "Process terminated by signal."
                level = logging.ERROR
                if self._exit_code == -signal.SIGKILL:
                    message += " Likely out of memory error (OOM)."
                    level = logging.CRITICAL
                message += " For more information, see https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html#process-terminated-by-signal."
                self.process_log.log(level, message, signal=int(self._exit_code), signal_name=name)
            elif self._exit_code:
                # Run of the mill exit code (1, 42, etc).
                # Most task errors should be caught in the task runner and _that_ exits with 0.
                self.process_log.warning("Process exited abnormally", exit_code=self._exit_code)
        return self._exit_code


# Sockets, even the `.makefile()` function don't correctly do line buffering on reading. If a chunk is read
# and it doesn't contain a new line character, `.readline()` will just return the chunk as is.
#
# This returns a callback suitable for attaching to a `selector` that reads in to a buffer, and yields lines
# to a (sync) generator
def make_buffered_socket_reader(
    gen: Generator[None, bytes | bytearray, None],
    on_close: Callable[[socket], None],
    buffer_size: int = 4096,
):
    buffer = bytearray()  # This will hold our accumulated binary data
    read_buffer = bytearray(buffer_size)  # Temporary buffer for each read

    # We need to start up the generator to get it to the point it's at waiting on the yield
    next(gen)

    def cb(sock: socket):
        nonlocal buffer, read_buffer
        # Read up to `buffer_size` bytes of data from the socket
        n_received = sock.recv_into(read_buffer)

        if not n_received:
            # If no data is returned, the connection is closed. Return whatever is left in the buffer
            if len(buffer):
                with suppress(StopIteration):
                    gen.send(buffer)
            return False

        buffer.extend(read_buffer[:n_received])

        # We could have read multiple lines in one go, yield them all
        while (newline_pos := buffer.find(b"\n")) != -1:
            line = buffer[: newline_pos + 1]
            try:
                gen.send(line)
            except StopIteration:
                return False
            buffer = buffer[newline_pos + 1 :]  # Update the buffer with remaining data

        return True

    return cb, on_close


def length_prefixed_frame_reader(
    gen: Generator[None, _RequestFrame, None], on_close: Callable[[socket], None]
):
    length_needed: int | None = None
    # This will hold our accumulated/partial binary frame if it doesn't come in a single read
    buffer: memoryview | None = None
    # position in the buffer to store next read
    pos = 0
    decoder = msgspec.msgpack.Decoder[_RequestFrame](_RequestFrame)

    # We need to start up the generator to get it to the point it's at waiting on the yield
    next(gen)

    def cb(sock: socket):
        nonlocal buffer, length_needed, pos

        if length_needed is None:
            # Read the 32bit length of the frame
            bytes = sock.recv(4)
            if bytes == b"":
                return False

            length_needed = int.from_bytes(bytes, byteorder="big")
            buffer = memoryview(bytearray(length_needed))
        if length_needed and buffer:
            n = sock.recv_into(buffer[pos:])
            if n == 0:
                # EOF
                return False
            pos += n

            if pos >= length_needed:
                request = decoder.decode(buffer)
                buffer = None
                pos = 0
                length_needed = None
                try:
                    gen.send(request)
                except StopIteration:
                    return False
        return True

    return cb, on_close


def process_log_messages_from_subprocess(
    loggers: tuple[FilteringBoundLogger, ...],
) -> Generator[None, bytes | bytearray, None]:
    from structlog.stdlib import NAME_TO_LEVEL

    loggers = tuple(
        reconfigure_logger(
            log,
            structlog.processors.CallsiteParameterAdder,
            # We need these logger to print _everything_ they are given. The subprocess itself does the level
            # filtering.
            level_override=logging.NOTSET,
        )
        for log in loggers
    )

    while True:
        # Generator receive syntax, values are "sent" in  by the `make_buffered_socket_reader` and returned to
        # the yield.
        line = yield

        try:
            event = msgspec.json.decode(line)
        except Exception:
            log.exception("Malformed json log line", line=line)
            continue

        if ts := event.get("timestamp"):
            # We use msgspec to decode the timestamp as it does it orders of magnitude quicker than
            # datetime.strptime cn
            event["timestamp"] = msgspec.json.decode(f'"{ts}"', type=datetime)

        if exc := event.pop("exception", None):
            # TODO: convert the dict back to a pretty stack trace
            event["error_detail"] = exc

        if level := NAME_TO_LEVEL.get(event.pop("level")):
            msg = event.pop("event", None)
            for target in loggers:
                target.log(level, msg, **event)


def forward_to_log(
    target_loggers: tuple[FilteringBoundLogger, ...], logger: str, level: int
) -> Generator[None, bytes | bytearray, None]:
    while True:
        line = yield
        # Strip off new line
        line = line.rstrip()
        try:
            msg = line.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            msg = line.decode("ascii", errors="replace")
        for log in target_loggers:
            log.log(level, msg, logger=logger)


def ensure_secrets_backend_loaded() -> list[BaseSecretsBackend]:
    """
    Initialize secrets backend with auto-detected context.

    Detection strategy:
    1. SUPERVISOR_COMMS exists and is set → client chain (ExecutionAPISecretsBackend)
    2. _AIRFLOW_PROCESS_CONTEXT=server env var → server chain (MetastoreBackend)
    3. Neither → fallback chain (only env vars + external backends, no MetastoreBackend)

    Client contexts: task runner in worker (has SUPERVISOR_COMMS)
    Server contexts: API server, scheduler (set _AIRFLOW_PROCESS_CONTEXT=server)
    Fallback contexts: supervisor, unknown contexts (no SUPERVISOR_COMMS, no env var)

    The fallback chain ensures supervisor can use external secrets (AWS Secrets Manager,
    Vault, etc.) while falling back to API client, without trying MetastoreBackend.
    """
    import os

    from airflow.sdk.configuration import ensure_secrets_loaded
    from airflow.sdk.execution_time.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

    # 1. Check for client context (SUPERVISOR_COMMS)
    try:
        from airflow.sdk.execution_time import task_runner

        if hasattr(task_runner, "SUPERVISOR_COMMS") and task_runner.SUPERVISOR_COMMS is not None:
            # Client context: task runner with SUPERVISOR_COMMS
            return ensure_secrets_loaded(default_backends=DEFAULT_SECRETS_SEARCH_PATH_WORKERS)
    except (ImportError, AttributeError):
        pass

    # 2. Check for explicit server context
    if os.environ.get("_AIRFLOW_PROCESS_CONTEXT") == "server":
        # Server context: API server, scheduler
        # uses the default server list
        return ensure_secrets_loaded()

    # 3. Fallback for unknown contexts (supervisor, etc.)
    # Only env vars + external backends from config, no MetastoreBackend, no ExecutionAPISecretsBackend
    fallback_backends = [
        "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
    ]
    return ensure_secrets_loaded(default_backends=fallback_backends)
