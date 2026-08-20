import threading
import time
from typing import Final
from unittest.mock import MagicMock

from litellm.constants import LOGGING_EXECUTOR_MAX_PENDING_TASKS
from litellm.litellm_core_utils.thread_pool_executor import (
    BoundedLoggingThreadPoolExecutor,
    executor,
)


def test_submit_drops_tasks_when_backlog_is_full():
    release: Final = threading.Event()
    started: Final = threading.Event()
    ran_first: Final = threading.Event()
    ran_second: Final = threading.Event()
    ran_dropped: Final = threading.Event()

    def blocking_task(ran: threading.Event) -> None:
        ran.set()
        started.set()
        release.wait(timeout=10)

    pool: Final = BoundedLoggingThreadPoolExecutor(max_workers=1, max_pending_tasks=2)
    try:
        first: Final = pool.submit(blocking_task, ran_first)
        assert started.wait(timeout=10)
        second: Final = pool.submit(blocking_task, ran_second)
        dropped: Final = pool.submit(blocking_task, ran_dropped)

        assert dropped.cancelled()
        assert not first.cancelled()
        assert not second.cancelled()

        release.set()
        first.result(timeout=10)
        second.result(timeout=10)
        assert ran_first.is_set()
        assert ran_second.is_set()
        assert not ran_dropped.is_set()
    finally:
        release.set()
        pool.shutdown(wait=True)


def test_submit_releases_slots_after_completion():
    pool: Final = BoundedLoggingThreadPoolExecutor(max_workers=1, max_pending_tasks=1)

    def submit_and_wait() -> str:
        future: Final = pool.submit(lambda: "ok")
        assert not future.cancelled()
        return future.result(timeout=10)

    try:
        results: Final = tuple(submit_and_wait() for _ in range(5))
        assert results == ("ok",) * 5
    finally:
        pool.shutdown(wait=True)


def test_drop_warning_is_rate_limited(monkeypatch):
    from litellm import _logging

    warning_mock: Final = MagicMock()
    monkeypatch.setattr(_logging.verbose_logger, "warning", warning_mock)

    release: Final = threading.Event()
    started: Final = threading.Event()

    def blocking_task() -> None:
        started.set()
        release.wait(timeout=10)

    pool: Final = BoundedLoggingThreadPoolExecutor(
        max_workers=1, max_pending_tasks=1, drop_log_interval_seconds=60.0
    )
    try:
        pool.submit(blocking_task)
        assert started.wait(timeout=10)

        assert pool.submit(time.sleep, 0).cancelled()
        assert pool.submit(time.sleep, 0).cancelled()
        assert pool.submit(time.sleep, 0).cancelled()

        assert warning_mock.call_count == 1
        assert warning_mock.call_args.args[1:] == (1, 1)
    finally:
        release.set()
        pool.shutdown(wait=True)


def test_global_executor_is_bounded():
    assert isinstance(executor, BoundedLoggingThreadPoolExecutor)
    assert executor._max_pending_tasks == LOGGING_EXECUTOR_MAX_PENDING_TASKS
