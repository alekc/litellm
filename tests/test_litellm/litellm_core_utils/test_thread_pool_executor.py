import threading
import time
from typing import Final

from litellm.constants import LOGGING_EXECUTOR_MAX_PENDING_TASKS
from litellm.litellm_core_utils.thread_pool_executor import (
    BoundedLoggingThreadPoolExecutor,
    executor,
)


def test_submit_drops_tasks_when_backlog_is_full():
    release: Final = threading.Event()
    started: Final = threading.Event()
    executed: Final[list[str]] = []

    def blocking_task(name: str) -> None:
        executed.append(name)
        started.set()
        release.wait(timeout=10)

    pool: Final = BoundedLoggingThreadPoolExecutor(max_workers=1, max_pending_tasks=2)
    try:
        first: Final = pool.submit(blocking_task, "first")
        assert started.wait(timeout=10)
        second: Final = pool.submit(blocking_task, "second")
        dropped: Final = pool.submit(blocking_task, "dropped")

        assert dropped.cancelled()
        assert not first.cancelled()
        assert not second.cancelled()

        release.set()
        first.result(timeout=10)
        second.result(timeout=10)
        assert executed == ["first", "second"]
        assert "dropped" not in executed
    finally:
        release.set()
        pool.shutdown(wait=True)


def test_submit_releases_slots_after_completion():
    pool: Final = BoundedLoggingThreadPoolExecutor(max_workers=1, max_pending_tasks=1)
    try:
        for _ in range(5):
            future = pool.submit(lambda: "ok")
            assert future.result(timeout=10) == "ok"
            assert not future.cancelled()
    finally:
        pool.shutdown(wait=True)


def test_drop_warning_is_rate_limited(monkeypatch):
    from litellm import _logging

    warnings: Final[list[tuple[object, ...]]] = []
    monkeypatch.setattr(
        _logging.verbose_logger,
        "warning",
        lambda msg, *args: warnings.append(args),
    )

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

        assert len(warnings) == 1
        assert warnings[0] == (1, 1)
    finally:
        release.set()
        pool.shutdown(wait=True)


def test_global_executor_is_bounded():
    assert isinstance(executor, BoundedLoggingThreadPoolExecutor)
    assert executor._max_pending_tasks == LOGGING_EXECUTOR_MAX_PENDING_TASKS
