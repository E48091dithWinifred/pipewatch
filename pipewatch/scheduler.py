"""Scheduler for running pipewatch checks on a configurable interval."""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class SchedulerConfig:
    interval_seconds: int = 60
    max_runs: Optional[int] = None  # None means run indefinitely
    jitter_seconds: int = 0  # extra sleep to stagger runs


@dataclass
class SchedulerStats:
    runs_completed: int = 0
    runs_failed: int = 0
    last_run_at: Optional[str] = None
    next_run_at: Optional[str] = None
    errors: list[str] = field(default_factory=list)

    @property
    def total_runs(self) -> int:
        return self.runs_completed + self.runs_failed

    def record_success(self) -> None:
        self.runs_completed += 1
        self.last_run_at = datetime.utcnow().isoformat()

    def record_failure(self, error: str) -> None:
        self.runs_failed += 1
        self.last_run_at = datetime.utcnow().isoformat()
        self.errors.append(error)


def run_scheduler(
    task: Callable[[], None],
    config: SchedulerConfig,
    stop_event: Optional[object] = None,  # threading.Event-compatible
) -> SchedulerStats:
    """Run *task* repeatedly according to *config*.

    Args:
        task: Zero-argument callable executed on each tick.
        config: Scheduler timing configuration.
        stop_event: Optional threading.Event; loop exits when set.

    Returns:
        SchedulerStats accumulated over all runs.
    """
    stats = SchedulerStats()

    def _should_stop() -> bool:
        if stop_event is not None and stop_event.is_set():  # type: ignore[union-attr]
            return True
        if config.max_runs is not None and stats.total_runs >= config.max_runs:
            return True
        return False

    while not _should_stop():
        try:
            logger.info("Scheduler: starting run #%d", stats.total_runs + 1)
            task()
            stats.record_success()
            logger.info("Scheduler: run completed successfully")
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            stats.record_failure(msg)
            logger.error("Scheduler: run failed — %s", msg)

        if _should_stop():
            break

        sleep_for = config.interval_seconds + config.jitter_seconds
        next_run = datetime.utcnow().isoformat()
        stats.next_run_at = next_run
        logger.debug("Scheduler: sleeping %ds until next run", sleep_for)
        time.sleep(sleep_for)

    return stats
