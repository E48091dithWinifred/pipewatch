"""pager.py — Paginate and slice lists of pipeline statuses for CLI display."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, List, Optional, TypeVar

from pipewatch.checker import PipelineStatus

T = TypeVar("T")

DEFAULT_PAGE_SIZE = 10


@dataclass
class Page(Generic[T]):
    items: List[T]
    page: int
    page_size: int
    total: int

    @property
    def total_pages(self) -> int:
        if self.page_size <= 0:
            return 1
        return max(1, (self.total + self.page_size - 1) // self.page_size)

    @property
    def has_next(self) -> bool:
        return self.page < self.total_pages

    @property
    def has_prev(self) -> bool:
        return self.page > 1

    def summary(self) -> str:
        start = (self.page - 1) * self.page_size + 1 if self.items else 0
        end = start + len(self.items) - 1 if self.items else 0
        return f"Page {self.page}/{self.total_pages} ({start}-{end} of {self.total})"


def paginate(
    statuses: List[PipelineStatus],
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Page[PipelineStatus]:
    """Return a Page slice of *statuses* for the given 1-based page number."""
    if page_size <= 0:
        raise ValueError("page_size must be a positive integer")
    if page < 1:
        raise ValueError("page must be >= 1")

    total = len(statuses)
    start = (page - 1) * page_size
    end = start + page_size
    items = statuses[start:end]
    return Page(items=items, page=page, page_size=page_size, total=total)


def iter_pages(
    statuses: List[PipelineStatus],
    page_size: int = DEFAULT_PAGE_SIZE,
):
    """Yield every Page for *statuses* in order."""
    total_pages = max(1, (len(statuses) + page_size - 1) // page_size) if statuses else 1
    for p in range(1, total_pages + 1):
        yield paginate(statuses, page=p, page_size=page_size)
