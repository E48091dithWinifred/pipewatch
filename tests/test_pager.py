"""Tests for pipewatch.pager."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.pager import Page, iter_pages, paginate


def _make_status(name: str, level: AlertLevel = AlertLevel.OK) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="ok",
        error_rate=0.0,
        latency_ms=100.0,
    )


@pytest.fixture()
def sample_statuses():
    return [_make_status(f"pipe_{i}") for i in range(25)]


# --- paginate ---

def test_paginate_first_page_length(sample_statuses):
    page = paginate(sample_statuses, page=1, page_size=10)
    assert len(page.items) == 10


def test_paginate_last_page_partial(sample_statuses):
    page = paginate(sample_statuses, page=3, page_size=10)
    assert len(page.items) == 5


def test_paginate_correct_items(sample_statuses):
    page = paginate(sample_statuses, page=2, page_size=10)
    assert page.items[0].pipeline_name == "pipe_10"
    assert page.items[-1].pipeline_name == "pipe_19"


def test_paginate_total(sample_statuses):
    page = paginate(sample_statuses, page=1, page_size=10)
    assert page.total == 25


def test_paginate_total_pages(sample_statuses):
    page = paginate(sample_statuses, page=1, page_size=10)
    assert page.total_pages == 3


def test_paginate_has_next_true(sample_statuses):
    page = paginate(sample_statuses, page=1, page_size=10)
    assert page.has_next is True


def test_paginate_has_next_false_on_last(sample_statuses):
    page = paginate(sample_statuses, page=3, page_size=10)
    assert page.has_next is False


def test_paginate_has_prev_false_on_first(sample_statuses):
    page = paginate(sample_statuses, page=1, page_size=10)
    assert page.has_prev is False


def test_paginate_has_prev_true(sample_statuses):
    page = paginate(sample_statuses, page=2, page_size=10)
    assert page.has_prev is True


def test_paginate_empty_list():
    page = paginate([], page=1, page_size=10)
    assert page.items == []
    assert page.total == 0
    assert page.total_pages == 1


def test_paginate_invalid_page_size():
    with pytest.raises(ValueError, match="page_size"):
        paginate([], page=1, page_size=0)


def test_paginate_invalid_page_number():
    with pytest.raises(ValueError, match="page"):
        paginate([], page=0, page_size=10)


# --- Page.summary ---

def test_summary_format(sample_statuses):
    page = paginate(sample_statuses, page=2, page_size=10)
    assert page.summary() == "Page 2/3 (11-20 of 25)"


def test_summary_empty():
    page = paginate([], page=1, page_size=10)
    assert "0" in page.summary()


# --- iter_pages ---

def test_iter_pages_yields_all(sample_statuses):
    pages = list(iter_pages(sample_statuses, page_size=10))
    assert len(pages) == 3


def test_iter_pages_covers_all_items(sample_statuses):
    all_items = [item for page in iter_pages(sample_statuses, page_size=10) for item in page.items]
    assert len(all_items) == 25


def test_iter_pages_empty_yields_one_page():
    pages = list(iter_pages([], page_size=10))
    assert len(pages) == 1
    assert pages[0].items == []
