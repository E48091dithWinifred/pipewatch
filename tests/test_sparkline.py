"""Tests for pipewatch.sparkline module."""
from __future__ import annotations

import pytest

from pipewatch.sparkline import sparkline, labeled_sparkline


def test_sparkline_empty_returns_empty_string():
    assert sparkline([]) == ""


def test_sparkline_single_value_returns_one_char():
    result = sparkline([42.0])
    assert len(result) == 1


def test_sparkline_uniform_values_returns_middle_block():
    result = sparkline([5.0, 5.0, 5.0])
    assert all(c == result[0] for c in result)


def test_sparkline_ascending_ends_higher_than_start():
    result = sparkline([1.0, 2.0, 3.0, 4.0, 5.0])
    assert result[-1] >= result[0]


def test_sparkline_descending_ends_lower_than_start():
    result = sparkline([5.0, 4.0, 3.0, 2.0, 1.0])
    assert result[-1] <= result[0]


def test_sparkline_respects_width():
    values = list(range(50))
    result = sparkline(values, width=10)
    assert len(result) == 10


def test_sparkline_width_larger_than_data():
    values = [1.0, 2.0, 3.0]
    result = sparkline(values, width=20)
    assert len(result) == 3


def test_sparkline_only_block_chars():
    blocks = " ▁▂▃▄▅▆▇█"
    result = sparkline([0, 10, 5, 8, 3])
    for ch in result:
        assert ch in blocks


def test_labeled_sparkline_contains_label():
    result = labeled_sparkline("error_rate", [0.01, 0.02, 0.03], unit="%")
    assert result.startswith("error_rate:")


def test_labeled_sparkline_contains_latest():
    result = labeled_sparkline("latency", [100.0, 200.0, 150.0], unit="ms")
    assert "latest=150.00ms" in result


def test_labeled_sparkline_empty_values():
    result = labeled_sparkline("x", [])
    assert "x:" in result
    assert "nan" in result
