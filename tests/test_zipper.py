import pytest
from pipewatch.checker import PipelineStatus, AlertLevel
from pipewatch.zipper import ZippedPair, ZipResult, zip_statuses


def _make_status(name: str, level: AlertLevel = AlertLevel.OK) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="ok",
        error_rate=0.0,
        latency_ms=100.0,
    )


@pytest.fixture
def left_statuses():
    return [
        _make_status("alpha", AlertLevel.OK),
        _make_status("beta", AlertLevel.WARNING),
        _make_status("gamma", AlertLevel.CRITICAL),
    ]


@pytest.fixture
def right_statuses():
    return [
        _make_status("alpha", AlertLevel.WARNING),
        _make_status("beta", AlertLevel.OK),
        _make_status("delta", AlertLevel.CRITICAL),
    ]


def test_zip_returns_zip_result(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    assert isinstance(result, ZipResult)


def test_zip_total_includes_all_names(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    assert result.total == 4  # alpha, beta, gamma, delta


def test_zip_matched_count(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    assert result.matched == 2  # alpha, beta


def test_zip_left_only_count(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    assert result.left_only == 1  # gamma


def test_zip_pair_both_present(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    alpha = next(p for p in result.pairs if p.name == "alpha")
    assert alpha.both_present is True


def test_zip_pair_only_in_left(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    gamma = next(p for p in result.pairs if p.name == "gamma")
    assert gamma.only_in_left is True


def test_zip_pair_summary_contains_name(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    alpha = next(p for p in result.pairs if p.name == "alpha")
    assert "alpha" in alpha.summary()


def test_zip_result_summary_contains_counts(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    s = result.summary()
    assert "total=" in s
    assert "matched=" in s


def test_zip_empty_inputs():
    result = zip_statuses([], [])
    assert result.total == 0
    assert result.matched == 0


def test_zip_sorted_by_name(left_statuses, right_statuses):
    result = zip_statuses(left_statuses, right_statuses)
    names = [p.name for p in result.pairs]
    assert names == sorted(names)
