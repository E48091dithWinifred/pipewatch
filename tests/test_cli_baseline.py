"""Tests for pipewatch.cli_baseline."""
import json
import os
import pytest

from pipewatch.cli_baseline import _load_statuses, cmd_baseline_capture, cmd_baseline_drift
from pipewatch.checker import AlertLevel


@pytest.fixture()
def status_file(tmp_path):
    data = [
        {"pipeline_name": "pipe_a", "level": "OK", "error_rate": 0.01, "latency_ms": 120.0, "message": ""},
        {"pipeline_name": "pipe_b", "level": "WARNING", "error_rate": 0.08, "latency_ms": 300.0, "message": "slow"},
    ]
    p = tmp_path / "statuses.json"
    p.write_text(json.dumps(data))
    return str(p)


def test_load_statuses_returns_list(status_file):
    result = _load_statuses(status_file)
    assert len(result) == 2


def test_load_statuses_correct_level(status_file):
    result = _load_statuses(status_file)
    assert result[0].level == AlertLevel.OK
    assert result[1].level == AlertLevel.WARNING


def test_cmd_baseline_capture_creates_file(status_file, tmp_path, capsys):
    class Args:
        statuses_file = status_file
        dir = str(tmp_path / "bl")
        tag = "test"

    cmd_baseline_capture(Args())
    assert os.path.exists(os.path.join(Args.dir, "baseline_test.json"))
    captured = capsys.readouterr()
    assert "captured" in captured.out
    assert "2 pipeline" in captured.out


def test_cmd_baseline_drift_no_baseline_exits(status_file, tmp_path):
    class Args:
        statuses_file = status_file
        dir = str(tmp_path / "empty")
        tag = "missing"
        no_color = True

    with pytest.raises(SystemExit) as exc:
        cmd_baseline_drift(Args())
    assert exc.value.code == 1


def test_cmd_baseline_drift_no_drift(status_file, tmp_path, capsys):
    from pipewatch.cli_baseline import cmd_baseline_capture

    bl_dir = str(tmp_path / "bl")

    class CaptureArgs:
        statuses_file = status_file
        dir = bl_dir
        tag = "v1"

    cmd_baseline_capture(CaptureArgs())
    capsys.readouterr()

    class DriftArgs:
        statuses_file = status_file
        dir = bl_dir
        tag = "v1"
        no_color = True

    cmd_baseline_drift(DriftArgs())
    captured = capsys.readouterr()
    assert "within baseline tolerance" in captured.out


def test_cmd_baseline_drift_detects_degradation(tmp_path, capsys):
    from pipewatch.cli_baseline import cmd_baseline_capture

    bl_dir = str(tmp_path / "bl")
    original = [
        {"pipeline_name": "pipe_a", "level": "OK", "error_rate": 0.01, "latency_ms": 100.0, "message": ""}
    ]
    orig_file = str(tmp_path / "orig.json")
    with open(orig_file, "w") as fh:
        json.dump(original, fh)

    class CaptureArgs:
        statuses_file = orig_file
        dir = bl_dir
        tag = "v1"

    cmd_baseline_capture(CaptureArgs())
    capsys.readouterr()

    degraded = [
        {"pipeline_name": "pipe_a", "level": "CRITICAL", "error_rate": 0.20, "latency_ms": 500.0, "message": "bad"}
    ]
    deg_file = str(tmp_path / "deg.json")
    with open(deg_file, "w") as fh:
        json.dump(degraded, fh)

    class DriftArgs:
        statuses_file = deg_file
        dir = bl_dir
        tag = "v1"
        no_color = True

    with pytest.raises(SystemExit) as exc:
        cmd_baseline_drift(DriftArgs())
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert "DRIFT" in captured.out
