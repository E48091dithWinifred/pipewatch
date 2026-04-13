"""ASCII sparkline rendering for pipeline metric series."""
from __future__ import annotations

from typing import List, Sequence

_BLOCKS = " ▁▂▃▄▅▆▇█"


def sparkline(values: Sequence[float], width: int = 20) -> str:
    """Render a sequence of floats as a compact ASCII sparkline string."""
    if not values:
        return ""

    vals = list(values)[-width:]
    lo, hi = min(vals), max(vals)
    span = hi - lo

    chars: List[str] = []
    for v in vals:
        if span == 0:
            idx = len(_BLOCKS) // 2
        else:
            idx = round((v - lo) / span * (len(_BLOCKS) - 1))
        chars.append(_BLOCKS[idx])

    return "".join(chars)


def labeled_sparkline(
    label: str,
    values: Sequence[float],
    unit: str = "",
    width: int = 20,
) -> str:
    """Return a formatted line: '<label>: [sparkline] latest=<val><unit>'."""
    spark = sparkline(values, width=width)
    latest = values[-1] if values else float("nan")
    return f"{label}: [{spark}] latest={latest:.2f}{unit}"
