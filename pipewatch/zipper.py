"""Zip two lists of pipeline statuses together by name."""
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.checker import PipelineStatus


@dataclass
class ZippedPair:
    name: str
    left: Optional[PipelineStatus]
    right: Optional[PipelineStatus]

    @property
    def both_present(self) -> bool:
        return self.left is not None and self.right is not None

    @property
    def only_in_left(self) -> bool:
        return self.left is not None and self.right is None

    @property
    def only_in_right(self) -> bool:
        return self.right is None and self.right is None

    def summary(self) -> str:
        left_level = self.left.level.value if self.left else "missing"
        right_level = self.right.level.value if self.right else "missing"
        return f"{self.name}: left={left_level} right={right_level}"


@dataclass
class ZipResult:
    pairs: List[ZippedPair]

    @property
    def total(self) -> int:
        return len(self.pairs)

    @property
    def matched(self) -> int:
        return sum(1 for p in self.pairs if p.both_present)

    @property
    def left_only(self) -> int:
        return sum(1 for p in self.pairs if p.only_in_left)

    @property
    def right_only(self) -> int:
        return sum(1 for p in self.pairs if p.only_in_right)

    def summary(self) -> str:
        return (
            f"total={self.total} matched={self.matched} "
            f"left_only={self.left_only} right_only={self.right_only}"
        )


def zip_statuses(
    left: List[PipelineStatus],
    right: List[PipelineStatus],
) -> ZipResult:
    left_map = {s.pipeline_name: s for s in left}
    right_map = {s.pipeline_name: s for s in right}
    all_names = sorted(set(left_map) | set(right_map))
    pairs = [
        ZippedPair(
            name=name,
            left=left_map.get(name),
            right=right_map.get(name),
        )
        for name in all_names
    ]
    return ZipResult(pairs=pairs)
