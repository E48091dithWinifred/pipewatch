"""Pipeline dependency mapper — tracks upstream/downstream relationships."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class PipelineNode:
    name: str
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)

    def has_upstream(self) -> bool:
        return len(self.upstream) > 0

    def has_downstream(self) -> bool:
        return len(self.downstream) > 0

    def summary(self) -> str:
        up = ", ".join(self.upstream) if self.upstream else "none"
        down = ", ".join(self.downstream) if self.downstream else "none"
        return f"{self.name} | upstream: {up} | downstream: {down}"


@dataclass
class DependencyMap:
    nodes: Dict[str, PipelineNode] = field(default_factory=dict)

    def add_node(self, name: str) -> PipelineNode:
        if name not in self.nodes:
            self.nodes[name] = PipelineNode(name=name)
        return self.nodes[name]

    def add_edge(self, upstream: str, downstream: str) -> None:
        up_node = self.add_node(upstream)
        down_node = self.add_node(downstream)
        if downstream not in up_node.downstream:
            up_node.downstream.append(downstream)
        if upstream not in down_node.upstream:
            down_node.upstream.append(upstream)

    def get(self, name: str) -> Optional[PipelineNode]:
        return self.nodes.get(name)

    def roots(self) -> List[str]:
        """Return pipelines with no upstream dependencies."""
        return [n for n, node in self.nodes.items() if not node.has_upstream()]

    def leaves(self) -> List[str]:
        """Return pipelines with no downstream dependents."""
        return [n for n, node in self.nodes.items() if not node.has_downstream()]


def build_map(edges: List[Dict[str, str]]) -> DependencyMap:
    """Build a DependencyMap from a list of {upstream, downstream} edge dicts."""
    dep_map = DependencyMap()
    for edge in edges:
        dep_map.add_edge(edge["upstream"], edge["downstream"])
    return dep_map


def affected_by(dep_map: DependencyMap, failed: str) -> List[str]:
    """Return all pipelines transitively downstream of *failed*."""
    visited: List[str] = []
    queue = list(dep_map.nodes.get(failed, PipelineNode(failed)).downstream)
    while queue:
        name = queue.pop(0)
        if name not in visited:
            visited.append(name)
            node = dep_map.nodes.get(name)
            if node:
                queue.extend(node.downstream)
    return visited
