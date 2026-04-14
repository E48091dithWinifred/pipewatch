"""Tests for pipewatch.mapper."""
import pytest
from pipewatch.mapper import (
    PipelineNode,
    DependencyMap,
    build_map,
    affected_by,
)


# ---------------------------------------------------------------------------
# PipelineNode
# ---------------------------------------------------------------------------

def test_node_has_upstream_false_by_default():
    node = PipelineNode(name="a")
    assert node.has_upstream() is False


def test_node_has_downstream_true_when_populated():
    node = PipelineNode(name="a", downstream=["b"])
    assert node.has_downstream() is True


def test_node_summary_contains_name():
    node = PipelineNode(name="etl_sales", upstream=["raw_sales"], downstream=["report"])
    s = node.summary()
    assert "etl_sales" in s
    assert "raw_sales" in s
    assert "report" in s


def test_node_summary_none_when_empty():
    node = PipelineNode(name="solo")
    assert "none" in node.summary()


# ---------------------------------------------------------------------------
# DependencyMap
# ---------------------------------------------------------------------------

@pytest.fixture()
def simple_map() -> DependencyMap:
    dm = DependencyMap()
    dm.add_edge("ingest", "transform")
    dm.add_edge("transform", "load")
    return dm


def test_add_edge_creates_both_nodes(simple_map):
    assert "ingest" in simple_map.nodes
    assert "load" in simple_map.nodes


def test_add_edge_sets_downstream(simple_map):
    assert "transform" in simple_map.nodes["ingest"].downstream


def test_add_edge_sets_upstream(simple_map):
    assert "ingest" in simple_map.nodes["transform"].upstream


def test_add_edge_no_duplicates():
    dm = DependencyMap()
    dm.add_edge("a", "b")
    dm.add_edge("a", "b")
    assert dm.nodes["a"].downstream.count("b") == 1


def test_roots_returns_nodes_without_upstream(simple_map):
    assert simple_map.roots() == ["ingest"]


def test_leaves_returns_nodes_without_downstream(simple_map):
    assert simple_map.leaves() == ["load"]


def test_get_returns_none_for_unknown(simple_map):
    assert simple_map.get("nonexistent") is None


# ---------------------------------------------------------------------------
# build_map
# ---------------------------------------------------------------------------

def test_build_map_from_edge_list():
    edges = [
        {"upstream": "src", "downstream": "stage"},
        {"upstream": "stage", "downstream": "sink"},
    ]
    dm = build_map(edges)
    assert len(dm.nodes) == 3
    assert "sink" in dm.nodes["stage"].downstream


def test_build_map_empty_edges():
    dm = build_map([])
    assert dm.nodes == {}


# ---------------------------------------------------------------------------
# affected_by
# ---------------------------------------------------------------------------

def test_affected_by_direct_downstream(simple_map):
    result = affected_by(simple_map, "ingest")
    assert "transform" in result
    assert "load" in result


def test_affected_by_leaf_returns_empty(simple_map):
    result = affected_by(simple_map, "load")
    assert result == []


def test_affected_by_unknown_pipeline_returns_empty(simple_map):
    result = affected_by(simple_map, "ghost")
    assert result == []


def test_affected_by_no_duplicates():
    dm = DependencyMap()
    dm.add_edge("a", "c")
    dm.add_edge("b", "c")
    dm.add_edge("c", "d")
    result = affected_by(dm, "a")
    assert result.count("c") == 1
    assert result.count("d") == 1
