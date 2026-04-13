"""Route pipeline statuses to named output channels based on alert level."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class RouteRule:
    """A single routing rule mapping levels to a channel name."""

    channel: str
    levels: List[str] = field(default_factory=list)  # e.g. ["WARNING", "CRITICAL"]
    name_prefix: Optional[str] = None

    def matches(self, status: PipelineStatus) -> bool:
        level_match = (
            not self.levels or status.level.name in self.levels
        )
        prefix_match = (
            self.name_prefix is None
            or status.pipeline_name.startswith(self.name_prefix)
        )
        return level_match and prefix_match


@dataclass
class RouterConfig:
    """Collection of routing rules."""

    rules: List[RouteRule] = field(default_factory=list)
    default_channel: str = "default"


Rout = Dict[str, List[PipelineStatus]]
ChannelHandler = Callable[[str, List[PipelineStatus]], None]


def route_statuses(
    statuses: List[PipelineStatus],
    config: RouterConfig,
) -> RoutingTable:
    """Distribute statuses into channels according to routing rules.

    A status is placed in the channel of the *first* matching rule.
    If no rule matches, it goes to ``config.default_channel``.
    """
    table: RoutingTable = {}

    for status in statuses:
        channel = config.default_channel
        for rule in config.rules:
            if rule.matches(status):
                channel = rule.channel
                break
        table.setdefault(channel, []).append(status)

    return table


def dispatch_channels(
    table: RoutingTable,
    handler: ChannelHandler,
) -> None:
    """Call *handler* once per channel with its list of statuses."""
    for channel, statuses in table.items():
        handler(channel, statuses)
