"""Type definitions for the in-process flow execution engine."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


class NodeKind(enum.Enum):
    TASK = "task"
    CONDITION = "condition"


class NodeStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class ExecutionStatus(enum.Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class Node:
    node_id: str
    kind: NodeKind
    fn: Callable[[Any], Any]


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    label: Optional[str] = None  # "true" / "false" on condition out-edges


@dataclass(frozen=True)
class Flow:
    nodes: Dict[str, Node]
    edges: List[Edge]


@dataclass
class NodeResult:
    status: NodeStatus = NodeStatus.PENDING
    input: Any = None
    output: Any = None
    attempts: int = 0
    error: Optional[str] = None
