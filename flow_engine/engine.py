"""进程内集成流执行引擎。

流为有向无环图，节点分普通节点与条件节点，支持并发执行、
指数退避重试、取消与汇合（join）语义。
"""
from __future__ import annotations

import enum
import itertools
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


class NodeKind(enum.Enum):
    TASK = "task"
    CONDITION = "condition"


class NodeStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ExecutionStatus(enum.Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Node:
    node_id: str
    kind: NodeKind
    func: Callable[[Any], Any]


@dataclass
class Edge:
    src: str
    dst: str
    label: Optional[str] = None  # 条件节点出边为 "true"/"false"，普通节点为 None


@dataclass
class Flow:
    nodes: Dict[str, Node] = field(default_factory=dict)
    edges: List[Edge] = field(default_factory=list)


@dataclass
class NodeResult:
    status: NodeStatus = NodeStatus.PENDING
    input: Any = None
    output: Any = None
    attempts: int = 0
    error: Optional[str] = None


@dataclass
class ExecutionReport:
    status: ExecutionStatus
    nodes: Dict[str, NodeResult]


class _Execution:
    """单次执行的内部状态，所有状态迁移都在 _lock 下完成。"""

    def __init__(self, engine: "Engine", execution_id: str, flow: Flow,
                 payload: Any, max_attempts: int) -> None:
        self.engine = engine
        self.execution_id = execution_id
        self.flow = flow
        self.payload = payload
        self.max_attempts = max(1, max_attempts)

        self.status = ExecutionStatus.RUNNING
        self.results: Dict[str, NodeResult] = {
            nid: NodeResult() for nid in flow.nodes
        }
        self.done = threading.Event()
        self.lock = threading.Lock()

        self.in_edges: Dict[str, List[Edge]] = {nid: [] for nid in flow.nodes}
        self.out_edges: Dict[str, List[Edge]] = {nid: [] for nid in flow.nodes}
        for edge in flow.edges:
            self.out_edges[edge.src].append(edge)
            self.in_edges[edge.dst].append(edge)

        # 每条入边是否已确定（活/死），以及活前驱的输出
        self.undetermined: Dict[str, int] = {
            nid: len(edges) for nid, edges in self.in_edges.items()
        }
        self.live_inputs: Dict[str, Dict[str, Any]] = {nid: {} for nid in flow.nodes}
        self.unresolved = len(flow.nodes)  # 尚未 SUCCESS/FAILED/SKIPPED 的节点数
        self.timers: List[threading.Timer] = []
        self.in_flight = 0  # 正在运行的尝试数

    # ---- 调度入口 ----

    def start(self) -> None:
        with self.lock:
            for nid, edges in self.in_edges.items():
                if not edges:
                    self.results[nid].input = self.payload
                    self._start_attempt_locked(nid)
            self._check_finished_locked()

    # ---- 节点尝试 ----

    def _start_attempt_locked(self, nid: str) -> None:
        result = self.results[nid]
        result.status = NodeStatus.RUNNING
        result.attempts += 1
        self.in_flight += 1
        self.engine._executor.submit(self._run_attempt, nid, result.input)

    def _run_attempt(self, nid: str, node_input: Any) -> None:
        node = self.flow.nodes[nid]
        error: Optional[str] = None
        output: Any = None
        try:
            output = node.func(node_input)
        except Exception as exc:  # noqa: BLE001 - 节点异常需记录并触发重试
            error = f"{type(exc).__name__}: {exc}"
        with self.lock:
            self._on_attempt_done_locked(nid, output, error)

    def _on_attempt_done_locked(self, nid: str, output: Any, error: Optional[str]) -> None:
        result = self.results[nid]
        self.in_flight -= 1
        if self.status is not ExecutionStatus.RUNNING:
            # 执行已失败/取消：跑完的尝试照常记录，但不向下游推进
            if error is None:
                result.status = NodeStatus.SUCCESS
                result.output = output
            else:
                result.error = error
            self._maybe_set_done_locked()
            return

        if error is not None:
            result.error = error
            if result.attempts < self.max_attempts:
                delay = self.engine._retry_base_delay * (2 ** (result.attempts - 1))
                timer = threading.Timer(delay, self._retry, args=(nid,))
                timer.daemon = True
                self.timers.append(timer)
                timer.start()
            else:
                result.status = NodeStatus.FAILED
                self._terminate_locked(ExecutionStatus.FAILED)
            return

        result.status = NodeStatus.SUCCESS
        result.output = output
        self.unresolved -= 1

        node = self.flow.nodes[nid]
        if node.kind is NodeKind.CONDITION:
            live_label = "true" if output else "false"
            propagated = result.input  # 下游拿到条件节点的原样输入
            for edge in self.out_edges[nid]:
                self._resolve_edge_locked(edge, edge.label == live_label, propagated)
        else:
            for edge in self.out_edges[nid]:
                self._resolve_edge_locked(edge, True, output)
        self._check_finished_locked()

    def _retry(self, nid: str) -> None:
        with self.lock:
            if self.status is not ExecutionStatus.RUNNING:
                return  # 等待中的重试直接放弃
            if self.results[nid].status is not NodeStatus.RUNNING:
                return
            self._start_attempt_locked(nid)

    # ---- 边与汇合 ----

    def _resolve_edge_locked(self, edge: Edge, alive: bool, value: Any) -> None:
        dst = edge.dst
        if self.results[dst].status is not NodeStatus.PENDING:
            return  # 下游节点已确定去留
        self.undetermined[dst] -= 1
        if alive:
            self.live_inputs[dst][edge.src] = value
        if self.undetermined[dst] > 0:
            return

        if self.live_inputs[dst]:
            result = self.results[dst]
            result.input = dict(self.live_inputs[dst])
            self._start_attempt_locked(dst)
        else:
            # 入边全死：跳过，且其出边全部算死
            self.results[dst].status = NodeStatus.SKIPPED
            self.unresolved -= 1
            for out in self.out_edges[dst]:
                self._resolve_edge_locked(out, False, None)

    # ---- 终态 ----

    def _check_finished_locked(self) -> None:
        if self.status is ExecutionStatus.RUNNING and self.unresolved == 0:
            self._terminate_locked(ExecutionStatus.SUCCEEDED)

    def _terminate_locked(self, status: ExecutionStatus) -> None:
        self.status = status
        for timer in self.timers:
            timer.cancel()
        self._maybe_set_done_locked()

    def _maybe_set_done_locked(self) -> None:
        # 终态后仍等在途尝试跑完并记录，执行才算结束
        if self.status is not ExecutionStatus.RUNNING and self.in_flight == 0:
            self.done.set()

    def cancel(self) -> None:
        with self.lock:
            if self.status is ExecutionStatus.RUNNING:
                self._terminate_locked(ExecutionStatus.CANCELLED)


class Engine:
    """集成流执行引擎，可同时运行多个执行。"""

    def __init__(self, max_workers: int = 8, retry_base_delay: float = 2.0) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._retry_base_delay = retry_base_delay
        self._executions: Dict[str, _Execution] = {}
        self._id_counter = itertools.count(1)
        self._lock = threading.Lock()

    def submit(self, flow: Flow, payload: Any, max_attempts: int = 3) -> str:
        with self._lock:
            execution_id = f"exec-{next(self._id_counter)}"
            execution = _Execution(self, execution_id, flow, payload, max_attempts)
            self._executions[execution_id] = execution
        execution.start()
        return execution_id

    def cancel(self, execution_id: str) -> None:
        self._get(execution_id).cancel()

    def wait(self, execution_id: str, timeout: Optional[float] = None) -> ExecutionReport:
        execution = self._get(execution_id)
        if not execution.done.wait(timeout):
            raise TimeoutError(f"execution {execution_id} did not finish in time")
        with execution.lock:
            nodes = {
                nid: NodeResult(
                    status=r.status,
                    input=r.input,
                    output=r.output,
                    attempts=r.attempts,
                    error=r.error,
                )
                for nid, r in execution.results.items()
            }
            return ExecutionReport(status=execution.status, nodes=nodes)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False)

    def _get(self, execution_id: str) -> _Execution:
        with self._lock:
            try:
                return self._executions[execution_id]
            except KeyError:
                raise KeyError(f"unknown execution id: {execution_id}") from None
