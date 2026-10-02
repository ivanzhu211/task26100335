"""In-process DAG flow execution engine."""

from __future__ import annotations

import itertools
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from .model import Edge, ExecutionStatus, Flow, NodeKind, NodeResult, NodeStatus


class _Execution:
    """Mutable state of one running flow execution."""

    def __init__(self, flow: Flow, payload: Any, max_attempts: int) -> None:
        self.flow = flow
        self.payload = payload
        self.max_attempts = max(1, max_attempts)
        self.status = ExecutionStatus.RUNNING
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.cancelled = threading.Event()
        self.results: Dict[str, NodeResult] = {
            node_id: NodeResult() for node_id in flow.nodes
        }
        self.edges: List[Edge] = list(flow.edges)
        self.in_edges: Dict[str, List[int]] = {node_id: [] for node_id in flow.nodes}
        self.out_edges: Dict[str, List[int]] = {node_id: [] for node_id in flow.nodes}
        for idx, edge in enumerate(self.edges):
            self.out_edges[edge.src].append(idx)
            self.in_edges[edge.dst].append(idx)
        self.edge_alive: Dict[int, Optional[bool]] = {
            idx: None for idx in range(len(self.edges))
        }
        self.propagated: Dict[str, Any] = {}
        self.remaining = len(flow.nodes)
        self.in_flight = 0


class FlowEngine:
    """Runs multiple flow executions concurrently inside one process."""

    def __init__(self, max_workers: int = 8) -> None:
        self._executions: Dict[str, _Execution] = {}
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def submit(self, flow: Flow, payload: Any, max_attempts: int = 3) -> str:
        """Start an asynchronous execution and return its execution id."""
        exe = _Execution(flow, payload, max_attempts)
        with self._lock:
            execution_id = f"exec-{next(self._ids)}"
            self._executions[execution_id] = exe
        sources = [nid for nid in flow.nodes if not exe.in_edges[nid]]
        with exe.lock:
            if exe.remaining == 0:
                exe.status = ExecutionStatus.SUCCEEDED
                exe.done.set()
        for node_id in sources:
            exe.results[node_id].input = payload
            self._run_attempt(exe, node_id)
        return execution_id

    def cancel(self, execution_id: str) -> None:
        """Cancel a running execution."""
        exe = self._get(execution_id)
        with exe.lock:
            if exe.status is ExecutionStatus.RUNNING:
                exe.status = ExecutionStatus.CANCELLED
                exe.cancelled.set()
                self._maybe_done_locked(exe)

    def wait(self, execution_id: str, timeout: Optional[float] = None) -> Dict[str, NodeResult]:
        """Block until the execution finishes; return per-node results."""
        exe = self._get(execution_id)
        if not exe.done.wait(timeout):
            raise TimeoutError(f"execution {execution_id} did not finish in time")
        with exe.lock:
            return {
                node_id: NodeResult(
                    status=res.status,
                    input=res.input,
                    output=res.output,
                    attempts=res.attempts,
                    error=res.error,
                )
                for node_id, res in exe.results.items()
            }

    def status(self, execution_id: str) -> ExecutionStatus:
        return self._get(execution_id).status

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False)

    def _get(self, execution_id: str) -> _Execution:
        with self._lock:
            try:
                return self._executions[execution_id]
            except KeyError:
                raise KeyError(f"unknown execution id: {execution_id}") from None

    def _maybe_done_locked(self, exe: _Execution) -> None:
        if exe.status is not ExecutionStatus.RUNNING and exe.in_flight == 0:
            exe.done.set()

    def _run_attempt(self, exe: _Execution, node_id: str) -> None:
        with exe.lock:
            if exe.status is not ExecutionStatus.RUNNING:
                return
            res = exe.results[node_id]
            res.attempts += 1
            res.status = NodeStatus.RUNNING
            exe.in_flight += 1
            input_value = res.input
        future = self._executor.submit(exe.flow.nodes[node_id].fn, input_value)
        future.add_done_callback(lambda fut: self._on_attempt_done(exe, node_id, fut))

    def _schedule_retry(self, exe: _Execution, node_id: str, delay: float) -> None:
        def waiter() -> None:
            if exe.cancelled.wait(delay):
                return  # execution failed or was cancelled while waiting
            self._run_attempt(exe, node_id)

        threading.Thread(target=waiter, daemon=True).start()

    def _on_attempt_done(self, exe: _Execution, node_id: str, future: Future) -> None:
        try:
            output, error = future.result(), None
        except Exception as exc:  # node function raised
            output, error = None, exc
        retry: Optional[float] = None
        ready: List[str] = []
        with exe.lock:
            exe.in_flight -= 1
            res = exe.results[node_id]
            if exe.status is not ExecutionStatus.RUNNING:
                # Execution already failed/cancelled: record the finished
                # attempt but do not propagate anything downstream.
                if error is None:
                    res.status = NodeStatus.SUCCEEDED
                    res.output = output
                else:
                    res.status = NodeStatus.FAILED
                    res.error = repr(error)
                self._maybe_done_locked(exe)
                return
            if error is None:
                res.status = NodeStatus.SUCCEEDED
                res.output = output
                ready = self._propagate_locked(exe, node_id)
            elif res.attempts >= exe.max_attempts:
                res.status = NodeStatus.FAILED
                res.error = repr(error)
                exe.status = ExecutionStatus.FAILED
                exe.cancelled.set()
                self._maybe_done_locked(exe)
            else:
                res.status = NodeStatus.PENDING
                retry = float(2 ** res.attempts)  # 2s, 4s, 8s, ...
        if retry is not None:
            self._schedule_retry(exe, node_id, retry)
        for ready_id in ready:
            self._run_attempt(exe, ready_id)

    def _propagate_locked(self, exe: _Execution, node_id: str) -> List[str]:
        """Mark out-edges of a finished node and resolve downstream nodes.

        Returns ids of nodes that became runnable and must be started
        outside the lock.
        """
        node = exe.flow.nodes[node_id]
        res = exe.results[node_id]
        exe.propagated[node_id] = (
            res.input if node.kind is NodeKind.CONDITION else res.output
        )
        exe.remaining -= 1
        for idx in exe.out_edges[node_id]:
            edge = exe.edges[idx]
            if node.kind is NodeKind.CONDITION:
                expected = "true" if res.output else "false"
                exe.edge_alive[idx] = edge.label == expected
            else:
                exe.edge_alive[idx] = True
        ready: List[str] = []
        queue = [exe.edges[idx].dst for idx in exe.out_edges[node_id]]
        while queue:
            dst = queue.pop()
            dst_res = exe.results[dst]
            if dst_res.status is not NodeStatus.PENDING or dst_res.attempts > 0:
                continue  # already resolved
            in_idxs = exe.in_edges[dst]
            if any(exe.edge_alive[idx] is None for idx in in_idxs):
                continue  # not all in-edges determined yet
            alive = [idx for idx in in_idxs if exe.edge_alive[idx]]
            if alive:
                dst_res.input = {
                    exe.edges[idx].src: exe.propagated[exe.edges[idx].src]
                    for idx in alive
                }
                ready.append(dst)
            else:
                dst_res.status = NodeStatus.SKIPPED
                exe.remaining -= 1
                for idx in exe.out_edges[dst]:
                    exe.edge_alive[idx] = False
                queue.extend(exe.edges[idx].dst for idx in exe.out_edges[dst])
        if exe.remaining == 0 and exe.status is ExecutionStatus.RUNNING:
            exe.status = ExecutionStatus.SUCCEEDED
            exe.cancelled.set()
            self._maybe_done_locked(exe)
        return ready
