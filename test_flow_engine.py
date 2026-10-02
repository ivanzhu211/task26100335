import threading
import time

import pytest

from flow_engine import (
    Edge,
    ExecutionStatus,
    Flow,
    FlowEngine,
    Node,
    NodeKind,
    NodeStatus,
)


@pytest.fixture
def engine():
    eng = FlowEngine()
    yield eng
    eng.shutdown()


def test_nested_conditions_with_join(engine):
    """Outer condition true, inner condition false; two live paths join.

    Topology:
        src -> c1 --true--> t1 -> c2 --false--> t2 --+
                |            |                       |
                |            +----------------------+--> join
                |                     c2 --true--> inner_true -> leaf_dead
                +--false--> dead1 -> dead2
    """
    flow = Flow(
        nodes={
            "src": Node("src", NodeKind.TASK, lambda x: x + 1),
            "c1": Node("c1", NodeKind.CONDITION, lambda m: True),
            "t1": Node("t1", NodeKind.TASK, lambda m: ("t1", m)),
            "dead1": Node("dead1", NodeKind.TASK, lambda m: "dead1"),
            "dead2": Node("dead2", NodeKind.TASK, lambda m: "dead2"),
            "c2": Node("c2", NodeKind.CONDITION, lambda m: False),
            "inner_true": Node("inner_true", NodeKind.TASK, lambda m: "inner"),
            "leaf_dead": Node("leaf_dead", NodeKind.TASK, lambda m: "leaf"),
            "t2": Node("t2", NodeKind.TASK, lambda m: ("t2", m)),
            "join": Node("join", NodeKind.TASK, lambda m: m),
        },
        edges=[
            Edge("src", "c1"),
            Edge("c1", "t1", "true"),
            Edge("c1", "dead1", "false"),
            Edge("dead1", "dead2"),
            Edge("t1", "c2"),
            Edge("c2", "inner_true", "true"),
            Edge("inner_true", "leaf_dead"),
            Edge("c2", "t2", "false"),
            Edge("t1", "join"),
            Edge("t2", "join"),
        ],
    )
    eid = engine.submit(flow, payload=10, max_attempts=1)
    results = engine.wait(eid, timeout=5)

    assert engine.status(eid) is ExecutionStatus.SUCCEEDED

    executed = {"src", "c1", "t1", "c2", "t2", "join"}
    skipped = {"dead1", "dead2", "inner_true", "leaf_dead"}
    for node_id in executed:
        assert results[node_id].status is NodeStatus.SUCCEEDED, node_id
        assert results[node_id].attempts == 1, node_id
    for node_id in skipped:
        assert results[node_id].status is NodeStatus.SKIPPED, node_id
        assert results[node_id].attempts == 0, node_id
        assert results[node_id].output is None, node_id

    # Source node gets the trigger payload.
    assert results["src"].input == 10
    assert results["src"].output == 11

    # Condition node downstream receives the condition's original input.
    assert results["c1"].input == {"src": 11}
    assert results["t1"].input == {"c1": {"src": 11}}

    # Inner condition false: t2 gets c2's original input (which is t1's output).
    t1_output = results["t1"].output
    assert results["c2"].input == {"t1": t1_output}
    assert results["t2"].input == {"c2": {"t1": t1_output}}

    # Join node input contains only live predecessors.
    assert set(results["join"].input) == {"t1", "t2"}
    assert results["join"].input["t1"] == t1_output
    assert results["join"].input["t2"] == results["t2"].output


def test_retry_with_backoff_then_success(engine):
    calls = []

    def flaky(x):
        calls.append(time.monotonic())
        if len(calls) < 3:
            raise RuntimeError(f"boom {len(calls)}")
        return x * 2

    flow = Flow(
        nodes={"a": Node("a", NodeKind.TASK, flaky)},
        edges=[],
    )
    start = time.monotonic()
    eid = engine.submit(flow, payload=21, max_attempts=3)
    results = engine.wait(eid, timeout=15)
    elapsed = time.monotonic() - start

    assert engine.status(eid) is ExecutionStatus.SUCCEEDED
    assert results["a"].status is NodeStatus.SUCCEEDED
    assert results["a"].attempts == 3
    assert results["a"].output == 42
    # First retry waits 2s, second waits 4s.
    assert elapsed >= 6.0
    assert calls[1] - calls[0] >= 1.9
    assert calls[2] - calls[1] >= 3.9


def test_node_failure_fails_execution(engine):
    ran_downstream = threading.Event()

    def always_fails(x):
        raise ValueError("nope")

    def downstream(m):
        ran_downstream.set()
        return m

    flow = Flow(
        nodes={
            "bad": Node("bad", NodeKind.TASK, always_fails),
            "after": Node("after", NodeKind.TASK, downstream),
        },
        edges=[Edge("bad", "after")],
    )
    eid = engine.submit(flow, payload=None, max_attempts=2)
    results = engine.wait(eid, timeout=15)

    assert engine.status(eid) is ExecutionStatus.FAILED
    assert results["bad"].status is NodeStatus.FAILED
    assert results["bad"].attempts == 2
    assert "nope" in results["bad"].error
    # Downstream never starts after a failure.
    assert results["after"].status is NodeStatus.PENDING
    assert results["after"].attempts == 0
    assert not ran_downstream.is_set()


def test_cancel_lets_running_attempt_finish_without_propagating(engine):
    started = threading.Event()
    release = threading.Event()
    downstream_ran = threading.Event()

    def slow(x):
        started.set()
        release.wait(timeout=5)
        return "slow-done"

    def downstream(m):
        downstream_ran.set()
        return m

    flow = Flow(
        nodes={
            "slow": Node("slow", NodeKind.TASK, slow),
            "after": Node("after", NodeKind.TASK, downstream),
        },
        edges=[Edge("slow", "after")],
    )
    eid = engine.submit(flow, payload=None, max_attempts=1)
    assert started.wait(timeout=5)
    engine.cancel(eid)
    release.set()
    results = engine.wait(eid, timeout=5)

    assert engine.status(eid) is ExecutionStatus.CANCELLED
    # The in-flight attempt ran to completion and was recorded...
    assert results["slow"].status is NodeStatus.SUCCEEDED
    assert results["slow"].output == "slow-done"
    assert results["slow"].attempts == 1
    # ...but its output never propagated downstream.
    assert results["after"].status is NodeStatus.PENDING
    assert results["after"].attempts == 0
    assert not downstream_ran.is_set()


def test_cancel_abandons_pending_retry(engine):
    first_attempt_done = threading.Event()
    calls = []

    def always_fails(x):
        calls.append(time.monotonic())
        first_attempt_done.set()
        raise RuntimeError("fail")

    flow = Flow(
        nodes={"a": Node("a", NodeKind.TASK, always_fails)},
        edges=[],
    )
    eid = engine.submit(flow, payload=None, max_attempts=5)
    assert first_attempt_done.wait(timeout=5)
    engine.cancel(eid)  # cancels while the 2s retry backoff is pending
    results = engine.wait(eid, timeout=5)

    assert engine.status(eid) is ExecutionStatus.CANCELLED
    assert results["a"].attempts == 1
    # Give the abandoned retry a chance to (incorrectly) fire.
    time.sleep(0.3)
    assert results["a"].attempts == 1


def test_multiple_concurrent_executions(engine):
    flow = Flow(
        nodes={
            "double": Node("double", NodeKind.TASK, lambda x: x * 2),
            "plus": Node("plus", NodeKind.TASK, lambda m: m["double"] + 1),
        },
        edges=[Edge("double", "plus")],
    )
    eid1 = engine.submit(flow, payload=1, max_attempts=1)
    eid2 = engine.submit(flow, payload=10, max_attempts=1)
    assert eid1 != eid2

    res1 = engine.wait(eid1, timeout=5)
    res2 = engine.wait(eid2, timeout=5)
    assert res1["plus"].output == 3
    assert res2["plus"].output == 21
    assert engine.status(eid1) is ExecutionStatus.SUCCEEDED
    assert engine.status(eid2) is ExecutionStatus.SUCCEEDED
