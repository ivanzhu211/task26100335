import threading
import time

import pytest

from flow_engine import (
    Edge,
    Engine,
    ExecutionStatus,
    Flow,
    Node,
    NodeKind,
    NodeStatus,
)


def task(nid, func):
    return Node(nid, NodeKind.TASK, func)


def cond(nid, func):
    return Node(nid, NodeKind.CONDITION, func)


@pytest.fixture
def engine():
    eng = Engine(retry_base_delay=0.01)
    yield eng
    eng.shutdown()


def test_linear_flow(engine):
    flow = Flow(
        nodes={
            "a": task("a", lambda x: x + 1),
            "b": task("b", lambda x: x["a"] * 2),
        },
        edges=[Edge("a", "b")],
    )
    report = engine.wait(engine.submit(flow, 1))
    assert report.status is ExecutionStatus.SUCCEEDED
    assert report.nodes["a"].output == 2
    assert report.nodes["b"].input == {"a": 2}
    assert report.nodes["b"].output == 4
    assert report.nodes["b"].attempts == 1


def test_nested_conditions_with_join(engine):
    # 外层条件 outer 走真，内层条件 inner 走假；
    # 活路 outer->inner->work 与死路下的 dead_only 汇合到 join。
    flow = Flow(
        nodes={
            "outer": cond("outer", lambda x: True),
            "inner": cond("inner", lambda x: False),
            "work": task("work", lambda x: ("work", x)),
            "dead_only": task("dead_only", lambda x: ("dead_only", x)),
            "other_branch": task("other_branch", lambda x: ("other", x)),
            "join": task("join", lambda x: ("join", x)),
        },
        edges=[
            Edge("outer", "inner", "true"),
            Edge("outer", "dead_only", "false"),
            Edge("inner", "work", "false"),
            Edge("inner", "other_branch", "true"),
            Edge("dead_only", "join"),
            Edge("work", "join"),
            Edge("other_branch", "join"),
        ],
    )
    report = engine.wait(engine.submit(flow, "payload"))
    assert report.status is ExecutionStatus.SUCCEEDED

    # 谁执行、谁跳过
    assert report.nodes["outer"].status is NodeStatus.SUCCESS
    assert report.nodes["inner"].status is NodeStatus.SUCCESS
    assert report.nodes["work"].status is NodeStatus.SUCCESS
    assert report.nodes["join"].status is NodeStatus.SUCCESS
    assert report.nodes["dead_only"].status is NodeStatus.SKIPPED
    assert report.nodes["other_branch"].status is NodeStatus.SKIPPED

    # 条件节点下游拿到的是条件节点的原样输入
    assert report.nodes["inner"].input == {"outer": "payload"}
    assert report.nodes["work"].input == {"inner": {"outer": "payload"}}

    # 汇合节点输入里只有活前驱
    assert set(report.nodes["join"].input) == {"work"}
    assert report.nodes["join"].input["work"] == ("work", {"inner": {"outer": "payload"}})

    # 跳过节点不消耗尝试次数
    assert report.nodes["dead_only"].attempts == 0
    assert report.nodes["other_branch"].attempts == 0


def test_retry_with_exponential_backoff(engine):
    calls = []
    timestamps = []

    def flaky(x):
        calls.append(1)
        timestamps.append(time.monotonic())
        if len(calls) < 3:
            raise RuntimeError("boom")
        return "ok"

    flow = Flow(nodes={"n": task("n", flaky)})
    report = engine.wait(engine.submit(flow, None, max_attempts=3))
    assert report.status is ExecutionStatus.SUCCEEDED
    assert report.nodes["n"].attempts == 3
    assert report.nodes["n"].output == "ok"
    # 退避：第一次等 base*1，第二次等 base*2
    d1 = timestamps[1] - timestamps[0]
    d2 = timestamps[2] - timestamps[1]
    assert d1 >= 0.01
    assert d2 >= 0.02


def test_node_failure_fails_execution(engine):
    def always_fail(x):
        raise ValueError("nope")

    downstream_ran = []
    flow = Flow(
        nodes={
            "bad": task("bad", always_fail),
            "after": task("after", lambda x: downstream_ran.append(1)),
        },
        edges=[Edge("bad", "after")],
    )
    report = engine.wait(engine.submit(flow, None, max_attempts=2))
    assert report.status is ExecutionStatus.FAILED
    assert report.nodes["bad"].status is NodeStatus.FAILED
    assert report.nodes["bad"].attempts == 2
    assert "nope" in report.nodes["bad"].error
    # 失败后不再推进下游
    assert report.nodes["after"].status is NodeStatus.PENDING
    assert downstream_ran == []


def test_cancel_stops_pending_retries(engine):
    started = threading.Event()
    release = threading.Event()

    def blocker(x):
        started.set()
        release.wait(5)
        raise RuntimeError("fail once")

    flow = Flow(nodes={"n": task("n", blocker)})
    exec_id = engine.submit(flow, None, max_attempts=5)
    assert started.wait(5)
    engine.cancel(exec_id)
    release.set()
    report = engine.wait(exec_id, timeout=5)
    assert report.status is ExecutionStatus.CANCELLED
    # 等待中的重试被放弃：只跑了一次尝试
    time.sleep(0.1)
    report = engine.wait(exec_id)
    assert report.nodes["n"].attempts == 1


def test_cancel_records_running_attempt_without_propagating(engine):
    started = threading.Event()
    release = threading.Event()
    downstream_ran = []

    def slow(x):
        started.set()
        release.wait(5)
        return "slow-done"

    flow = Flow(
        nodes={
            "slow": task("slow", slow),
            "after": task("after", lambda x: downstream_ran.append(1)),
        },
        edges=[Edge("slow", "after")],
    )
    exec_id = engine.submit(flow, None)
    assert started.wait(5)
    engine.cancel(exec_id)
    release.set()
    report = engine.wait(exec_id, timeout=5)
    assert report.status is ExecutionStatus.CANCELLED
    # 正在跑的尝试跑完并记录，但输出不向下游推进
    assert report.nodes["slow"].status is NodeStatus.SUCCESS
    assert report.nodes["slow"].output == "slow-done"
    assert report.nodes["after"].status is NodeStatus.PENDING
    assert downstream_ran == []


def test_concurrent_executions(engine):
    flow = Flow(
        nodes={
            "a": task("a", lambda x: x * 2),
            "b": task("b", lambda x: x["a"] + 1),
        },
        edges=[Edge("a", "b")],
    )
    ids = [engine.submit(flow, i) for i in range(10)]
    reports = [engine.wait(eid, timeout=5) for eid in ids]
    for i, report in enumerate(reports):
        assert report.status is ExecutionStatus.SUCCEEDED
        assert report.nodes["b"].output == i * 2 + 1


def test_wait_unknown_execution(engine):
    with pytest.raises(KeyError):
        engine.wait("nope")


def test_default_retry_delay_is_two_seconds():
    eng = Engine()  # 默认首次失败后等 2 秒重试
    timestamps = []

    def fail_once_then_ok(x):
        timestamps.append(time.monotonic())
        if len(timestamps) == 1:
            raise RuntimeError("boom")
        return "ok"

    try:
        flow = Flow(nodes={"n": task("n", fail_once_then_ok)})
        report = eng.wait(eng.submit(flow, None, max_attempts=2), timeout=10)
        assert report.status is ExecutionStatus.SUCCEEDED
        assert timestamps[1] - timestamps[0] >= 2.0
    finally:
        eng.shutdown()
