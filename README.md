# task26100335

进程内集成流执行引擎（纯 Python 标准库，无外部服务依赖）。

## 模型

- 流是节点 + 有向边组成的 DAG，定义见 `flow_engine/model.py`。
- 普通节点（`NodeKind.TASK`）：处理函数接收输入、返回输出，可阻塞、可抛异常。
- 条件节点（`NodeKind.CONDITION`）：处理函数返回真/假；出边标 `true`/`false`，
  只有标签匹配的出边是活的，下游拿到条件节点的原样输入。
- 无入边的节点以触发载荷为输入；其余节点的输入是「活前驱 id → 其输出」的映射。
- 汇合：节点的所有入边确定后，至少一条活入边才执行（只用活入边数据）；
  全死则跳过，跳过节点的出边全部算死。

## 用法

```python
from flow_engine import Edge, Flow, FlowEngine, Node, NodeKind

engine = FlowEngine()
flow = Flow(
    nodes={
        "a": Node("a", NodeKind.TASK, lambda x: x + 1),
        "b": Node("b", NodeKind.TASK, lambda m: m["a"] * 2),
    },
    edges=[Edge("a", "b")],
)
eid = engine.submit(flow, payload=1, max_attempts=3)  # 异步发起，返回执行 id
results = engine.wait(eid)   # 阻塞到结束，返回每个节点的状态/输入/输出/尝试次数
engine.cancel(eid)           # 取消执行（对未结束的执行有效）
```

节点失败后按 2s、4s、8s… 翻倍退避重试，最多 `max_attempts` 次；用完仍失败则执行失败。
执行失败或被取消后不再开始新尝试，等待中的重试直接放弃；正在跑的尝试会跑完并记录，
但输出不再向下游推进。

## 测试

```bash
python3 -m pytest test_flow_engine.py -v
```
