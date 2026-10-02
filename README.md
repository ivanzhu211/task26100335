# flow-engine

进程内集成流执行引擎：流为有向无环图，支持多执行并发、指数退避重试、取消与汇合语义。

## 用法

```python
from flow_engine import Edge, Engine, Flow, Node, NodeKind

flow = Flow(
    nodes={
        "check": Node("check", NodeKind.CONDITION, lambda x: x > 0),
        "pos": Node("pos", NodeKind.TASK, lambda x: x["check"] * 2),
    },
    edges=[Edge("check", "pos", "true")],
)

engine = Engine()  # 默认重试退避 2s/4s/8s...
exec_id = engine.submit(flow, payload=1, max_attempts=3)
report = engine.wait(exec_id)   # 阻塞到结束，返回每节点状态/输入/输出/尝试次数
# engine.cancel(exec_id)        # 随时取消
```

- 无入边节点以触发载荷为输入；其余节点输入为 `{活前驱id: 其输出}`。
- 条件节点出边标 `"true"/"false"`，仅标签匹配的边为活，下游拿到条件节点的原样输入。
- 汇合：入边全部确定后决定去留；至少一条活入边即执行，全死则跳过且其出边全死。
- 节点用尽尝试次数则执行失败；失败/取消后不再开始新尝试，在途尝试跑完并记录但不向下游推进。

## 测试

```bash
python3 -m pytest tests/ -q
```
