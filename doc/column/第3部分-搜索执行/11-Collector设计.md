# P3-11 Collector：把“匹配”与“收集/聚合”解耦

> 本文主问题：Collector 的抽象如何让搜索执行可复用，并承载 TopK/计数/聚合？

## 本文目标

- 读懂 Collector/SegmentCollector 的职责边界
- 跑通一个自定义 collector 的例子
- 理解 requires_scoring 的意义（能关掉就关掉）

## 源码入口（建议阅读顺序）

1. `src/collector/mod.rs`：Collector trait 与常见实现
2. `src/core/searcher.rs`：search 流程如何驱动 collector
3. `examples/custom_collector.rs`：自定义 collector 示例
4. `src/collector/top_collector.rs` / `top_docs` 相关实现（作为经典样例）

## 可运行实验

```bash
cargo run --example custom_collector
```

### 验证点

- 你能解释：Collector 为什么需要分 segment 收集再合并
- 你能说明：Collector 什么时候需要 score，什么时候不需要

## TODO

- [ ] 画一张“search → scorer → collector”的数据流图
- [ ] FAQ：Collector 能否做 early termination？限制是什么？

