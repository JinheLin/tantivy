# P4-15 MergePolicy 与后台合并：段数量、空间回收与性能

> 本文主问题：为什么需要 merge？merge policy 决定了什么？后台合并如何与写入并发？

## 本文目标

- 理解 merge 的两个核心收益：减少 segment 数量、清理 tombstone
- 读懂 merge policy 的接口与默认实现（LogMergePolicy）
- 了解 merge operation/merger 的大体流程

## 源码入口（建议阅读顺序）

1. `ARCHITECTURE.md` 的 Merges 小节
2. `src/indexer/merge_policy.rs`：MergePolicy trait
3. `src/indexer/log_merge_policy.rs`：默认策略
4. `src/indexer/merge_operation.rs` / `merger.rs`：合并执行
5. `src/indexer/segment_updater.rs`：合并与 segment 生命周期管理

## 可运行实验（建议）

先跑多线程写入示例，观察段数量变化（需要你在代码里加一点点日志/输出）。

```bash
cargo run --example index_from_multiple_threads
```

### 验证点

- 你能解释：为什么 segment 太多会拖慢搜索（term lookup * segment_count）
- 你能描述：merge 如何在后台进行而不阻塞写入主流程（高层即可）

## TODO

- [ ] 做一张“写入线程/合并线程/搜索线程”的并发关系图
- [ ] FAQ：merge policy 调参的常见方向有哪些？

