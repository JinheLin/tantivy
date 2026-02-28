# P3-09 Searcher 快照一致性：SegmentReader 与 generation

> 本文主问题：为什么 commit 后旧的 searcher 看不到新数据？这如何保证一致性？

## 本文目标

- 读懂 Searcher 持有的是什么（SegmentReader 列表 + generation）
- 理解：Searcher 是“不可变快照”，如何避免并发写导致的不一致
- 了解 Warmer API 为什么需要 generation 信息

## 源码入口（建议阅读顺序）

1. `src/core/searcher.rs`：Searcher、SearcherGeneration、search 执行流程
2. `src/index/segment_reader.rs`：SegmentReader 打开与组件访问
3. `src/reader/mod.rs` / `src/reader/warming.rs`：IndexReader、ReloadPolicy、Warmer
4. `examples/warmer.rs`：warm 的使用示例

## 可运行实验

```bash
cargo run --example warmer
```

### 验证点

- 你能用一句话解释：searcher 为什么是快照
- 你能解释：reload policy 的差异（手动 vs on_commit 等）

## TODO

- [ ] 画 Searcher/IndexReader/IndexWriter 的并发关系图
- [ ] FAQ：searcher 快照会导致什么“读到旧数据”的风险？

