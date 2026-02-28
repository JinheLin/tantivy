# P4-14 Deletes & Alive Bitset：删的是 term，看的是真相

> 本文主问题：Tantivy 的删除为什么是“delete term”？alive bitset 是如何让删除可见的？

## 本文目标

- 读懂 delete 的基本机制：记录 delete 操作 + commit 时生成/更新 delete 文件
- 理解 delete_opstamp 与一致性快照的关系
- 为 merge（回收 tombstone）做铺垫

## 源码入口（建议阅读顺序）

1. `ARCHITECTURE.md` 的 Deletes 小节
2. `src/indexer/index_writer.rs`：delete queue、advance deletes、commit 路径
3. `src/indexer/delete_queue.rs`：删除操作的记录与游标
4. `src/fastfield/alive_bitset.rs`：alive bitset 的存储格式
5. `examples/deleting_updating_documents.rs`：删除/更新示例

## 可运行实验

```bash
cargo run --example deleting_updating_documents
```

### 验证点

- 你能解释：为什么 delete 不会立刻“物理移除”文档
- 你能回答：删除为什么需要等 commit 才对搜索可见

## TODO

- [ ] 画一张 delete 操作从记录到可见性的时序图
- [ ] FAQ：大量 delete 会如何影响查询性能？

