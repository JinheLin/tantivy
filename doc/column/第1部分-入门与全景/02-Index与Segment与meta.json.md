# P1-02 Index/Segment/Meta：不可变分段模型与 meta.json

> 本文主问题：为什么 Tantivy 选择“不可变 segment + meta.json”来组织索引？

## 本文目标

- 读懂：segment 是什么、为什么不可变、meta.json 记录了什么
- 理解 commit 做了什么（写新段 + 原子更新元数据）
- 为后续的 delete/merge/searcher 快照铺垫概念

## 关键概念（先给结论）

- `Segment`：一批文档的不可变索引产物（包含倒排/列存/store 等组件文件）
- `meta.json`：索引的“目录索引”，记录 schema、segments 列表与设置
- `opstamp`：操作序号，用于描述 commit/delete 的推进

## 源码入口（建议阅读顺序）

1. `ARCHITECTURE.md`：Index/Segments、Deletes、Merges 小节
2. `src/index/index_meta.rs`：IndexMeta、SegmentMeta、meta.json 的读写
3. `src/index/segment.rs`：Segment 的文件组件与打开读写
4. `src/index/segment_component.rs`：不同组件文件扩展名与枚举
5. `src/indexer/index_writer.rs`：commit 如何产生 segment 并更新 meta

## 可运行实验（观察段变化）

建议在临时目录下重复 commit（用你自己的小程序/示例改造）。

```bash
# 先跑一遍示例确认环境 OK
cargo run --example basic_search
```

### 验证点

- 你能解释：为什么打开 index 很快（mmap + 直接读磁盘布局）
- 你能说清：一次 commit 后，哪些文件一定新增/变化（概念层面即可）

## TODO

- [ ] 给出“segment 文件命名规则”的图示（uuid + 扩展名）
- [ ] 补一个小实验：连续 commit 两次，段数量/元数据如何变化

