# P2-08 DocStore vs FastField：行存/列存的取舍与正确用法

> 本文主问题：为什么展示结果用 store，而排序/聚合用 fast field？

## 本文目标

- 读懂 docstore 的读取成本来自哪里（定位 block + 解压）
- 读懂 fast field 的 O(1) 随机访问为何成立（bitpacking + min_value）
- 给出“正确用法”的经验法则（避免滥用 store）

## 源码入口（建议阅读顺序）

1. `ARCHITECTURE.md` 的 store/fastfield 小节
2. `src/store/reader.rs` / `src/store/writer.rs`：store 的块结构与压缩
3. `src/store/compressors.rs`：lz4/zstd/none 的选择
4. `src/fastfield/writer.rs` / `src/fastfield/readers.rs`：fast field 写读
5. `src/fastfield/facet_reader.rs`：facet 作为 fast field 的特例

## 可运行实验

```bash
cargo run --example faceted_search
```

### 验证点

- 你能解释：为什么“每次 query 命中上千 doc 就去读 store”会很慢
- 你能描述：fast field 读取的关键计算步骤是什么

## TODO

- [ ] 做一张“store vs fastfield”的对比表（访问模式、压缩、典型用途）
- [ ] FAQ：什么时候该把字段同时设为 STORED + FAST？

