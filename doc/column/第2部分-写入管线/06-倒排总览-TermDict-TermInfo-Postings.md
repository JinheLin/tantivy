# P2-06 倒排总览：TermDict → TermInfo → Postings 的两级映射

> 本文主问题：为什么倒排索引要拆成“字典（Term → TermInfo）+ postings（TermInfo → docset）”？

## 本文目标

- 画清楚倒排索引的两级映射与查找路径
- 读懂 termdict 的实现轮廓（fst / sstable）
- 了解 TermInfo 里有哪些关键信息（offset、doc_freq 等）

## 源码入口（建议阅读顺序）

1. `ARCHITECTURE.md` 的 inverted index/termdict/postings 小节
2. `src/termdict/mod.rs`：TermDictionary 抽象与实现选择
3. `src/termdict/fst_termdict/*`：fst 词典实现（Term → ordinal）
4. `src/postings/term_info.rs`：TermInfo 数据结构
5. `src/postings/mod.rs`：postings 的组织与访问入口

## 可运行实验（推荐）

```bash
cargo run --example iterating_docs_and_positions
```

### 验证点

- 你能描述：一次 term 查询时，如何从 termdict 找到 postings
- 你能解释：term ordinal 的意义是什么

## TODO

- [ ] 画一张“term lookup 路径图”（Term → TermInfo → Postings）
- [ ] FAQ：为什么不用 HashMap 存 term → postings？

