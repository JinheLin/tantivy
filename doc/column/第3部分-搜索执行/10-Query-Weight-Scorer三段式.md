# P3-10 Query/Weight/Scorer：三段式接口与扩展套路

> 本文主问题：为什么 Query 不直接返回 DocSet？Weight/Scorer 的分层解决了什么问题？

## 本文目标

- 建立 Query → Weight → Scorer 的心智模型（“配方 → 绑定 searcher → 绑定 segment”）
- 读懂一个典型 Query（如 TermQuery/BooleanQuery）的实现结构
- 给出自定义 Query 的最小实现骨架

## 源码入口（建议阅读顺序）

1. `src/query/query.rs`：Query trait 文档与默认实现
2. `src/query/weight.rs` / `src/query/scorer.rs`：Weight/Scorer 接口
3. `src/query/term_query/*`：TermQuery 的实现（推荐作为第一个样例）
4. `src/query/boolean_query/*`：组合查询
5. `src/query/bm25.rs`：打分与统计

## 可运行实验

```bash
cargo run --example fuzzy_search
```

### 验证点

- 你能解释：为什么 Weight 需要绑到 Searcher（统计量/字段信息）
- 你能解释：为什么 Scorer 需要绑到 SegmentReader（局部 docid 空间）

## TODO

- [ ] 写一个“自定义 Query 骨架”小节（只到结构，不展开优化）
- [ ] FAQ：EnableScoring 关闭后哪些路径会变快？

