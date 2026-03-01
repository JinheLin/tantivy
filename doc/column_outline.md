# Tantivy 源码解析专栏（可执行大纲）

<<<<<<< HEAD
> 版本基线：本仓库 `tantivy 0.24.0`（见 `Cargo.toml`）
=======
> 版本基线：本仓库 `tantivy 0.26.0`（见 `Cargo.toml`）
>>>>>>> 68c15cbb (Codex changes)
>
> 写作目标：读者能“跑起来、看得懂、改得动”——每篇文章都要有可复现实验与源码入口。

## 使用方式（建议）

1. 先从 `doc/column/说明.md` 过一遍写作约定。
2. 按下方顺序阅读/写作：每篇文章对应一个文件（`doc/column/第*部分-*/*.md`）。
3. 每篇文章写完后，把该篇的 TODO 勾完，并在本文件的状态表里更新状态。

## 写作统一约定（强制）

- 每篇文章至少包含：
  - 1 个**可运行实验**（`cargo run --example ...` 或 `cargo test ...`）
  - 1 个**关键数据结构/trait**解释（画图或写清楚输入/输出/不变量）
  - 1 份**源码阅读入口清单**（按阅读顺序）
  - 5 个以内的 FAQ（回答读者最可能问的）
- 引用源码时优先写：`文件路径 + 关键符号名`，必要时补充行号（行号可能漂移）。
- 每篇只讲清楚一个“主线问题”，把扩展内容放在“延伸”里。

## 专栏结构与文件

### 第 1 部分｜入门与全景（4 篇）

- [ ] P1-01 从示例理解 Tantivy 的最小闭环（Index → Writer → commit → Reader/Searcher → Query → Collector）→ `doc/column/第1部分-入门与全景/01-最小闭环.md`
- [ ] P1-02 Index/Segment/Meta：不可变分段模型与 meta.json → `doc/column/第1部分-入门与全景/02-Index与Segment与meta.json.md`
- [ ] P1-03 Directory 与 mmap：I/O 抽象与“把缓存交给 OS” → `doc/column/第1部分-入门与全景/03-Directory与mmap.md`
- [ ] P1-04 Schema/Document/Term：数据模型如何决定索引结构 → `doc/column/第1部分-入门与全景/04-Schema与Document与Term.md`

### 第 2 部分｜写入管线：从文档到段文件（4 篇）

- [ ] P2-05 Tokenizer：分析链与可配置的文本处理 → `doc/column/第2部分-写入管线/05-Tokenizer分析链.md`
- [ ] P2-06 倒排总览：TermDict → TermInfo → Postings 的两级映射 → `doc/column/第2部分-写入管线/06-倒排总览-TermDict-TermInfo-Postings.md`
- [ ] P2-07 Postings/Positions/FieldNorm：为 BM25 与短语查询服务 → `doc/column/第2部分-写入管线/07-Postings-Positions-FieldNorm-短语与BM25.md`
- [ ] P2-08 DocStore vs FastField：行存/列存的取舍与正确用法 → `doc/column/第2部分-写入管线/08-DocStore与FastField.md`

### 第 3 部分｜搜索执行：从 Query 到结果（4 篇）

- [ ] P3-09 Searcher 快照一致性：SegmentReader 与 generation → `doc/column/第3部分-搜索执行/09-Searcher快照一致性.md`
- [ ] P3-10 Query/Weight/Scorer：三段式接口与扩展套路 → `doc/column/第3部分-搜索执行/10-Query-Weight-Scorer三段式.md`
- [ ] P3-11 Collector：把“匹配”与“收集/聚合”解耦 → `doc/column/第3部分-搜索执行/11-Collector设计.md`
- [ ] P3-12 QueryParser 与 query-grammar：从字符串到 AST 再到 Query → `doc/column/第3部分-搜索执行/12-QueryParser与query-grammar.md`

### 第 4 部分｜高级能力与工程化（4 篇）

- [ ] P4-13 Aggregation：类 Elasticsearch 的聚合执行与合并 → `doc/column/第4部分-高级能力与工程化/13-Aggregation聚合.md`
- [ ] P4-14 Deletes & Alive Bitset：删的是 term，看的是真相 → `doc/column/第4部分-高级能力与工程化/14-删除与AliveBitset.md`
- [ ] P4-15 MergePolicy 与后台合并：段数量、空间回收与性能 → `doc/column/第4部分-高级能力与工程化/15-MergePolicy与后台合并.md`
- [ ] P4-16 收官专题（四选一）：JSON/索引排序/Warmer/多线程写入 → `doc/column/第4部分-高级能力与工程化/16-收官专题.md`

## 进度状态表（手动维护）

| ID | 标题 | 状态 | 预计发布日期 | 备注 |
|---|---|---|---|---|
<<<<<<< HEAD
| P1-01 | 最小闭环 | DRAFT |  |  |
=======
| P1-01 | 最小闭环 | TODO |  |  |
>>>>>>> 68c15cbb (Codex changes)
| P1-02 | Index/Segment/Meta | TODO |  |  |
| P1-03 | Directory/mmap | TODO |  |  |
| P1-04 | Schema/Document/Term | TODO |  |  |
| P2-05 | Tokenizer | TODO |  |  |
| P2-06 | 倒排总览 | TODO |  |  |
| P2-07 | Postings/Positions/FieldNorm | TODO |  |  |
| P2-08 | Store/FastField | TODO |  |  |
| P3-09 | Searcher 快照 | TODO |  |  |
| P3-10 | Query/Weight/Scorer | TODO |  |  |
| P3-11 | Collector | TODO |  |  |
| P3-12 | QueryParser/Grammar | TODO |  |  |
| P4-13 | Aggregation | TODO |  |  |
| P4-14 | Deletes/AliveBitset | TODO |  |  |
| P4-15 | MergePolicy/Merge | TODO |  |  |
| P4-16 | 收官专题 | TODO |  |  |
<<<<<<< HEAD
=======

>>>>>>> 68c15cbb (Codex changes)
