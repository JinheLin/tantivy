# P1-04 Schema/Document/Term：数据模型如何决定索引结构

> 本文主问题：Schema 的字段选项（TEXT/STORED/FAST）如何决定落到哪些结构里？

## 本文目标

- 读懂 Schema 如何描述字段类型与索引选项
- 理解 Document 在 Tantivy 内部的表示（以及为何有 trait 化）
- 把 “Term = (field, bytes)” 与倒排索引连接起来

## 源码入口（建议阅读顺序）

1. `src/schema/schema.rs`：Schema/SchemaBuilder/FieldEntry
2. `src/schema/field_type.rs`：字段类型系统与解析
3. `src/schema/document/mod.rs`：Document trait、序列化/反序列化约束
4. `src/schema/document/default_document.rs`：默认 `TantivyDocument`（CompactDoc）
5. `src/schema/term.rs`：Term 的编码方式与常用构造

## 可运行实验

```bash
cargo run --example index_with_json
```

### 验证点

- 你能说清：一个字段被标记为 STORED/TEXT/FAST 分别意味着什么
- 你能解释：为什么 Tantivy 不支持“同一输入字段用不同 tokenizer 索引两次”（至少在当前模型下）

## TODO

- [ ] 补一张“字段选项 → 数据结构”的对照表
- [ ] FAQ：`TantivyDocument` 与自定义 Document 的取舍？

