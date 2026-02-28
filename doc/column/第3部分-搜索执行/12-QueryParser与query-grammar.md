# P3-12 QueryParser 与 query-grammar：从字符串到 AST 再到 Query

> 本文主问题：用户输入字符串如何变成可执行 Query？语法层与执行层如何分离？

## 本文目标

- 读懂 QueryParser 的职责：解析、字段展开、默认字段、短语/范围等
- 读懂 `query-grammar` crate 的定位：把 AST/语法解析从主 crate 解耦
- 了解错误处理与用户体验取舍

## 源码入口（建议阅读顺序）

1. `src/query/query_parser/query_parser.rs`：QueryParser 入口
2. `query-grammar/src/user_input_ast.rs`：AST 定义
3. `query-grammar/src/query_grammar.rs`：语法解析
4. `src/query/query_parser/*`：AST → Query 的转换逻辑

## 可运行实验

```bash
cargo run --example basic_search
```

### 验证点

- 你能举例解释：`(a AND b) OR "c d"` 这类输入如何落到 Query 组合
- 你能解释：为什么把 grammar 拆成 workspace 子 crate（编译速度/隔离）

## TODO

- [ ] 给出 10 条“常见用户查询字符串”→“对应 Query 结构”的对照
- [ ] FAQ：QueryParser 的错误为什么可能不适合直接展示给用户？

