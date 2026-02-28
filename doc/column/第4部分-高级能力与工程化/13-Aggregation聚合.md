# P4-13 Aggregation：类 Elasticsearch 的聚合执行与合并

> 本文主问题：聚合请求如何被解析、在每个 segment 收集、最后合并成结果？

## 本文目标

- 读懂聚合请求结构（req）与执行时需要的 accessor（with_accessor）
- 读懂 segment 级收集结果如何合并成最终结果
- 能跟着示例跑一遍并定位关键模块

## 源码入口（建议阅读顺序）

1. `examples/aggregation.rs`：先从用法进入
2. `src/aggregation/README.md`：模块组织说明
3. `src/aggregation/agg_req.rs`：请求结构
4. `src/aggregation/agg_req_with_accessor.rs`：绑定 fast field accessor
5. `src/aggregation/collector.rs`：Collector 侧的聚合执行
6. `src/aggregation/intermediate_agg_result.rs` / `agg_result.rs`：合并与最终输出

## 可运行实验

```bash
cargo run --example aggregation
```

### 验证点

- 你能解释：为什么聚合天然依赖 fast field（以及为何 store 不适合）
- 你能描述：segment 结果合并时需要处理哪些问题（bucket 合并、metric 合并）

## TODO

- [ ] 画一张“req → with_accessor → segment_collect → merge → result”的流程图
- [ ] FAQ：聚合在多 segment 下如何保证结果一致？

