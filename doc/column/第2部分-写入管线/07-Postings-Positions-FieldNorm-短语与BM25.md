# P2-07 Postings/Positions/FieldNorm：为 BM25 与短语查询服务

> 本文主问题：postings 为什么按 block 组织？positions/fieldnorm 在搜索时如何被用到？

## 本文目标

- 理解 postings block（例如 128 文档一块）与压缩思路
- 理解 positions 文件如何支撑 phrase query
- 理解 fieldnorm 如何参与 BM25（长度归一化）

## 源码入口（建议阅读顺序）

1. `src/postings/postings.rs` / `src/postings/serializer.rs`：postings 写入与读取轮廓
2. `src/postings/compression/*`：压缩与编码细节
3. `src/positions/*`：positions 相关 reader/writer
4. `src/fieldnorm/*`：fieldnorm 的存储与读取
5. `src/query/bm25.rs`：BM25 计算与统计量

## 可运行实验

```bash
cargo run --example phrase_prefix_search
```

### 验证点

- 你能回答：没有 positions 的 field 是否支持 phrase query？为什么？
- 你能把 fieldnorm 和 BM25 公式中的“文档长度”对应起来

## TODO

- [ ] 画一张 postings/positions/fieldnorm 的“文件组件图”
- [ ] FAQ：为什么 postings 迭代要求 docid 有序？

