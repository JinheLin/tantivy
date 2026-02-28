# P2-05 Tokenizer：分析链与可配置的文本处理

> 本文主问题：Tokenizer/TokenFilter 这一套抽象，如何影响召回、精度与索引结构？

## 本文目标

- 读懂：Tokenizer 是什么、TokenFilter 是什么、如何组合成 pipeline
- 跑通：自定义 tokenizer 的例子，并观察 token 流变化
- 理解：tokenization 发生在写入侧哪些位置

## 源码入口（建议阅读顺序）

1. `examples/custom_tokenizer.rs`
2. `src/tokenizer/tokenizer.rs`：Tokenizer/TokenStream/TokenFilter 的核心接口
3. `src/tokenizer/tokenizer_manager.rs`：TokenizerManager 注册与获取
4. `src/tokenizer/*`：内置 tokenizer/filter（lowercase、stopword、stemmer…）
5. `tokenizer-api/src/lib.rs`：对外 tokenizer API（第三方 tokenizer 扩展点）

## 可运行实验

```bash
cargo run --example custom_tokenizer
```

### 验证点

- 你能列出 pipeline 每一步对 token 的变换（如 lowercasing、stemming）
- 你能解释：为什么 tokenizer 配置属于 Schema 的一部分

## TODO

- [ ] 补一个“token 流前后对比”的表格
- [ ] FAQ：中文分词为什么需要第三方 crate？

