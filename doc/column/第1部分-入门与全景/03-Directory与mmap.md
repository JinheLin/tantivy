# P1-03 Directory 与 mmap：I/O 抽象与“把缓存交给 OS”

> 本文主问题：Directory trait 抽象了什么？为什么 Tantivy 强依赖 mmap？

## 本文目标

- 读懂 `Directory` trait 的能力边界（为什么不支持“就地修改文件”）
- 对比 `MmapDirectory` 与 `RamDirectory` 的实现思路与使用场景
- 理解：打开 index 为何几乎是 O(1)（主要是 mmap）

## 源码入口（建议阅读顺序）

1. `ARCHITECTURE.md` 的 directory 小节
2. `src/directory/directory.rs`：Directory trait、读写接口、抽象约束
3. `src/directory/mmap_directory.rs`：mmap 的落地实现
4. `src/directory/ram_directory.rs`：内存目录实现（便于理解接口语义）
5. `doc/src/basis.md`：Straight from disk 的理念描述

## 可运行实验

```bash
cargo test --tests --lib
```

### 验证点

- 你能解释：为什么 Tantivy 的数据结构倾向于“序列化后只读”
- 你能回答：自定义 Directory 需要满足什么隐含假设（尤其是随机读 + mmap 语义）

## TODO

- [ ] 补一张“Directory 与组件读写”的交互图
- [ ] FAQ：为什么目录抽象不是 `Read/Write` 流？

