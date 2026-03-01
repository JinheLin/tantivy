# 15. MergePolicy 与后台合并

> 适用版本：tantivy `d0c5ffb0`（2026-03-01）  
> 关键词：`MergePolicy` / `LogMergePolicy` / `SegmentUpdater` / `merge_thread_pool`

## TL;DR

- **MergePolicy 决定“合并什么”**：它只负责从一组 `SegmentMeta` 中快速计算出合并候选（`MergeCandidate`），真正的合并工作在后台线程完成。  
  参考：[`src/indexer/merge_policy.rs`](../../../src/indexer/merge_policy.rs)。
- **后台合并由 SegmentUpdater 协调**：段列表变化（flush 新段、commit、merge 结束）都会触发一次 `consider_merge_options()`，按策略启动合并任务。  
  参考：[`src/indexer/segment_updater.rs`](../../../src/indexer/segment_updater.rs)。
- **默认策略是 LogMergePolicy**：按段规模“分层”合并，并支持用“删除比例阈值”触发 expunge deletes（默认阈值为 `1.0`，等价于不因 deletes 触发）。  
  参考：[`src/indexer/log_merge_policy.rs`](../../../src/indexer/log_merge_policy.rs)。
- **合并是异步的、可不等待**：应用直接退出是安全的；如果你希望关停前“尽量合并完”，可以显式等待 merge 线程。  
  参考：`IndexWriter::wait_merging_threads()`（[`src/indexer/index_writer.rs`](../../../src/indexer/index_writer.rs)）。

## 背景：为什么需要合并（merge）

在 Tantivy 里，索引数据以 **segment（段）** 的形式落盘。段大体满足两个特点：

1. **段一旦写完就趋于不可变**：新文档不会“原地插入”旧段，而是进入新的段；
2. **删除是“打标记”**：删除操作通常会产生 tombstone（例如 delete bitset），而不是立刻回收空间。

这会带来几个现实问题：

- **段数量膨胀**：每次 flush/commit 都可能产生新段。段越多，查询需要打开/遍历的段读者越多，开销会上升。
- **删除无法立刻回收**：删除比例高时，磁盘空间、缓存命中、posting 扫描都可能受到影响。
- **小段很多时，系统调度成本变高**：包括文件句柄、缓存、线程切换、求交/求并等细节成本。

“合并（merge）”的作用就是：把多个段合并成一个更大的段，并在此过程中**吸收删除**、减少段数量，从而在空间与查询性能上做折中优化。

## 核心概念：MergePolicy/候选/后台线程

### MergePolicy：只做“选段”，不做“干活”

`MergePolicy` 是一个 trait，核心方法只有一个：

- `compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate>`

它的输入是段的元信息列表（`SegmentMeta`），输出是若干“建议合并”的段 ID 列表（`MergeCandidate(Vec<SegmentId>)`）。  
非常关键的一点：**这个方法运行在 SegmentUpdater 的单线程上，会阻塞其它段更新**，因此实现必须“足够快”，不能做昂贵操作（尤其是 IO）。  
参考：[`src/indexer/merge_policy.rs`](../../../src/indexer/merge_policy.rs)。

### MergeCandidate / MergeOperation

- `MergeCandidate`：策略层给出的“建议合并哪些段”的结果；
- `MergeOperation`：SegmentUpdater 把候选包装成一次具体合并任务（包含 `target_opstamp` 等信息），并用于跟踪“哪些段正在合并中”，避免重复合并。

### 后台合并线程池：merge_thread_pool

真正的合并是重 CPU/IO 的任务（读多个段、写新段、序列化多种组件），因此 Tantivy 把它放在单独的线程池里跑：

- SegmentUpdater 本身有一个单线程 `pool`（线程名 `segment_updater`），负责串行处理段状态变更；
- 另外有一个 `merge_thread_pool`（线程名 `merge_thread_{i}`），用于并行执行合并任务。

参考：`SegmentUpdater::create()`（[`src/indexer/segment_updater.rs`](../../../src/indexer/segment_updater.rs)）。

## 默认策略：LogMergePolicy（按“层级”合并 + 可选 expunge deletes）

默认的 merge policy 是 `LogMergePolicy`（`DefaultMergePolicy = LogMergePolicy`），它的直觉是：

- 先按段大小把段分到不同“层级（level）”；
- 在同一层级里，段规模接近，合并的性价比更好；
- 若某层级段数超过阈值，或某段删除比例超过阈值，就触发合并。

核心代码在：[`src/indexer/log_merge_policy.rs`](../../../src/indexer/log_merge_policy.rs)。

### 关键参数怎么理解

`LogMergePolicy` 可配置参数（只列最常用/最有影响的）：

- `min_num_segments`：一个层级里至少多少个段才会被建议合并。  
  越小：合并更频繁（写放大更高）；越大：段数可能更多（读放大更高）。
- `max_docs_before_merge`：超过该 doc 数的段不会被当作候选段（但更小段合并后仍可能超过它）。  
  用来避免“特别大的段”频繁被卷入合并。
- `min_layer_size` 与 `level_log_size`：控制“分层”的粒度。  
  可以理解为决定“哪些段算同一个量级”，以及层级边界增长的速度。
- `del_docs_ratio_before_merge`：允许的删除比例上限。若层级内任一段的 `num_deleted_docs/max_doc` 超过阈值，就触发该层级的合并。  
  这相当于用 merge 来 **expunge deletes（清理删除）**。

注意：当前默认 `del_docs_ratio_before_merge = 1.0`，等价于“不会因为 deletes 触发合并”（源码里也明确写了这是为了兼容性，未来可能会变）。如果你的业务删除很多，通常需要显式调低它，例如 `0.1` 或 `0.2`。

## 后台合并的主流程：从“段变化”到“合并完成”

这里用源码视角把合并流程串起来。建议对照阅读：

- `SegmentUpdater::schedule_add_segment()` / `schedule_commit()` / `start_merge()` / `end_merge()`  
  [`src/indexer/segment_updater.rs`](../../../src/indexer/segment_updater.rs)
- `IndexWriter::merge()` / `IndexWriter::set_merge_policy()` / `IndexWriter::wait_merging_threads()`  
  [`src/indexer/index_writer.rs`](../../../src/indexer/index_writer.rs)

### 1) 什么时候会触发 consider_merge_options()

至少包含三类时机：

1. **indexing worker flush 出新段**：`SegmentWriter::finalize()` 后，把新段作为 `SegmentEntry` 交给 `SegmentUpdater::schedule_add_segment()`，随后就会 `consider_merge_options()`。
2. **commit 完成**：`schedule_commit()` 在保存 metas、做 GC 后也会 `consider_merge_options()`。
3. **一次合并结束**：`end_merge()` 在把新段接入段列表、必要时保存 metas 后会再次 `consider_merge_options()`，因为新的段组合可能产生新的合并机会。

### 2) committed vs uncommitted：为什么要分开算

`consider_merge_options()` 会把可参与合并的段分成两组：

- **committed segments**：已经写入 `meta.json`，可被 reader/searcher 打开；
- **uncommitted segments**：已经落盘但还没被 commit 接纳（对外不可见）。

它们**不会混在一起合并**（源码注释也写明了原因），因此策略会对两组分别 `compute_merge_candidates()`。

这点在工程实践上很有用：你可以把“对外可见的读路径稳定性”与“后台写路径优化”分离开思考。

### 3) start_merge：把重活丢到 merge_thread_pool

当 SegmentUpdater 决定要合并时，会调用 `start_merge(merge_operation)`：

- 先从 `SegmentManager` 拿到要合并的 `SegmentEntry` 列表（若失败会 warn，但**不致命**）；
- 然后在 `merge_thread_pool` 里 `spawn` 一个任务执行真正的 `merge(...)`；
- merge 任务用 `catch_unwind` 包起来，防止 merge 线程 panic 直接把进程搞崩，并把错误回传。

### 4) merge(...)：生成新段（并吸收 deletes 到 target_opstamp）

`merge(...)` 做的事情可以概括为：

1. 为目标 index 创建一个新段（`index.new_segment()`）；
2. 对参与合并的每个段先 `advance_deletes(..., target_opstamp)`，把删除推进到指定的 `opstamp`；
3. 用 `IndexMerger` 把多个段合成一个“视图”，再通过 `SegmentSerializer` 写出新段；
4. 产出新段的 `SegmentMeta`，包装成新的 `SegmentEntry` 返回。

这里的关键是 `target_opstamp`：它决定了“合并时至少要吸收到哪一个时刻的删除”。  
对 committed 段来说，它通常是“commit 时的 opstamp”；对 uncommitted 段来说，是“当前写入进度的 opstamp”。

### 5) end_merge：接入新段 + 追平删除 + 可能保存 metas

merge 线程把结果回传后，SegmentUpdater 会在自己的单线程里执行 `end_merge(...)`：

- 合并期间可能又发生了 deletes/commit，因此在接纳新段前，可能需要再推进一次 deletes，确保新段 delete 信息追平到 committed 的 opstamp；
- 用 `segment_manager.end_merge(...)` 把旧段替换成新段；
- 如果这次合并发生在 committed 段集合上，还会触发一次 `save_metas(...)`，把新的段列表写入 `meta.json`；
- 最后再触发一次 `consider_merge_options()` 和一次 GC。

这保证了“对外可见的段列表更新”是**串行且原子**的（`meta.json` 用 `atomic_write` 写入）。

## API 用法：如何配置与控制合并

### 1) 设置/替换 MergePolicy

`IndexWriter` 提供了直接设置 merge policy 的接口：

```rust
use tantivy::indexer::LogMergePolicy;
use tantivy::Index;

fn configure_merge_policy(index: &Index) -> tantivy::Result<()> {
    let mut index_writer = index.writer(50_000_000)?;

    let mut policy = LogMergePolicy::default();
    policy.set_min_num_segments(4);
    policy.set_max_docs_before_merge(5_000_000);
    policy.set_del_docs_ratio_before_merge(0.2); // 20% deletes 触发合并/清理

    index_writer.set_merge_policy(Box::new(policy));
    Ok(())
}
```

如果你希望完全关闭后台合并（例如用于某些测试/实验），可以使用 `NoMergePolicy`：

```rust
use tantivy::indexer::NoMergePolicy;

index_writer.set_merge_policy(Box::new(NoMergePolicy));
```

### 2) 配置 merge 线程数（num_merge_threads）

merge 线程数来自 `IndexWriterOptions`（默认 `4`）。你可以用 `Index::writer_with_options(...)` 自定义：

```rust
use tantivy::indexer::IndexWriterOptions;

let options = IndexWriterOptions::builder()
    .num_worker_threads(4)
    .num_merge_threads(2)
    .memory_budget_per_thread(50_000_000)
    .build();
let mut index_writer = index.writer_with_options(options)?;
```

调参经验（仅供方向，不是绝对值）：

- merge 线程过少：合并 backlog 堆积，段数长期偏多，查询开销上升；
- merge 线程过多：IO/CPU 争用加剧，可能影响写入 tail latency 或系统整体吞吐。

### 3) 手动触发一次合并（IndexWriter::merge）

除了后台策略触发，你也可以显式对一组 `segment_ids` 发起合并：

```rust
let segment_ids = index.searchable_segment_ids()?;
if segment_ids.len() >= 2 {
    // 注意：这是异步的，会返回 FutureResult
    let _merge_future = index_writer.merge(&segment_ids[..]);
}
```

手动合并适合：

- 你有明确的一组段需要合并（例如离线构建结束后）；
- 或者你想在特定时刻触发一次“清理 deletes”的合并。

### 4) 等待合并结束（可选）

Tantivy 明确说明：**退出时不等待合并是安全的**，残留的临时/过期文件会在之后的垃圾回收里清掉。  
如果你希望“关停前尽量把合并跑完”，可以在关停流程里调用：

- `IndexWriter::wait_merging_threads(self) -> Result<()>`：会停止 indexing worker，并等待 merge 线程结束（注意它会消费掉 writer）。

## 工程实践：怎么选策略与参数

下面给一些更偏工程化的建议，帮助你把“写放大、读放大、延迟、资源占用”几件事一起权衡。

1. **写入侧：内存预算越小，越容易产生更多小段**  
   小段多会更频繁触发合并，导致更多后台 IO。对吞吐敏感的场景，优先保证 `memory_budget_per_thread` 足够大，减少 flush 频率。
2. **删除多的业务：显式设置 deletes 阈值**  
   默认 `del_docs_ratio_before_merge = 1.0` 不会因为 deletes 触发合并；如果删除比例高，建议从 `0.2` 或 `0.1` 试起（同时观察 merge IO 与查询收益）。
3. **低延迟写入：控制 merge 并发与单次 merge 规模**  
   通过 `num_merge_threads` 控制并发，通过 `max_docs_before_merge` 避免大段被频繁卷入，从而减少突发 IO 峰值。
4. **离线构建/批量导入：可以更激进**  
   批量导入更关注最终形态而不是在线 tail latency，可以适当提高 merge 并发或调低 `min_num_segments`，最后在收尾阶段等待合并完成。

## 常见问题

### Q1：为什么 delete 之后磁盘空间不马上下降？

因为 delete 通常只是写 tombstone（例如 delete bitset / tombstone file）。空间真正回收往往要靠合并把“活文档”重写到新段里，从而丢弃被删文档的存储。

### Q2：为什么看不到合并发生？

常见原因：

- 段数太少（只有 1 个段且没有删除时，SegmentUpdater 会直接跳过合并机会）；
- 段太大被 `max_docs_before_merge` 过滤掉；
- deletes 比例阈值没配置（默认 `1.0` 等于不触发）；
- 合并是异步的，你需要通过日志或等待方法确认它是否完成。

### Q3：实现自定义 MergePolicy 时有哪些坑？

- `compute_merge_candidates()` 跑在 SegmentUpdater 单线程上，应避免任何可能阻塞的事情（IO、复杂计算、大量分配）。
- 输出候选时最好考虑“避免过大的合并”，否则会造成写放大、长尾 merge 任务，影响在线系统资源。

## 小结

- `MergePolicy` 决定合并候选，默认是 `LogMergePolicy`；真正合并在后台 `merge_thread_pool` 执行。
- `SegmentUpdater` 串行协调段状态变化，并在合并完成时原子更新 `meta.json`，保证对外可见状态一致。
- 删除的空间回收与查询收益，很大程度依赖合并；删除多的业务通常需要显式配置 deletes 阈值。
- 不等待合并退出是安全的；需要更“整洁”的关停时，再使用 `wait_merging_threads()`。

## 延伸阅读

- `MergePolicy` trait：[`src/indexer/merge_policy.rs`](../../../src/indexer/merge_policy.rs)
- 默认策略 `LogMergePolicy`：[`src/indexer/log_merge_policy.rs`](../../../src/indexer/log_merge_policy.rs)
- 后台合并协调者 `SegmentUpdater`：[`src/indexer/segment_updater.rs`](../../../src/indexer/segment_updater.rs)
- `IndexWriter` 合并相关 API：[`src/indexer/index_writer.rs`](../../../src/indexer/index_writer.rs)
- 创建 writer 的 options：[`src/index/index.rs`](../../../src/index/index.rs)

