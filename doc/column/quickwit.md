## Task 1

### User

quickwit 的大部分数据都保存在 S3 这种远程存储上。但是存储层用了 tantivy，而 tantivy 底层用了 mmap，这是如何处理的？

### Assistant (Commentary)

我先在代码里确认 Quickwit 是怎么把远端对象存储和 Tantivy 的目录层接起来的，重点看 `Directory` 实现、split 下载路径，以及是否有本地缓存/热缓存来适配 `mmap`。

### Assistant (Commentary)

我已经定位到关键路径了：搜索侧并不是直接对 S3 做 `mmap`，而是绕过 `MmapDirectory`，用 Quickwit 自己实现的 `tantivy::Directory` 去做按需 range read，再叠加 hotcache 和可选 split cache。接下来我把这条链路和索引侧的 `mmap` 用法分别核对一下。

### Assistant (Commentary)

搜索侧链路已经确认：`open_index_with_caches` 最终把远端 split 包装成 `StorageDirectory -> CachingDirectory -> HotDirectory`，没有走 `MmapDirectory`。我再补齐写入侧和本地 split cache 里 `mmap` 的使用场景，这样能把“为什么能兼容”讲完整。

### Assistant (Commentary)

还有一个关键点我在核对：`StorageDirectory` 明确只支持异步读，同步读会报错。这意味着真正执行 Tantivy 查询前，Quickwit 必须先把会被同步访问的片段预热到缓存里，否则根本跑不起来。我要把这部分也确认出来。

### Assistant (Commentary)

关键机制确认了：Quickwit 先异步 `warmup` 把查询必需的 postings/term dict/fast field 等字节区间拉到缓存，再把真正的 Tantivy CPU 查询放到线程池里跑。这样运行期的同步读取只会命中 `HotDirectory`/`CachingDirectory`，不会真的去同步访问 S3。

### Assistant (Final Answer)

不是“把 S3 做成 `mmap`”，而是 Quickwit 在搜索侧根本不走 `MmapDirectory`。更准确地说，Tantivy 依赖的是 `Directory` 抽象，`mmap` 只是本地文件系统上的一种实现；Quickwit 对远端对象存储实现了另一套 `Directory` 适配层。

一句话总结：Quickwit 的做法是 `对象存储 range read + split bundle + hotcache + warmup + byte-range cache`，把 Tantivy 查询期会发生的同步随机读，提前转成异步预取和缓存；真正的 `mmap` 只保留在本地磁盘路径上。

**怎么接起来的**

- Quickwit 先把一个 split 打成单个 `.split` 文件。这个文件前面是 Tantivy 的各个索引文件拼接，尾部是 bundle metadata 和 `hotcache`。[split-format.md:5](/DATA/disk1/jinhelin/quickwit/docs/internals/split-format.md#L5) [split-format.md:19](/DATA/disk1/jinhelin/quickwit/docs/internals/split-format.md#L19)
- 搜索时，searcher 先只从远端拿 footer 和 hotcache，然后用 `BundleStorage` 把 `.split` 重新映射成“多个逻辑文件对应多个字节区间”。[leaf.rs:118](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L118) [leaf.rs:155](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L155) [bundle_storage.rs:71](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/bundle_storage.rs#L71) [bundle_storage.rs:224](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/bundle_storage.rs#L224)
- 然后 Quickwit 用自己的 `StorageDirectory` 暴露给 Tantivy。这个目录实现只支持异步读取，底层实际做的是对象存储的 `get_slice`，不是 `mmap`。[storage_directory.rs:69](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L69) [storage_directory.rs:119](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L119) [leaf.rs:209](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L209)
- `HotDirectory` 再套在最外层，把预先计算好的 hotcache 放到前面，优先满足“打开索引时大量小随机读”的那部分访问。[hot_directory.rs:341](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/hot_directory.rs#L341) [hot_directory.rs:428](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/hot_directory.rs#L428)

**为什么 Tantivy 还能正常跑**

- 关键点是：`StorageDirectory` 的同步读是禁用的，所以 Quickwit 必须在真正执行 Tantivy 搜索前先 `warmup`。代码里也直接写了，Tantivy 搜索阶段不能异步取数据，因此要先把 postings、term dict、fast fields、fieldnorms 等需要的字节范围异步拉进缓存。[storage_directory.rs:52](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L52) [leaf.rs:256](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L256) [leaf.rs:268](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L268) [leaf.rs:581](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L581)
- warmup 完成后，真正的 Tantivy CPU 查询才开始跑。这时同步读通常都命中 `HotDirectory` 或短生命周期 `ByteRangeCache`，因此不会在查询线程里同步打 S3。[leaf.rs:530](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L530) [leaf.rs:607](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L607)

**`hotcache` 是怎么来的**

- 在索引侧，Quickwit 对本地 split 目录确实会用 `MmapDirectory`，然后调用 `write_hotcache` 去观察“打开一个索引到底读了哪些字节”，再把这些热点切片序列化成 hotcache，跟 split 一起上传。[packager.rs:210](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/actors/packager.rs#L210) [packager.rs:312](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/actors/packager.rs#L312) [hot_directory.rs:472](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/hot_directory.rs#L472)
- 这就是为什么文档里说“靠 hotcache 可以在高延迟存储上很快打开 split”。[architecture.md:40](/DATA/disk1/jinhelin/quickwit/docs/overview/architecture.md#L40) [architecture.md:99](/DATA/disk1/jinhelin/quickwit/docs/overview/architecture.md#L99)

**本地磁盘场景下的 `mmap`**

- 在索引/merge 路径上，如果 split 已经在本地磁盘，Quickwit 还是会直接用 `MmapDirectory`。例如 indexer 的本地 split cache 命中时，merge 打开 split 就是 mmap。[indexing_split_store.rs:179](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/split_store/indexing_split_store.rs#L179)
- 搜索侧的可选 `searcher split cache` 虽然会把整个 `.split` 下载到本地磁盘，但它依然主要是“本地文件 range read 缓存”，不是 `mmap`；读取是通过 `read_exact_at` 做范围读。[searcher-split-cache.md:4](/DATA/disk1/jinhelin/quickwit/docs/internals/searcher-split-cache.md#L4) [split_cache/mod.rs:128](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/split_cache/mod.rs#L128) [download_task.rs:26](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/split_cache/download_task.rs#L26) [file_descriptor_cache.rs:156](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/file_descriptor_cache.rs#L156)

所以最终答案是：Quickwit 不是解决“远端 mmap”这个问题，而是绕开它。远端搜索走的是自定义 `Directory` + 异步预热 + 缓存；`mmap` 只在本地文件可用时使用。


## Task 2

### User

搜索侧还会使用 mmap 吗？

### Assistant (Commentary)

我再做一个精确确认：只看搜索侧 crate 和 searcher 相关路径，检查有没有直接引用 `MmapDirectory`，以及本地 split cache 命中后是否会切到 mmap。

### Assistant (Final Answer)

正常的 searcher 查询路径里，基本不使用 `mmap`。

搜索侧打开 split 的链路是 `BundleStorage -> StorageDirectory -> CachingDirectory -> HotDirectory`，也就是把 `.split` 当成可 range read 的远端或本地对象来读，而不是 `MmapDirectory`。[leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L168) [leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L231) `StorageDirectory` 还明确只支持 async read，同步读会报 unsupported operation，这和 mmap 路线是两套思路。[storage_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L50) [storage_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L119)

即便启用了 searcher split cache，把整个 `.split` 下载到本地磁盘后，搜索侧也不是改成 mmap；它只是把本地文件当作一个 range-readable cache，用 `read_exact_at` 做区间读取。[split_cache/mod.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/split_cache/mod.rs#L128) [file_descriptor_cache.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/file_descriptor_cache.rs#L156)

真正还会用 `mmap` 的，是索引/merge 这些本地磁盘路径，不是 searcher 查询路径。比如 packager 生成 hotcache 时会用 `MmapDirectory`，indexing split cache 命中后做 merge 也会用 `MmapDirectory`。[packager.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/actors/packager.rs#L210) [indexing_split_store.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/split_store/indexing_split_store.rs#L193)

如果你关心的是“搜索请求执行时会不会因为本地 split cache 命中而转成 mmap”，答案也是不会。


## Task 3

### User

tantivy 默认是使用 mmap 的，这套不适应 mmap 的存储层读取逻辑是如何实现的？

### Assistant (Final Answer)

核心点先说清楚：Tantivy 常见的默认目录实现是 `MmapDirectory`，但它真正依赖的不是 `mmap`，而是 `Directory -> FileHandle -> FileSlice` 这套抽象。Quickwit 不是去“让 S3 支持 mmap”，而是直接换掉这层实现，把对象存储包装成一个能按字节区间读取的 `Directory`。

大致链路是这样：

```text
S3 上的 xxx.split
-> 读取 footer / hotcache
-> BundleStorage: 把一个 .split 重新映射成多个 Tantivy 逻辑文件
-> StorageDirectory: 实现 tantivy::Directory，底层是 async range read
-> CachingDirectory: 动态 byte-range cache
-> HotDirectory: 静态 hotcache
-> tantivy::Index::open(...)
```

关键实现分 4 步。

1. 把远端 split 变成“逻辑文件系统”

Quickwit 先把一个 split 打包成单个 `.split` 文件，尾部带两块元数据：一块记录每个 Tantivy 文件在 `.split` 里的字节范围，另一块是 `hotcache`。[split-format.md](/DATA/disk1/jinhelin/quickwit/docs/internals/split-format.md#L5) [split-format.md](/DATA/disk1/jinhelin/quickwit/docs/internals/split-format.md#L19)

搜索时，Quickwit 先拿到 footer，然后 `BundleStorage` 根据 footer 里的 offsets，把对 `foo.term`、`bar.idx` 这种逻辑文件的读取，翻译成对 `xxx.split` 某个字节区间的读取。[bundle_storage.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/bundle_storage.rs#L71) [bundle_storage.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/bundle_storage.rs#L224)

2. 用自定义 `Directory` 替代 `MmapDirectory`

Quickwit 的 `StorageDirectory` 实现了 Tantivy 的 `Directory`。它返回的 `FileHandle` 不做 mmap，而是在 `read_bytes_async` 里直接调用底层存储的 `get_slice(path, range)`。[storage_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L50) [storage_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L89) [storage_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L119)

这一步很重要：Tantivy 看到的仍然是一个标准 `Directory`，所以它的 reader、term dict、postings、fast field reader 都还能照常工作，只是底层 byte source 从 mmap 变成了 range read。

3. 用缓存把“远端随机读”变成“本地命中”

Quickwit 不直接让查询线程同步打 S3，因为那样又慢又危险。它前面叠了两层缓存。

`HotDirectory`
- 这是静态缓存。
- 索引打包时，Quickwit 会在本地用 `MmapDirectory` 打开 split，观察“打开一个索引到底读了哪些字节”，然后把这些热点切片写成 `hotcache`。[packager.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/actors/packager.rs#L210) [packager.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/actors/packager.rs#L312) [hot_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/hot_directory.rs#L472)
- 搜索时，`HotDirectory` 的 `read_bytes` 先查静态 cache，命中就直接返回内存里的 bytes。[hot_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/hot_directory.rs#L383) [hot_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/hot_directory.rs#L428)

`CachingDirectory`
- 这是动态 byte-range cache。
- warmup 阶段异步读下来的 range 会放进这个 cache，后续真正执行查询时，同样的同步读就直接命中缓存。[caching_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/caching_directory.rs#L73) [caching_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/caching_directory.rs#L84)

此外还有两层更粗粒度的缓存：
- fast field 长期缓存，包在 `BundleStorage` 外面。[leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L226)
- 可选的 searcher split cache，把整个 `.split` 下载到本地磁盘，减少对象存储 GET；但它也不是 mmap，而是本地文件 `read_exact_at` 的 range read。[split_cache/mod.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/split_cache/mod.rs#L128) [file_descriptor_cache.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/file_descriptor_cache.rs#L156)

4. 先异步 warmup，再跑 Tantivy CPU 查询

这是这套方案能成立的关键。

`StorageDirectory` 明确不支持同步读取；同步读 miss 会失败。[storage_directory.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-directories/src/storage_directory.rs#L52)

所以 Quickwit 在真正执行查询前，会先根据 query 和 collector 计算 `warmup_info`，异步把 term dict、postings、fast fields、fieldnorms 等需要的字节区间拉到缓存里。[leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L256) [leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L268) [leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L332) [leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L372)

等 warmup 完成之后，才把真正的 Tantivy 搜索丢到 CPU 线程池里跑。[leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L581) [leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L607)

所以本质上它不是“远端 mmap”，而是：

- 用 `Directory` 抽象接入远端存储
- 用 split footer 把一个远端对象虚拟成多个 Tantivy 文件
- 用 hotcache 和 warmup 把查询所需随机读提前搬到本地内存
- 让真正的同步搜索阶段只消费缓存里的 bytes

换句话说，Quickwit 是把 Tantivy 从“依赖 OS page cache 的 mmap 模式”，改成了“依赖自己管理的 byte-range cache 模式”。如果你愿意，我可以下一条把这套流程再画成“打开一个 split 到执行一次 query”的时序图。


## Task 4

### User

所以是将  tantivy 的 MmapDirectory 替换为 BundleStorage 还是 StorageDirectory？

### Assistant (Final Answer)

不是 `BundleStorage`，而是 `StorageDirectory`。更准确一点，最终替代 `MmapDirectory` 传给 Tantivy 的，其实是外面再包了一层缓存的 `HotDirectory`。

对应关系是：

- `MmapDirectory` 是 `tantivy::Directory` 的一个实现
- `StorageDirectory` 也是 `tantivy::Directory` 的一个实现
- `BundleStorage` 不是 `Directory`，它是 Quickwit 自己的 `Storage` 实现

所以如果你问“和 `MmapDirectory` 同一抽象层、可直接替换它的是谁”，答案是 `StorageDirectory`，或者更完整地说是：

```text
HotDirectory(
  CachingDirectory(
    StorageDirectory(
      BundleStorage(...)
    )
  )
)
```

代码上也很直接：

- `BundleStorage` 先从 `.split` 里解析 footer，把逻辑文件映射到字节区间。[bundle_storage.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-storage/src/bundle_storage.rs#L71)
- 然后 `StorageDirectory::new(bundle_storage_with_cache)` 把这个 `Storage` 适配成 Tantivy 的 `Directory`。[leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L226) [leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L231)
- 最后 Tantivy 实际 `Index::open(...)` 的是 `HotDirectory`，不是裸的 `StorageDirectory`。[leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L233) [leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L240)

所以可以这么理解：

- `BundleStorage` 负责“把一个 `.split` 文件伪装成多个 Tantivy 文件”
- `StorageDirectory` 负责“把 Quickwit 的存储层伪装成 Tantivy 的 Directory”
- `HotDirectory/CachingDirectory` 负责“把查询期需要的同步读变成缓存命中”

如果只保留一句话：  
`MmapDirectory` 在搜索侧主要是被 `StorageDirectory` 这条 `Directory` 链替换了，`BundleStorage` 是它下面的一层。


## Task 5

### User

一个 split 就是一个 tantivy 实例的全部文件吗？

### Assistant (Commentary)

我回答得精确一点：split 不是 Tantivy 的一个 segment，而更接近“一个完整但较小的 Tantivy index”。我用代码把这个对应关系再核一下。

### Assistant (Final Answer)

可以近似理解为“是”，但要加两个限定：

1. `split` 不是整个 Quickwit index 的全部文件，只是其中一个独立可查询的小子索引。文档里也写了，split 是 “a small piece of an index”。[architecture.md](/DATA/disk1/jinhelin/quickwit/docs/overview/architecture.md#L38)
2. `split` 也不是 Tantivy 的一个 `segment`。一个 split 内部可以包含多个 Tantivy segment 文件。

更准确的说法是：

- 一个 split 对应一个“可独立 `Index::open(...)` 的小 Tantivy index”。搜索侧打开一个 split 时，最后确实是对这个 split 构造一个 Tantivy `Index`。[leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L209) [leaf.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-search/src/leaf.rs#L240)
- 打包 split 时，Quickwit 会把这个小 index 的 `meta.json` 和所有 segment 相关文件都收进去。[packager.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/actors/packager.rs#L188)
- 另外，`.split` 里不只有 Tantivy 文件，还会额外带 Quickwit 自己的字段元数据、footer 和 hotcache。[split-format.md](/DATA/disk1/jinhelin/quickwit/docs/internals/split-format.md#L5)

所以如果你说：

- “一个 split 是不是一个小 Tantivy index 的全套文件？”  
  基本是的。
- “一个 split 是不是整个 Quickwit index 的全套文件？”  
  不是，整个 Quickwit index 是很多 split 的集合。
- “一个 split 是不是一个 Tantivy segment？”  
  不是，split 的粒度比 segment 更大。


## Task 6

### User

一个索引可能很大，那一个 split 文件/对象岂不是可能很大？

### Assistant (Commentary)

我查一下 Quickwit 对 split 大小的控制点，重点看索引配置里的 split target size、merge 策略和 mature split 的约束，这样能回答“会不会很大”以及“为什么可控”。

### Assistant (Commentary)

我把默认目标值和 merge 策略的语义打开看一下，免得只说“受控制”但没有具体量级。

### Assistant (Commentary)

控制点已经确认了：默认不是按字节切 split，而是按文档数 `split_num_docs_target`，默认 1000 万 docs。也就是说 split 的“字节大小”没有硬性统一上限，会随着单文档大小、字段、压缩率变化。

### Assistant (Final Answer)

对，`split` 文件/对象确实可能很大。

但这里要分两层看：

1. 整个索引可以很大，是因为它由很多 split 组成。
2. 单个 split 也可能不小，但 Quickwit 会尽量把它控制在一个“目标规模”附近，而不是无限长大。

Quickwit 默认主要按“文档数”控制 split，而不是按“字节数”控制。配置项是 `split_num_docs_target`，默认值是 `10_000_000`。[index_config/mod.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-config/src/index_config/mod.rs#L112) [index_config/mod.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-config/src/index_config/mod.rs#L142) indexer 在 workbench 里的文档数达到这个目标时就会触发 commit，生成一个 split。[indexer.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/actors/indexer.rs#L607)

所以结论是：

- 单个 split 的“doc 数量”通常被控制在目标值附近。
- 但单个 split 的“对象大小（bytes）”并没有一个像 `500MB` 这样的固定硬上限。
- 如果文档很大、stored fields 多、fast fields 多、压缩效果差，那一个 split 就可能很大。
- 反过来，如果文档小、压缩好，一个 1000 万 docs 的 split 也未必特别夸张。

merge policy 也会尽量把 mature split 控制在 `split_num_docs_target` 附近。代码里的注释就是这么写的：成熟 split 会落在接近 `split_num_docs_target` 的范围，达到这个目标后就不再继续 merge。[index_config/mod.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-config/src/index_config/mod.rs#L112) [stable_log_merge_policy.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-indexing/src/merge_policy/stable_log_merge_policy.rs#L115)

不过有一个很重要的补充：  
“split 很大”不等于“每次查询都要把整个大对象读完”。

搜索侧通常只会：

- 先读 split 的 footer/hotcache 来打开索引
- 再按需 range read 查询需要的 postings、term dict、fast field 等字节区间
- 只有启用了 searcher split cache 时，才可能把整个 `.split` 下载到本地磁盘缓存

所以大 split 的主要影响是：

- 对象本身更大
- 如果启用了整 split 本地缓存，占用更多磁盘
- 某些查询 warmup 可能读更多数据

但正常查询路径并不是“每次全量下载整个 split”。

另外，Quickwit 的 metastore 里也确实有 split 的字节大小统计字段 `split_size_bytes`，说明系统本身也把 split 大小当作一个重要指标来跟踪。[metastore.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-metastore/src/metastore/postgres/metastore.rs#L919)

如果你愿意，我下一条可以继续回答一个更具体的问题：  
“在默认配置下，一个 split 从工程上为什么敢做到这么大，还不至于把搜索拖垮？”


## Task 7

### User

理论上 split 应该越大，整个系统的读性能应该越好？

### Assistant (Final Answer)

不是单调关系。更准确地说，`split` 变大通常会降低“每个 split 的固定开销”，但不会让“整个系统的读性能”无限变好；超过某个点以后，收益会变小，甚至开始变差。

如果把场景理想化成“查询几乎总是扫全量数据”，那更大的 split 往往更好一些，因为会有更少的 split 需要打开、调度、缓存和管理。Quickwit 的 merge policy 也确实是在朝这个方向做，目标是把 mature split 做到接近 `split_num_docs_target`，默认是 `10_000_000` docs。[mod.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-config/src/index_config/mod.rs#L112) [mod.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-config/src/index_config/mod.rs#L142)

但真实系统里，split 过大很快会碰到几个反作用：

- 剪枝粒度会变差。Quickwit 会用 split 的时间范围等元数据先剪枝；split 越大，一个查询越容易“勉强命中”这个 split，于是你不得不读更多本来可以跳过的数据。[architecture.md](/DATA/disk1/jinhelin/quickwit/docs/overview/architecture.md#L42)
- 并行度和负载均衡会变差。Quickwit 是按 split 分发搜索任务的；split 太少、太大时，更容易出现某几个大 split 成为长尾，节点之间也更难均衡。[architecture.md](/DATA/disk1/jinhelin/quickwit/docs/overview/architecture.md#L87)
- cache 成本会变高。searcher split cache 缓存的是整个 split 文件；split 越大，下载越慢、占盘越多、驱逐也越粗暴。[searcher-split-cache.md](/DATA/disk1/jinhelin/quickwit/docs/internals/searcher-split-cache.md#L8)
- 查询延迟不一定更好。Quickwit 搜索不是整文件顺序读，而是先开 split，再按需做 range read。split 变大带来的主要收益是减少 per-split 固定开销，不是把 IO 变成“越大的顺序读越快”。所以当固定开销被摊薄后，再继续放大 split，收益就有限了。
- 写入和 merge 代价会上升。更大的 split 意味着更重的 merge、更大的上传对象、更慢的恢复和搬运。

所以工程上通常不是追求“split 越大越好”，而是追求“足够大，以摊薄固定开销；但又不能大到损害剪枝、并行和缓存”。Quickwit 默认用 `split_num_docs_target + merge policy` 就是在找这个平衡点。[mod.rs](/DATA/disk1/jinhelin/quickwit/quickwit/quickwit-config/src/index_config/mod.rs#L112)

可以粗略这么理解：

- 查询大多是长时间范围、低选择性扫描或聚合：可以偏向更大的 split。
- 查询大多是短时间范围、高选择性过滤、追求低 p95/p99：通常更小一些的 split 会更合适。

如果你愿意，我可以继续把这个问题再落到一个更具体的层面：  
“对 Quickwit 这种从对象存储按 range read 搜索的系统，split 大小到底主要影响哪几类成本，哪个通常先成为瓶颈。”


## Task 8

### User

$export-codex-session-markdown

### Assistant (Commentary)

使用 `export-codex-session-markdown` skill，按默认行为导出当前会话正文到当前目录下的 Markdown，并校验生成结果。
