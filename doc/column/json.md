# json 类型的写入路径、读取路径

## 总览
Tantivy 里 `json` 字段的实现核心不是“把一整块 JSON 直接写进去再直接读出来”，而是把同一份值分成 3 条路径处理：

- `store` 保留原始层次结构，供 `searcher.doc()` 取回。[src/indexer/segment_writer.rs#L356](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L356) [src/store/writer.rs#L95](/DATA/disk1/jinhelin/tantivy/src/store/writer.rs#L95)
- 倒排索引把 JSON 扁平化成 `json_path -> typed term`，供 `json.a.b:x`、phrase、term query 读取。[src/indexer/segment_writer.rs#L304](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L304) [src/core/json_utils.rs#L105](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L105)
- `fast field` 把 JSON 扁平化成 `column_name -> value`，供 `exists/range` 这类列式读取。[src/fastfield/writer.rs#L199](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L199) [src/fastfield/readers.rs#L83](/DATA/disk1/jinhelin/tantivy/src/fastfield/readers.rs#L83)

Schema 入口在 [src/schema/schema.rs#L188](/DATA/disk1/jinhelin/tantivy/src/schema/schema.rs#L188)，JSON 字段的配置项 `stored/indexing/fast/expand_dots_enabled` 在 [src/schema/json_object_options.rs#L12](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L12)。

如果用一个例子看，假设字段名是 `json`，值是 `{"a":{"b":"x"},"n":10}`：
- `store` 里保存的是整棵对象。
- 倒排里更像 `(field=json, path=a.b, str=x)` 和 `(field=json, path=n, i64=10)`。
- fast field 里更像列 `json.a.b` 和列 `json.n`。

## 写入路径
写入总入口是 `SegmentWriter::add_document`，顺序是：

```text
IndexWriter.add_document
  -> SegmentWriter.add_document
     -> FastFieldsWriter.add_document
     -> SegmentWriter.index_document
     -> StoreWriter.store
```

对应代码在 [src/indexer/segment_writer.rs#L348](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L348)。

- 如果你走 `TantivyDocument::parse_json`，它先要求“整份文档”是 top-level JSON object，再按 schema 把每个 field 转成 `OwnedValue`。[src/schema/document/default_document.rs#L198](/DATA/disk1/jinhelin/tantivy/src/schema/document/default_document.rs#L198)
- 这条 `parse_json` 路径下，`FieldType::JsonObject` 当前只接受 object 值；字符串/数字不会被当成 JSON field 值接受。[src/schema/field_type.rs#L438](/DATA/disk1/jinhelin/tantivy/src/schema/field_type.rs#L438) [src/schema/field_type.rs#L512](/DATA/disk1/jinhelin/tantivy/src/schema/field_type.rs#L512)
- 但如果你直接用 `doc!(json_field => ...)` 构造文档，JSON 字段可以写入标量根值，这个行为在测试里有覆盖。[src/indexer/segment_writer.rs#L537](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L537)

`store` 路径：
- `StoreWriter::store` 直接把 `stored` 字段序列化进 doc store。[src/store/writer.rs#L99](/DATA/disk1/jinhelin/tantivy/src/store/writer.rs#L99)
- JSON 对象/数组通过 `BinaryDocumentSerializer` / `BinaryValueSerializer` 递归写成 `OBJECT_CODE` / `ARRAY_CODE`，因此结构会被完整保留。[src/schema/document/se.rs#L29](/DATA/disk1/jinhelin/tantivy/src/schema/document/se.rs#L29) [src/schema/document/se.rs#L152](/DATA/disk1/jinhelin/tantivy/src/schema/document/se.rs#L152)

倒排路径：
- `PerFieldPostingsWriter` 对 `JsonObject` 专门选 `JsonPostingsWriter`，不是普通字符串 postings writer。[src/postings/per_field_postings_writer.rs#L55](/DATA/disk1/jinhelin/tantivy/src/postings/per_field_postings_writer.rs#L55)
- `SegmentWriter::index_document` 在 JSON 分支里调用 `index_json_value`，递归遍历 object/array，叶子值才真正落索引。[src/indexer/segment_writer.rs#L304](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L304) [src/core/json_utils.rs#L105](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L105)
- 字符串值会先分词再写 postings；数值/布尔/日期直接写 typed term。[src/core/json_utils.rs#L126](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L126) [src/core/json_utils.rs#L144](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L144)
- JSON 数组会被扁平化成“同一路径的多个值”。为了避免 phrase query 在数组元素之间串位，Tantivy 用 `IndexingPositionsPerPath` 按 path 单独维护 position offset。[src/core/json_utils.rs#L13](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L13)
- JSON term 的内部编码是 `[type=JSON][json_path][\0][inner ValueBytes]`；path 段之间用 `\x01`，path 结束用 `\x00`。[src/schema/term.rs#L341](/DATA/disk1/jinhelin/tantivy/src/schema/term.rs#L341) [common/src/json_path_writer.rs#L3](/DATA/disk1/jinhelin/tantivy/common/src/json_path_writer.rs#L3)
- `JsonPostingsWriter` 最终序列化时，会把 path 和 value 拼回完整 term；字符串 term 和非字符串 term 走不同的 postings 记录器。[src/postings/json_postings_writer.rs#L60](/DATA/disk1/jinhelin/tantivy/src/postings/json_postings_writer.rs#L60)

`fast field` 路径：
- `FastFieldsWriter.add_document` 遇到 JSON object，会先把顶层 field 名压进 path buffer，再递归写每个叶子值到 `ColumnarWriter`。[src/fastfield/writer.rs#L120](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L120) [src/fastfield/writer.rs#L199](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L199)
- 叶子值按类型落成 `record_str / record_numerical / record_bool / record_datetime`；字符串在 fast field 上也可以配置 tokenizer。[src/fastfield/writer.rs#L254](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L254)
- fast field 的 column name 和倒排 term 不同，它包含顶层 field 名；用户侧的 `json.a.b` 会先被编码成内部列名。[src/core/json_utils.rs#L317](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L317)
- `store` 是边 add 边写块；倒排和 fast field 则主要在 segment finalize 时真正序列化落盘。[src/store/writer.rs#L13](/DATA/disk1/jinhelin/tantivy/src/store/writer.rs#L13) [src/indexer/segment_writer.rs#L404](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L404) [src/indexer/segment_writer.rs#L412](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L412)

## 读取路径
读取也分两类，先区分你要“拿回原文档”，还是“按 JSON path 查询”。

- 取回原文档这条路是 `Searcher::doc -> StoreReader::get -> BinaryDocumentDeserializer`，最后反序列化成 `CompactDoc/OwnedValue`。[src/core/searcher.rs#L84](/DATA/disk1/jinhelin/tantivy/src/core/searcher.rs#L84) [src/store/reader.rs#L239](/DATA/disk1/jinhelin/tantivy/src/store/reader.rs#L239) [src/schema/document/default_document.rs#L378](/DATA/disk1/jinhelin/tantivy/src/schema/document/default_document.rs#L378)
- 然后 `to_json()` 会把 `OwnedValue::Object` 重新序列化成用户能看到的 JSON 结构。这条路读的是 `store`，不是倒排或 fast field。[src/schema/document/mod.rs#L246](/DATA/disk1/jinhelin/tantivy/src/schema/document/mod.rs#L246) [src/schema/document/owned_value.rs#L168](/DATA/disk1/jinhelin/tantivy/src/schema/document/owned_value.rs#L168)

按 JSON path 查询这条路：
- query parser 先把 `json.a.b` 拆成 `(field=json, json_path=a.b)`，支持 escaped dot；schema 侧的拆分逻辑在 [src/schema/schema.rs#L331](/DATA/disk1/jinhelin/tantivy/src/schema/schema.rs#L331)，query parser 入口在 [src/query/query_parser/query_parser.rs#L266](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L266)。
- 然后它用 `Term::from_field_json_path(...)` 构造 JSON term 前缀，再尝试把查询词解析成 `date/i64/u64/f64/bool`；解析失败才按字符串处理。[src/query/query_parser/query_parser.rs#L488](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L488) [src/core/json_utils.rs#L243](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L243)
- 最终普通 term query 会落到 `TermQuery -> SegmentReader::inverted_index -> InvertedIndexReader::read_postings`。[src/query/term_query/term_weight.rs#L113](/DATA/disk1/jinhelin/tantivy/src/query/term_query/term_weight.rs#L113) [src/index/segment_reader.rs#L222](/DATA/disk1/jinhelin/tantivy/src/index/segment_reader.rs#L222) [src/index/inverted_index_reader.rs#L265](/DATA/disk1/jinhelin/tantivy/src/index/inverted_index_reader.rs#L265)

按 JSON fast field 读取这条路：
- `FastFieldReaders` 会先把用户传入的 `json.a.b` 解析成内部列名，再从 `columnar` 打开对应列。[src/fastfield/readers.rs#L83](/DATA/disk1/jinhelin/tantivy/src/fastfield/readers.rs#L83) [src/fastfield/readers.rs#L220](/DATA/disk1/jinhelin/tantivy/src/fastfield/readers.rs#L220)
- `ExistsQuery` 直接基于这些动态列工作，`json_subpaths=true` 时还会把子路径列一起算进去。[src/query/exist_query.rs#L97](/DATA/disk1/jinhelin/tantivy/src/query/exist_query.rs#L97)
- `RangeQuery` 对 JSON 只支持 fast field，不支持纯倒排路径；具体执行在 `range_query_fastfield.rs` 里按列类型走 string ord / numeric / datetime 读取。[src/query/range_query/range_query.rs#L104](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query.rs#L104) [src/query/range_query/range_query_fastfield.rs#L79](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query_fastfield.rs#L79) [src/query/range_query/range_query_fastfield.rs#L257](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query_fastfield.rs#L257)

## 关键差异
- `store` 保留原始 JSON 结构；倒排和 fast field 都是“扁平化后的 path/value 表示”。
- 倒排 term 里的 JSON path 不带顶层 field 名，field 是单独存的；fast field 的 column name 带顶层 field 名。
- `expand_dots_enabled` 同时影响写入时的 path 生成和读取时的 path 解析。[src/schema/json_object_options.rs#L82](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L82) [common/src/json_path_writer.rs#L39](/DATA/disk1/jinhelin/tantivy/common/src/json_path_writer.rs#L39)
- 当前 `parse_json` 和直接 `doc!(...)` 构造 `Document` 的 JSON 根值能力不完全一致：前者对 JSON field 更严格，后者能写标量根值。

如果你要，我可以下一步继续把某个具体场景画成更细的调用链，比如：
1. `json.a.b:123` 是怎么从 query string 走到 posting list 的。
2. `json.a.b` 的 range query 是怎么走 columnar 的。
3. `searcher.doc()` 取回 stored JSON 时对象/数组的二进制格式。

# json 类型的 schema 设置

## 结论
Tantivy 现在的 `json` schema 配置是“按字段整体配置”，不是“按 json path 配置”。也就是说：

- 不能给不同 `json path` 指定不同 analyzer。
- 不能只把一部分 `json path` 设成 fast field。
- 不能在 schema 里声明某些 `json path` 要忽略、某些不要忽略。

这点从 `JsonObjectOptions` 本身就能看出来，它只有 4 类配置：`stored`、`indexing`、`fast`、`expand_dots_enabled`，没有 path 级别配置项。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L12)

## 能配什么
一个 JSON 字段能配的主要就是这几项：

- `stored`: 是否把原始 JSON 结构放进 doc store。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L47)
- `indexing: Option<TextFieldIndexing>`: 是否建立倒排索引；这里的 `tokenizer/index_option/fieldnorms` 只对这个 JSON 字段里的“所有字符串叶子值”统一生效，不区分 path。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L106) [segment_writer.rs](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L79)
- `fast`: 是否把这个 JSON 字段里的值做成 fast field；字符串 fast field 也只能统一用一个 tokenizer。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L123) [fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L59)
- `expand_dots_enabled`: key 里的 `.` 是当普通字符，还是当 path 分隔符。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L82)

一个典型写法大概是这样：

```rust
use tantivy::schema::{JsonObjectOptions, TextFieldIndexing, IndexRecordOption};

let json_options = JsonObjectOptions::default()
    .set_stored()
    .set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("default") // 对该 JSON 字段下所有 string leaf 生效
            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
    )
    .set_fast(Some("default"))       // 对该 JSON 字段下所有 string fast value 生效
    .set_expand_dots_enabled();

schema_builder.add_json_field("attrs", json_options);
```

如果你用简写：

- `TEXT` / `STRING` 决定字符串叶子的倒排行为
- `FAST` 决定整个 JSON 字段是否生成 fast field
- `STORED` 决定是否保留原始 JSON

比如示例里就是 `STORED | TEXT`。[json_field.rs](/DATA/disk1/jinhelin/tantivy/examples/json_field.rs#L17)

## 不同 json path 的 analyzer？
不支持。

实现上，倒排索引只会为“这个字段”选一个 analyzer，保存在 `per_field_text_analyzers[field_id]` 里，然后所有 JSON string leaf 都共用它。[segment_writer.rs](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L79) [segment_writer.rs](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L304)

fast field 也是同样逻辑：一个 JSON 字段最多一个 fast-field tokenizer，所有 string leaf 共用。[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L59)

如果你要：
- `a.b` 用 `default`
- `c.d` 用 `raw`
- `x.y` 不分词但可检索

当前做法只能是：
- 拆成多个顶层字段
- 或抽取成显式 schema field
- 或预处理 JSON，写入多个不同 JSON field

## 哪些 json path 是 fast field？
规则是动态的，不是 schema 里预先列出来的。

- 只要这个 JSON 字段开了 `FAST`，文档里实际出现的“叶子 path”都会被 materialize 成列。[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L199) [fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L254)
- 对象节点本身不会生成 value 列，只有叶子值会。
- 如果根本身是标量 JSON 值，空 path 也可以存在。
- 同一个 path 如果有 mixed types，底层可能对应多个 typed columns，不一定真的是“一个列”。[fastfield/readers.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/readers.rs#L325)

一个很直接的例子：

- `with` 没开 `FAST`，`with.hello` 才能读到 fast field。
- `without` 没开 `FAST`，`without.hello` 直接报错/不可用。[fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1053)

还有一个重要点：

- 对象路径自身不算 fast value。`ExistsQuery("json", false)` 是 0。
- 只有子路径列存在时，`ExistsQuery("json", true)` 才能命中整份 JSON。[exist_query.rs](/DATA/disk1/jinhelin/tantivy/src/query/exist_query.rs#L281)

如果你想看“索引里实际 materialize 了哪些 JSON 子路径”，可以用 `index.fields_metadata()`；它会把已经出现在倒排/columnar 里的 JSON 子路径列出来。注意 stored-only 的 JSON 可能不会出现在这里。[index.rs](/DATA/disk1/jinhelin/tantivy/src/index/index.rs#L486) [segment_reader.rs](/DATA/disk1/jinhelin/tantivy/src/index/segment_reader.rs#L301)

## 哪些 json path 会被忽略
严格说，不是“schema 配忽略”，而是运行时有几类 path/value 不会进入索引或 fast field：

- `null` 叶子值会被跳过，不会生成倒排 term，也不会生成 fast field value。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L123) [fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L267)
- key 里包含 `\0` 的 path 会被忽略。
  - 倒排侧直接跳过该 key。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L85)
  - fast field 侧在 columnar serialize 时跳过包含 `\0` 的列名。[columnar writer](/DATA/disk1/jinhelin/tantivy/columnar/src/columnar/writer/mod.rs#L290)
  - 这个行为有测试覆盖。[indexer/mod.rs](/DATA/disk1/jinhelin/tantivy/src/indexer/mod.rs#L150)
- fast field 只递归到深度 20；更深的 path 会被 fast field 截断/忽略。这是 fast field 专有的保护，不是 store / 倒排的统一限制。[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L12) [fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L262)
- 如果字段没开 `indexing`，所有 JSON path 都不能走 query parser 搜索，会报 `FieldNotIndexed`。[query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1403)
- 如果字段没开 `FAST`，所有 JSON path 都没有 fast field reader。[fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1053)

还有两类“不是忽略，但容易误解”的行为：

- `expand_dots_enabled = false` 时，带点的 key 不是被忽略，而是要转义访问，比如 `json.attr\\.age`；启用后才会把 `.` 当 path 分段。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L82) [fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1088)
- 默认 JSON 字段和显式 schema field 冲突时，query parser 会优先命中显式 schema field；默认 JSON 那边不会自动参与。这不是 path 被忽略，而是解析优先级如此。[json.md](/DATA/disk1/jinhelin/tantivy/doc/src/json.md#L89) [json_field.rs](/DATA/disk1/jinhelin/tantivy/examples/json_field.rs#L88)

# expand_dots_enabled 是什么功能？

`expand_dots_enabled` 是 JSON 字段级别的一个开关，用来决定 JSON 对象里的 key 中出现的 `.`，到底当成“普通字符”，还是当成“路径分隔符”。定义在 [json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L37)。

默认是 `false`。这时：

- `{"k8s.node.name": "hello"}` 会被当成一个单独的 key
- 查询时要转义点：`json.k8s\.node\.name:hello`
- fast field 访问也要按这个逻辑写

这在 [query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1349) 和 [fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1088) 的测试里都有体现。

开启后，也就是调用 `set_expand_dots_enabled()`，同样的 JSON：

```json
{"k8s.node.name": "hello"}
```

会被按下面这个结构处理：

```json
{"k8s": {"node": {"name": "hello"}}}
```

也就是说，`key` 里的点会被展开成 path。这样查询、聚合、fast field 都可以直接写成：

```text
json.k8s.node.name
```

相关逻辑在 [json_path_writer.rs](/DATA/disk1/jinhelin/tantivy/common/src/json_path_writer.rs#L39)、[term.rs](/DATA/disk1/jinhelin/tantivy/src/schema/term.rs#L46)、[segment_writer.rs](/DATA/disk1/jinhelin/tantivy/src/indexer/segment_writer.rs#L310)、[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L200)；对应测试在 [fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1139)。

要点是：

- 它同时影响倒排索引里的 JSON path 和 fast field 的列名。
- 它不改变 `store` 里保存的原始 JSON，只影响“索引时如何解释 path”。
- 它是“整个 JSON 字段统一生效”，不是按某个 path 单独配置。
- 开启后会引入歧义：`{"a.b":1}` 和 `{"a":{"b":1}}` 会落到同一路径，无法区分。这也是代码注释里明确提到的 tradeoff。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L84)

如果你想，我可以接着给你画一个“同一份 JSON 在 `expand_dots=false/true` 下，term 和 fast field 名字分别长什么样”的对照表。


`expand_dots_enabled` 可以理解成“把 JSON key 里的 `.` 当不当成路径分隔符”。

**效果**
用这个 JSON 做例子：

```json
{
  "a.b": {
    "c.d": "hello"
  }
}
```

如果 `expand_dots_enabled = false`：

- `"a.b"` 是一个完整 key，不会拆开
- `"c.d"` 也是一个完整 key
- 这条值的逻辑路径是 `["a.b", "c.d"]`
- 查询要写成：`json.a\.b.c\.d:hello`
- fast field 访问也要按这个语义写

如果 `expand_dots_enabled = true`：

- `"a.b"` 会被当成 `["a", "b"]`
- `"c.d"` 会被当成 `["c", "d"]`
- 这条值的逻辑路径变成 `["a", "b", "c", "d"]`
- 查询可以直接写：`json.a.b.c.d:hello`

这个行为来自 `JsonPathWriter::push()`，开启后会把 segment 内部的 `.` 替换成内部 path 分隔符 `\x01`。[json_path_writer.rs](/DATA/disk1/jinhelin/tantivy/common/src/json_path_writer.rs#L51)

**对照**
同一个文档 `{"k8s.node.name":"hello"}`：

| 设置 | 索引时理解 | 查询写法 | fast field 写法 | 内部 path |
|---|---|---|---|---|
| `false` | 单个 key `"k8s.node.name"` | `json.k8s\.node\.name:hello` | `json.k8s\.node\.name` | `k8s.node.name` |
| `true` | 嵌套路径 `k8s -> node -> name` | `json.k8s.node.name:hello` | `json.k8s.node.name` | `k8s\x01node\x01name` |

对应测试可以看：
- query parser 的 escaped dot 行为：[query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1349)
- fast field 在 `false/true` 下的行为：[fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1088) [fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1139)
- fast field 内部列名示例：[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L465) [fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L474)

有个细节很重要：

- `false` 时，转义有意义，`json.k8s.node.name` 和 `json.k8s\.node\.name` 指向不同路径。
- `true` 时，转义基本失去“保留字面点”的意义。因为即使你写了 `json.k8s\.node\.name`，最后也会被展开成 `k8s -> node -> name`。测试里 fast field 对 `json.attr.age` 和 `json.attr\.age` 都会命中同一列。[fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1154)

**最重要的副作用**
`expand_dots_enabled = true` 会引入路径冲突。

这两份 JSON：

```json
{"a.b": 1}
```

```json
{"a": {"b": 1}}
```

- 在 `false` 下是两条不同路径，可以区分。
- 在 `true` 下会落到同一条路径 `a.b`，无法区分。

这是它最大的 tradeoff，代码注释里也明确提到了歧义问题。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L82)

**它不影响什么**
- 不影响 `store` 里保存的原始 JSON，原始结构还是照存。
- 只影响索引时的 path 解释，以及查询/fast field 的路径解析。
- 不作用于 schema 顶层 field 名本身，只作用于 JSON 对象里的 key。顶层 schema field 的解析还是 `Schema::find_field` 那套逻辑。[schema.rs](/DATA/disk1/jinhelin/tantivy/src/schema/schema.rs#L331)

**怎么选**
- 如果你的数据源里很多 key 本来就带点，比如 `k8s.node.name`、`http.request.method`，而你希望用户直接写 `json.k8s.node.name`，就开 `true`。
- 如果你必须区分 `"a.b"` 和 `{"a":{"b":...}}`，或者日志里 key 的点只是名字的一部分，不是层级，就保持 `false`。

如果你愿意，我可以下一步直接给你画一张更具体的表，把这 4 个例子并排对比：

1. `{"a.b":1}`
2. `{"a":{"b":1}}`
3. `{"a.b":{"c.d":"x"}}`
4. schema field 本身也带点，比如 `json.shadow`

# 默认情况下，对于有 . 的 key，查询要怎么处理？请给出示例代码

默认情况下，`expand_dots_enabled = false`，所以 JSON key 里的 `.` 会被当成 key 名的一部分，不会自动拆成 path。查询时你需要把这些 `.` 转义成 `\.`。

关键点：

- 查询语法里写 `json.k8s\.node\.name:hello`
- 在 Rust 普通字符串里要再转义一次，写成 `"json.k8s\\.node\\.name:hello"`
- 更方便的是用 raw string：`r#"json.k8s\.node\.name:hello"#`

这个行为在 [src/schema/json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L165) 和测试 [src/query/query_parser/query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1349) 里都能看到。

示例 1：用 `QueryParser`

```rust
use serde_json::json;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, Index};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let json_field = schema_builder.add_json_field("json", TEXT); // 默认 expand_dots_enabled = false
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);
    let mut writer = index.writer_for_tests()?;

    writer.add_document(doc!(
        json_field => json!({
            "k8s.node.name": "hello"
        })
    ))?;
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![json_field]);

    // 注意：key 里的 . 要写成 \.
    let query = query_parser.parse_query(r#"json.k8s\.node\.name:hello"#)?;
    let hits = searcher.search(&query, &TopDocs::with_limit(10))?;

    assert_eq!(hits.len(), 1);
    Ok(())
}
```

示例 2：如果 key 里既有点，又有真正的嵌套

```rust
use serde_json::json;
use tantivy::collector::Count;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, Index};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let json_field = schema_builder.add_json_field("json", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);
    let mut writer = index.writer_for_tests()?;

    writer.add_document(doc!(
        json_field => json!({
            "a.b": {
                "c.d": "x"
            }
        })
    ))?;
    writer.commit()?;

    let searcher = index.reader()?.searcher();
    let query_parser = QueryParser::for_index(&index, vec![json_field]);

    // "a.b" 是一个 key，"c.d" 也是一个 key，所以两个点都要转义
    let query = query_parser.parse_query(r#"json.a\.b.c\.d:x"#)?;
    let count = searcher.search(&query, &Count)?;

    assert_eq!(count, 1);
    Ok(())
}
```

示例 3：不用 `QueryParser`，直接构造 `TermQuery`

```rust
use serde_json::json;
use tantivy::collector::Count;
use tantivy::query::TermQuery;
use tantivy::schema::{IndexRecordOption, Schema, TEXT};
use tantivy::{doc, Index, Term};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let json_field = schema_builder.add_json_field("json", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);
    let mut writer = index.writer_for_tests()?;

    writer.add_document(doc!(
        json_field => json!({
            "k8s.node.name": "hello"
        })
    ))?;
    writer.commit()?;

    let searcher = index.reader()?.searcher();

    let mut term = Term::from_field_json_path(json_field, r"k8s\.node\.name", false);
    term.append_type_and_str("hello");

    let query = TermQuery::new(term, IndexRecordOption::Basic);
    let count = searcher.search(&query, &Count)?;

    assert_eq!(count, 1);
    Ok(())
}
```

如果你还要查 fast field，默认情况下字段名也同样要转义。例如：

```rust
let col = searcher
    .segment_reader(0)
    .fast_fields()
    .column_opt::<i64>(r"json.attr\.age")?;
```

这个行为在测试 [src/fastfield/mod.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/mod.rs#L1088) 里有直接示例。

一句话总结：默认模式下，带 `.` 的 JSON key 不是层级，查询时把它当“字面 key”，把 `.` 写成 `\.` 即可。

# 不同的记录，相同的 json path，但是类型不一样，怎么处理？

我先把“同一路径 mixed type”的处理分成倒排、fast field、查询三部分确认一下，尤其是数值类型之间会不会合并。

我再补一个点：同一路径 mixed type 在 fast field/agg 上是不是会真的并存多列，还是只保留一列数值。我看下测试和实现里更具体的说法。

我把 aggregation 的 mixed-type 测试也打开一下，这样可以把“查询怎么兜底”和“聚合怎么分桶”一起说完整。

会共存，不会因为同一个 `json path` 在不同记录里类型不一致而报 schema 冲突。Tantivy 对 JSON path 不做“全局单一类型约束”。

**怎么处理**
- `store` 路径不做统一，原始值按各自真实类型存回去。`searcher.doc()` 取回时，`10` 还是数值，`"10"` 还是字符串，`true` 还是布尔。[store/reader.rs](/DATA/disk1/jinhelin/tantivy/src/store/reader.rs#L239) [schema/document/owned_value.rs](/DATA/disk1/jinhelin/tantivy/src/schema/document/owned_value.rs#L379)
- 倒排索引的键本质上是 `(json_path, type, value)`。同一路径但不同类型，会变成不同的 term，共同存在于索引里。[doc/src/json.md](/DATA/disk1/jinhelin/tantivy/doc/src/json.md#L6) [core/json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L123)
- fast field 也允许同一路径出现多种类型；底层可能对应多个 column，而不是一个 column。[columnar/README.md](/DATA/disk1/jinhelin/tantivy/columnar/README.md#L20) [fastfield/readers.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/readers.rs#L325)

比如这 3 条记录：

```json
{"json": {"a": 10}}
{"json": {"a": "10"}}
{"json": {"a": true}}
```

在逻辑上会变成：

- `(a, I64, 10)` 或数值型 term
- `(a, Str, "10")`
- `(a, Bool, true)`

它们不是互相覆盖，而是并存。

**查询时怎么匹配**
- 如果你走 `QueryParser`，Tantivy 会尽量“多猜几种类型”。例如 `json.a:10` 会同时生成数值 term 和字符串 term，所以数值 `10` 和字符串 `"10"` 都可能命中。[query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L972) [query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1361)
- `true`、日期字符串也有类似逻辑：既尝试 typed term，也保留字符串查询的可能。[query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1384)
- 但如果你自己手工构造 `TermQuery`，那就是强类型匹配。你构造的是 string term，就只匹配 string；构造的是 bool term，就只匹配 bool。

一个细节是：倒排写入时，能放进 `i64` 的 `u64` 会进一步按 `i64` 写，尽量减少数值类型分裂。[core/json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L144)

**fast field / range / 聚合**
- 对同一路径 mixed type，`fast field` 不是强行塞进一个统一列。
- 字符串、数值、布尔这类会分到不同类型列；同一路径下可能有多个列。[columnar/README.md](/DATA/disk1/jinhelin/tantivy/columnar/README.md#L22)
- 数值内部又会尽量合并成一个“能表示全部值”的数值列；columnar 的规则是按 `i64 -> u64 -> f64` 选择可容纳的类型。[columnar/README.md](/DATA/disk1/jinhelin/tantivy/columnar/README.md#L26)
- `RangeQuery` 在 JSON 上只支持 fast field，它会先看你查询边界是什么类型，再去挑对应列；字符串 range 只看字符串列，数值 range 只看数值列，不会跨到别的类型上。[query/range_query/range_query.rs](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query.rs#L108) [query/range_query/range_query_fastfield.rs](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query_fastfield.rs#L79)
- 数值 mixed type 的 fast field range 会做类型插值，比如查询边界是 `i64`，实际列可能是 `u64` 或 `f64`，实现里会转换后再跑范围过滤。[query/range_query/range_query_fastfield.rs](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query_fastfield.rs#L257)

测试里就有一个同一路径既有数字又有字符串的例子，字符串范围和数字范围各自只命中对应类型的值。[query/range_query/range_query_fastfield.rs](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query_fastfield.rs#L800)

聚合也是分类型处理的。`terms` 聚合在 `json.mixed_type` 上会把 `10`、`"blue"`、`"red"`、`-20.5`、`true` 分成不同 bucket，而不是强行归并成一种类型。[aggregation/agg_tests.rs](/DATA/disk1/jinhelin/tantivy/src/aggregation/agg_tests.rs#L851) [aggregation/agg_tests.rs](/DATA/disk1/jinhelin/tantivy/src/aggregation/agg_tests.rs#L930)

**一句话总结**
- 同一路径 mixed type：允许。
- 倒排：按 `(path, type, value)` 并存。
- QueryParser：尽量多发几种类型的查询去兜底。
- fast field：同一路径可能有多列；range/agg 按类型选列。
- `store`：原样保留真实类型。

如果你愿意，我可以下一步直接用一个具体例子，比如 `{"a":10}`, `{"a":"10"}`, `{"a":10.5}`, `{"a":true}`，把：
1. 倒排 term 长什么样，
2. fast field 列会怎么落，
3. `json.a:10`、range、terms agg 分别会返回什么  
完整写成一张对照表。


# 如果有一个 json path 是整数，理论上有一些会被存储到 i64，有一些会被存储到 u64，查询的时候怎么处理？

我再补看两个实现细节：一是 JSON 数字在文档构造阶段怎么定型，二是 fast field 数值列最终怎么选型，这样能把“i64/u64 混在同一路径”讲准确。

按当前实现，这件事分两层看：倒排查询和 fast field 查询。

普通倒排查询里，Tantivy 会尽量把“能放进 `i64` 的整数”统一按 `i64` 处理，所以实际并不会频繁出现“同一个小整数值有的存成 `i64`、有的存成 `u64`”的分裂。

具体是这样：

- JSON 数字在转成 `OwnedValue` 时，先尝试 `i64`，再尝试 `u64`，最后才是 `f64`。[owned_value.rs](/DATA/disk1/jinhelin/tantivy/src/schema/document/owned_value.rs#L379)
- 即便某个值已经是 `U64`，进入 JSON 倒排索引时，如果它还能放进 `i64`，也会被重新按 `i64` 写 term；只有大于 `i64::MAX` 的无符号整数才真的按 `u64` 写入。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L144)

所以对 term query 来说：

- `10` 这种值，不管原始来源更像 `i64` 还是 `u64`，最后通常都会落成 `i64` term。
- `9223372036854775808` 这种超过 `i64::MAX` 的值，才会落成 `u64` term。

查询时，`QueryParser` 也用了同样的偏好：

- 能解析成 `i64`，就先发 `i64` term。
- 不能解析成 `i64` 但能解析成 `u64`，就发 `u64` term。
- 另外还会带一个字符串 term 兜底。[query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L972) [query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1361)

这意味着：

- 查询 `json.a:10` 时，会查 `i64(10)`，能命中所有“值是 10”的普通整数文档。
- 查询 `json.a:9223372036854775808` 时，会查 `u64(9223372036854775808)`，命中那些超大无符号整数文档。

如果你不走 `QueryParser`，而是自己手工构造 `TermQuery`，那就没有这个自动兜底了。你构造 `i64` term 只能打到 `i64`，构造 `u64` term 只能打到 `u64`。

对 fast field 来说，规则又不一样：

- 同一个 JSON path 下，“数值”这一组不会保留成两个数值列，而是会选一个能容纳所有值的实际列类型。
- 选型优先级是 `i64`，其次 `u64`，最后 `f64`。[columnar/README.md](/DATA/disk1/jinhelin/tantivy/columnar/README.md#L22) [column_writers.rs](/DATA/disk1/jinhelin/tantivy/columnar/src/columnar/writer/column_writers.rs#L182)

所以例如：

- 全都是非负且都在 `i64` 范围内，最终列通常就是 `i64`。
- 全都是非负，但有值超过 `i64::MAX`，最终列可能变成 `u64`。
- 如果同时出现负数和超大 `u64`，那 `i64` 和 `u64` 都装不下，最终会退到 `f64`。

而 range query 在 JSON fast field 上会专门做“查询类型到实际列类型”的转换：

- 先看你边界 term 是 `i64` / `u64` / `f64` 哪种。
- 再看底层实际列是 `i64` / `u64` / `f64` 哪种。
- 然后把边界转换到实际列空间再执行范围过滤。[range_query_fastfield.rs](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query_fastfield.rs#L79) [range_query_fastfield.rs](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query_fastfield.rs#L257)

所以结论是：

- 倒排 term 查询：小整数基本统一成 `i64`，超大无符号整数才走 `u64`；`QueryParser` 会按这个规则自动生成对应 term。
- fast field range/agg：同一路径的整数不会长期分裂成独立的 `i64`/`u64` 数值列，而是收敛成一个实际数值列类型；查询时再做边界类型转换。
- 真正麻烦的不是 `i64` 和 `u64` 混用，而是这个 path 还混入了字符串、布尔等别的类型，那就会分成不同类型组分别处理。

一个直观例子：

- 文档 1: `{"a": 10}`
- 文档 2: `{"a": 20}`
- 文档 3: `{"a": 9223372036854775808}`

那么：

- `json.a:10` 会按 `i64(10)` 查，命中文档 1。
- `json.a:9223372036854775808` 会按 `u64(...)` 查，命中文档 3。
- 如果 `a` 是 fast field，range 查询会再根据底层列真实类型做转换，不要求你的查询边界类型和列类型完全一致。

如果你要，我可以下一条直接给你画一个表，把这几种情况并排列出来：

- 全部在 `i64` 范围
- 部分超过 `i64::MAX`
- 同时有负数和超大正数
- 再混入字符串 `"10"` 时分别会怎样

# {"a":"10"} 这样的 json，a 会被推导成什么类型

对于 JSON 值 `{"a":"10"}`，`a` 在索引时会被当成 **字符串类型 `Str`**，不是整数。

原因很直接：

- 这是一个 JSON string，不是 JSON number。
- Tantivy 对 JSON 字段里的 string 只会尝试两种解释：
  - 如果像 RFC3339 时间，就当 `Date`
  - 否则就当普通 `Str`
- `"10"` 不像日期，所以会落成 `Str("10")`。[doc/src/json.md](/DATA/disk1/jinhelin/tantivy/doc/src/json.md#L59) [src/schema/document/owned_value.rs#L395](/DATA/disk1/jinhelin/tantivy/src/schema/document/owned_value.rs#L395)

也就是说：

```json
{"a":"10"}
```

和

```json
{"a":10}
```

是不一样的：

- `{"a":"10"}` -> `a` 是 `Str`
- `{"a":10}` -> `a` 是数值，通常会按 `I64(10)` 进入倒排索引；只有超出 `i64` 范围才会变成 `U64`。[src/core/json_utils.rs#L144](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L144)

一个容易混淆的点是查询时：

- 你写 `json.a:10`
- QueryParser 可能会同时尝试“数字 10”和“字符串 `"10"`”两种 term 去匹配 JSON 字段。[src/query/query_parser/query_parser.rs#L972](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L972) [src/query/query_parser/query_parser.rs#L1361](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1361)

所以从“搜索效果”看，`json.a:10` 可能命中 `{"a":"10"}`，但这不代表索引时它被推导成了整数；它本质上还是按字符串存的。

如果你要，我可以接着把下面这 4 种情况并排写成表：

- `{"a":"10"}`
- `{"a":10}`
- `{"a":"2019-10-12T07:20:50Z"}`
- `{"a":true}`

# 介绍一下 tantivy 中，json 内部字段类型的推理逻辑

我补看一下日期字符串的判定入口和 `parse_json` 这条路径里的限制，避免把“文档解析阶段”和“索引阶段”的推理混在一起。

我先核对 Tantivy 里 JSON 值从 `serde_json::Value` 到索引项、查询解析、fast field 列的几处关键实现，确认回答基于当前仓库代码而不是文档印象。

我已经确认了数值、字符串、日期、查询侧和 columnar 侧的主逻辑，接着补一下日期预判和 `null`/数组在索引阶段的处理细节。

关键点都对上了，我再把查询端“把用户输入字符串推断成哪种 JSON term”的函数本身定位一下，这样可以直接说明优先级。

我已经把查询端的数值推断函数也定位到了，接下来直接整理成一套“文档值推断 → 索引归一化 → 查询推断 → 列式存储”的说明。

这里说的是 JSON field 内部 value 的自动推断，不是 schema 里显式声明的 `FieldType`。按当前仓库代码，Tantivy 里这套逻辑分 4 层。

1. 文档进入 Tantivy 时，先把 `serde_json::Value` 转成 `OwnedValue`。`null -> Null`，`bool -> Bool`，`number` 按 `i64 -> u64 -> f64` 依次尝试，`string` 只有在“首字节是数字”时才会尝试按 RFC3339 解析成 `Date`，否则就是 `Str`；`array/object` 递归处理。[owned_value.rs](/DATA/disk1/jinhelin/tantivy/src/schema/document/owned_value.rs#L379) [existing_type_impls.rs](/DATA/disk1/jinhelin/tantivy/src/schema/document/existing_type_impls.rs#L26)

2. 建倒排索引时，会把 JSON 展平到具体 path，再按叶子值类型发 term。`Str` 走 analyzer；`I64/F64/Bool/Date` 直接发对应 typed term；`Null` 不发 term；数组会把每个元素都按同一路径继续索引；对象继续下钻。还有一个细节：如果叶子值是 `U64`，索引时会先尝试塞进 `i64`，能放下就按 `i64` 建 term，只有超出 `i64::MAX` 才真用 `u64` term。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L123) [json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L211)

3. 查询解析时，JSON 查询里的字面量会再做一次“字符串到类型”的推断。顺序是 `Date -> i64 -> u64 -> f64 -> bool`；如果成功，会先生成一个 typed term。然后它还会再走 tokenizer 生成字符串 term，所以像 `json.a:10` 这类查询，查询侧可能同时查“数字 10”和“字符串 `"10"`”。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L243) [query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L948) [query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L1361)

4. fast field / columnar 的数值类型选择又是另一层逻辑。它不是按单条文档决定，而是看某个 path 对应这一列的所有数值是否兼容，然后按 `i64 -> u64 -> f64` 选最终列类型。也就是说，同一个 JSON path 上如果全是落在 `i64` 范围内的整数，最终列通常会收敛成 `i64`；出现超大无符号数才需要 `u64`；出现浮点就退到 `f64`。[column_writers.rs](/DATA/disk1/jinhelin/tantivy/columnar/src/columnar/writer/column_writers.rs#L152)

几个直接例子：

- `{"a": 10}`：当前实现里通常先变成 `I64(10)`，索引也是 `I64`
- `{"a": 9223372036854775808}`：会变成 `U64`
- `{"a": 10.5}`：会变成 `F64`
- `{"a": "10"}`：是 `Str("10")`，不会自动变成数字
- `{"a": "2024-01-02T03:04:05Z"}`：会被推成 `Date`
- `{"a": [1, "2", true]}`：同一路径 `a` 下会同时出现多种类型
- `{"a": null}`：文档值里是 `Null`，但倒排索引不会为它发 term

再补一句：仓库文档里还写着 number 的优先级是 `u64 -> i64 -> f64`，但当前代码实现实际是 `i64 -> u64 -> f64`，这里应以代码为准。[json.md](/DATA/disk1/jinhelin/tantivy/doc/src/json.md#L59)

如果你愿意，我下一条可以继续把这套逻辑串到“写入路径”上，按 `parse_json -> document -> indexer -> term/fastfield` 画一条完整链路。


# tantivy 的 json 类型的局限性

再补一个经常踩的边界：`parse_json` 这条入口要求整个文档是顶层 object，而且 `JsonObject` 字段在这条路径上只接受 object 值。

我顺手把一个更隐蔽的点也看了下：某些保留分隔字节会直接导致 path 被跳过，这个一般业务 JSON 不会遇到，但属于实现限制。

我还确认一下 `null` 在 fast field/columnar 这边是不是也被当成“没有值”，因为这会影响能不能区分 `null` 和缺失字段。

我把 `null` 的处理也钉死了：倒排和 columnar 都是跳过，所以 JSON 里的 `null` 基本等价于“不可检索值”。

Tantivy 的 `json` 类型，核心局限不在“能不能存 JSON”，而在“语义不够强、类型不够稳定、路径配置不够细”。

**主要局限**
- 类型是按每个值临时推断的，不是按 `json path` 固定声明的。同一个 path 在不同文档里可以同时出现 `str / i64 / u64 / f64 / bool / date`，Tantivy 不会在 segment 级别帮你统一类型。这会让查询、聚合、fast field 行为都变复杂。[json.md](/DATA/disk1/jinhelin/tantivy/doc/src/json.md#L54) [owned_value.rs](/DATA/disk1/jinhelin/tantivy/src/schema/document/owned_value.rs#L379)
- 查询语义有歧义。JSON 查询时，`json.a:10` 这类输入，查询侧会先尝试把 `"10"` 推成数值/日期/bool，再额外生成字符串 term，所以实际可能同时查多个类型。这提升召回，但降低了“类型确定性”。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L243) [query_parser.rs](/DATA/disk1/jinhelin/tantivy/src/query/query_parser/query_parser.rs#L948)
- 数组不是 nested object。数组里的对象会被展平为一包 terms，所以跨元素“串味”是正常的，`cart.product_type:sneakers AND cart.attributes.color:red` 可能错误命中同一文档里不同数组元素的值。[json.md](/DATA/disk1/jinhelin/tantivy/doc/src/json.md#L106)
- schema 配置只有字段级，没有 path 级。当前 `JsonObjectOptions` 只有 `stored / indexing / fast / expand_dots_enabled` 这类 field 级开关，没有“某个 path 用哪个 analyzer”“某个 path 才开 fast field”“某些 path 忽略索引”这种精细控制。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L12)
- 范围查询支持有限。当前实现里，JSON 的 `RangeQuery` 只支持 fast field；不是 fast field 的 JSON 会直接报错，不能像普通标量字段那样默认走倒排范围查询。[range_query.rs](/DATA/disk1/jinhelin/tantivy/src/query/range_query/range_query.rs#L103)
- `null` 支持很弱。倒排索引里 `Null` 直接跳过，columnar/fast field 里也还是跳过，代码里甚至留了 `TODO: Handle null`。这意味着你基本不能把 `null` 当成一个可检索、可聚合、可精确区分于“缺失字段”的值。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L123) [fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L267) [exist_query.rs](/DATA/disk1/jinhelin/tantivy/src/query/exist_query.rs#L13)
- 动态 JSON 叶子类型并不完整。`PreTokStr / Bytes / Facet / IpAddr` 在 JSON dynamic field 的索引路径上还是 `unimplemented!`，也就是说 JSON 里虽然能承载很多值形态，但真正能走完整动态索引能力的主要还是字符串、数值、布尔、日期。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L196)
- 带 `.` 的 key 天然有歧义。默认要转义查询；开了 `expand_dots_enabled` 又会把 `"a.b"` 当成路径展开，进而和真正的嵌套对象产生语义冲突。[json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L27)
- 如果你走 `TantivyDocument::parse_json` 这条导入路径，还有一个输入约束：整个文档必须是顶层 object，而且 `JsonObject` 字段值在这条路径上也要求是 object，不接受标量 root JSON 值。[default_document.rs](/DATA/disk1/jinhelin/tantivy/src/schema/document/default_document.rs#L197) [field_type.rs](/DATA/disk1/jinhelin/tantivy/src/schema/field_type.rs#L503)

**怎么理解**
如果你的数据是“字段很多、结构松散、主要做关键词检索和少量 exists/filter”，Tantivy 的 JSON field 很方便。  
如果你需要的是“强 schema、nested 语义、path 级 analyzer / fast field / ignore、稳定数值类型、严格 null 语义”，那它就不够强，通常要把关键 path 拆成显式 schema 字段，JSON 只保留做补充索引或存储。

如果你愿意，我下一条可以直接给你一份“什么时候该用 JSON field，什么时候该拆成普通字段”的实战准则。

# json 类型推导到倒排索引和 fast field 时，有什么区别？

先说结论：**JSON 值的第一步类型推导，对倒排索引和 fast field 是同一套**；真正的区别出现在“推导完以后怎么落盘”。

共同点是，JSON 先被转成 `OwnedValue`，规则一致：`number` 按 `i64 -> u64 -> f64`，字符串若能解析成 RFC3339 日期就变 `Date`，否则是 `Str`，`bool/null/array/object` 递归处理。[owned_value.rs](/DATA/disk1/jinhelin/tantivy/src/schema/document/owned_value.rs#L379)

**倒排索引**
- 倒排里存的是 `(json_path, value_type, value)` 这种 typed term。也就是说，类型是 term 的一部分。[json.md](/DATA/disk1/jinhelin/tantivy/doc/src/json.md#L6)
- 字符串一定走 analyzer，写成 postings term，并且有 position 信息。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L126)
- 数值/布尔/日期直接写 typed term，不走分词。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L144)
- 有一个专门的归一化：`U64` 如果放得进 `i64`，会被改写成 `i64` term；只有超出 `i64::MAX` 才保留成 `u64` term。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L144)
- 日期写倒排前会先截断到 `DATE_TIME_PRECISION_INDEXED`。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L186)
- `null` 不发 term，等于在倒排里不可搜。[json_utils.rs](/DATA/disk1/jinhelin/tantivy/src/core/json_utils.rs#L123)

**fast field / columnar**
- fast field 不是写 term，而是按 path 记到 columnar 里；同一路径的值最终变成列数据。[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L254)
- 字符串的行为和倒排不同：
  - 如果给 JSON fast field 配了 tokenizer，就把分词后的 token 逐个写入字符串列。
  - 如果没配 tokenizer，就把原始字符串直接写入列。
  这和倒排“字符串一定按 indexing tokenizer 建 postings”不一样。[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L270) [json_object_options.rs](/DATA/disk1/jinhelin/tantivy/src/schema/json_object_options.rs#L123)
- 数值在写 fast field 时，先按当前叶子值原样记录成 `I64/U64/F64`。[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L280)
- 但**最终列类型不是按单个值决定，而是按整列兼容性决定**：同一路径所有数值如果都能放进 `i64`，列就选 `i64`；否则再看能不能全放进 `u64`；再不行才退到 `f64`。[column_writers.rs](/DATA/disk1/jinhelin/tantivy/columnar/src/columnar/writer/column_writers.rs#L152)
- 所以它和倒排最大的差别是：
  - 倒排是“每个 term 自带明确类型”
  - fast field 是“同一路径的一组数值最后收敛成一个列类型”
- 日期写 fast field 时这里没有像倒排那样先做那次显式截断，而是直接 `record_datetime`。[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L304)
- `null` 在 fast field 里同样被跳过，代码里还留着 `TODO: Handle null`。[fastfield/writer.rs](/DATA/disk1/jinhelin/tantivy/src/fastfield/writer.rs#L267)

**一个最实用的理解**
- `{"a": 10}`
  - 倒排：通常就是 `path=a, type=I64, value=10`
  - fast field：`a` 这列通常也会落成 `i64`
- `{"a": 9223372036854775808}`
  - 倒排：会是 `U64`
  - fast field：如果这一路径里出现了这种值，数值列可能被迫是 `u64`
- `{"a": "hello world"}`
  - 倒排：按 indexing tokenizer 发多个 term
  - fast field：要么存原串，要么按 fast-field tokenizer 记 token，取决于 fast 配置
- `{"a": null}`
  - 倒排和 fast field 都基本当成“没有可索引值”

如果你想，我下一条可以继续把这个差异落到“查询行为”上，解释为什么同一个 JSON path 在 term query、range query、aggregation 上表现会不一样。
