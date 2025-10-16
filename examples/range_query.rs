use tantivy::collector::Count;
use tantivy::query::{FastFieldRangeQuery, InvertedIndexRangeQuery};
use tantivy::schema::*;
use tantivy::{doc, Index, Term};
use std::ops::Bound;
use std::time::{Duration, Instant};

fn bench_range_query(
    searcher: &tantivy::Searcher,
    field: Field,
    start_val: u64,
    end_val: u64,
    iterations: usize,
) -> (usize, Duration) {
    let mut total_duration = Duration::ZERO;
    let mut total_count = 0;
    for _ in 0..iterations {
        let lower = Bound::Included(Term::from_field_u64(field, start_val));
        let upper = Bound::Included(Term::from_field_u64(field, end_val));
        let query = InvertedIndexRangeQuery::new(lower, upper);
        let start = Instant::now();
        let count = searcher.search(&query, &Count).unwrap();
        total_duration += start.elapsed();
        total_count = count;
    }

    (total_count, total_duration / iterations as u32)
}

fn bench_fast_field(
    searcher: &tantivy::Searcher,
    field: Field,
    start_val: u64,
    end_val: u64,
    iterations: usize,
) -> (usize, Duration) {
    let mut total_duration = Duration::ZERO;
    let mut total_count = 0;
    for _ in 0..iterations {
        let lower = Bound::Included(Term::from_field_u64(field, start_val));
        let upper = Bound::Included(Term::from_field_u64(field, end_val));
        let query = FastFieldRangeQuery::new(lower, upper);   
        let start = Instant::now();
        let count = searcher.search(&query, &Count).unwrap();
        total_duration += start.elapsed();
        total_count = count;
    }

    (total_count, total_duration / iterations as u32)
}

fn main() -> tantivy::Result<()> {
    // === 配置参数 ===
    let total_docs: u64 = 1_000_000;
    let narrow_pct: f64 = 0.10;
    let wide_pct: f64 = 0.90;
    let iterations: usize = 3;

    // === Schema ===
    let mut schema_builder = Schema::builder();
    let timestamp_field = schema_builder.add_u64_field(
        "timestamp",
        STORED | FAST | INDEXED
    );
    let schema = schema_builder.build();

    // === Index ===
    let index = Index::create_in_ram(schema);
    let mut writer = index.writer(50_000_000)?;
    for i in 0..total_docs {
        writer.add_document(doc!(timestamp_field => i)).unwrap();
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // === 范围 ===
    let narrow_start = total_docs / 10;
    let narrow_end = narrow_start + (total_docs as f64 * narrow_pct) as u64;
    let wide_start = total_docs / 10;
    let wide_end = wide_start + (total_docs as f64 * wide_pct) as u64;

    println!("=== Tantivy 0.24 Range Query Benchmark ===");
    println!("总文档数: {}", total_docs);
    println!("迭代次数: {}", iterations);
    println!("小范围: {}..{}", narrow_start, narrow_end);
    println!("大范围: {}..{}", wide_start, wide_end);
    println!();

    // === 倒排索引 RangeQuery ===
    let (count_narrow_inverted, dur_narrow_inverted) =
        bench_range_query(&searcher, timestamp_field, narrow_start, narrow_end, iterations);
    let (count_wide_inverted, dur_wide_inverted) =
        bench_range_query(&searcher, timestamp_field, wide_start, wide_end, iterations);

    println!("[倒排索引 RangeQuery]");
    println!("  小范围: 命中 {} 条, 平均耗时 {:?}", count_narrow_inverted, dur_narrow_inverted);
    println!("  大范围: 命中 {} 条, 平均耗时 {:?}", count_wide_inverted, dur_wide_inverted);
    println!();

    // === Fast Field 模拟 ===
    let (count_narrow_fast, dur_narrow_fast) =
        bench_fast_field(&searcher, timestamp_field, narrow_start, narrow_end, iterations);
    let (count_wide_fast, dur_wide_fast) =
        bench_fast_field(&searcher, timestamp_field, wide_start, wide_end, iterations);

    println!("[Fast Field 手动扫描]");
    println!("  小范围: 命中 {} 条, 平均耗时 {:?}", count_narrow_fast, dur_narrow_fast);
    println!("  大范围: 命中 {} 条, 平均耗时 {:?}", count_wide_fast, dur_wide_fast);

    Ok(())
}
