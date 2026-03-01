// # Searcher snapshot consistency
//
// This example demonstrates that `Searcher` is an immutable snapshot.
//
// - After a commit, an existing `Searcher` does not see new documents.
// - Even after `IndexReader::reload()`, the old `Searcher` remains a snapshot.
//
// We use `ReloadPolicy::Manual` to make the behavior deterministic.

use tantivy::collector::Count;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, Index, ReloadPolicy, Searcher};

fn count_query(
    searcher: &Searcher,
    query_parser: &QueryParser,
    query_str: &str,
) -> tantivy::Result<usize> {
    let query = query_parser.parse_query(query_str)?;
    searcher.search(&query, &Count)
}

fn describe_searcher(searcher: &Searcher) -> String {
    let segments = searcher
        .segment_readers()
        .iter()
        .map(|segment_reader| (segment_reader.segment_id(), segment_reader.delete_opstamp()))
        .collect::<Vec<_>>();
    format!(
        "gen={} segments={:?}",
        searcher.generation().generation_id(),
        segments
    )
}

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let body = schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);

    let mut writer = index.writer(15_000_000)?;
    writer.add_document(doc!(body => "apple"))?;
    writer.commit()?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;

    let query_parser = QueryParser::for_index(&index, vec![body]);

    let searcher_before = reader.searcher();
    let count_before = count_query(&searcher_before, &query_parser, "apple")?;
    println!(
        "before commit2: {} count={}",
        describe_searcher(&searcher_before),
        count_before
    );
    assert_eq!(count_before, 1);

    // Second commit adds a new document.
    writer.add_document(doc!(body => "apple"))?;
    writer.commit()?;

    // The old searcher is a snapshot: it stays on the old view.
    let count_old_no_reload = count_query(&searcher_before, &query_parser, "apple")?;
    println!(
        "old searcher after commit2 (no reload): {} count={}",
        describe_searcher(&searcher_before),
        count_old_no_reload
    );
    assert_eq!(count_old_no_reload, 1);

    // With Manual policy, `reader.searcher()` without `reload()` is still the old snapshot.
    let searcher_still_old = reader.searcher();
    let count_still_old = count_query(&searcher_still_old, &query_parser, "apple")?;
    println!(
        "reader.searcher() without reload: {} count={}",
        describe_searcher(&searcher_still_old),
        count_still_old
    );
    assert_eq!(count_still_old, 1);

    // Reload swaps in a new Searcher snapshot.
    reader.reload()?;
    let searcher_after_reload = reader.searcher();
    let count_after_reload = count_query(&searcher_after_reload, &query_parser, "apple")?;
    println!(
        "after reload: {} count={}",
        describe_searcher(&searcher_after_reload),
        count_after_reload
    );
    assert_eq!(count_after_reload, 2);

    // Reloading does not mutate existing Searchers.
    let count_old_after_reload = count_query(&searcher_before, &query_parser, "apple")?;
    println!(
        "old searcher after reload: {} count={}",
        describe_searcher(&searcher_before),
        count_old_after_reload
    );
    assert_eq!(count_old_after_reload, 1);

    Ok(())
}
