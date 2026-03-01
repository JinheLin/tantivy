use tantivy::collector::{Count, TopDocs};
use tantivy::query::{QueryParser, TermQuery};
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, Term};

fn main() -> tantivy::Result<()> {
    // 1) Schema：分别覆盖 TEXT / STORED / FAST 三个选项。
    let mut schema_builder = Schema::builder();
    let id = schema_builder.add_text_field("id", STRING | STORED);
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT); // 不 STORED：用于演示“可搜但取不回”
    let price = schema_builder.add_u64_field("price", FAST); // 只 FAST：用于演示“可读但不可 TermQuery”
    let schema = schema_builder.build();

    println!("== schema ==");
    println!("{}", serde_json::to_string_pretty(&schema).expect("schema json"));

    // 2) 建索引并写入两篇文档。
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    writer.add_document(doc!(
        id => "doc-1",
        title => "The Old Man and the Sea",
        body => "He was an old man who fished alone in a skiff in the Gulf Stream.",
        price => 42u64
    ))?;

    writer.add_document(doc!(
        id => "doc-2",
        title => "Of Mice and Men",
        body => "A few miles south of Soledad, the Salinas River drops in close to the hillside.",
        price => 13u64
    ))?;

    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // 3) STORED：只有标记为 STORED 的字段才能从 docstore 取回。
    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    let query = query_parser.parse_query("sea")?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;
    let (_score, addr) = top_docs
        .into_iter()
        .next()
        .expect("expect at least 1 hit");
    let stored_doc: TantivyDocument = searcher.doc(addr)?;
    println!("== stored doc ==");
    println!("{}", stored_doc.to_json(&schema));

    // 4) FAST：可以从 fast field 读到值（列存随机读）。
    let segment = &searcher.segment_readers()[0];
    let price_col = segment.fast_fields().u64("price")?.first_or_default_col(0);
    println!("== fast field (price) ==");
    for doc in 0..segment.max_doc() {
        println!("price[doc{doc}] = {}", price_col.get_val(doc));
    }

    // 5) TermQuery：TermQuery 只对 indexed 字段有效；TEXT 字段也不会自动做 tokenizer 分析。
    println!("== term queries ==");

    let id_query = TermQuery::new(Term::from_field_text(id, "doc-2"), IndexRecordOption::Basic);
    let id_hits = searcher.search(&id_query, &Count)?;
    println!("TermQuery(id=\"doc-2\") hits = {id_hits}");

    let title_upper =
        TermQuery::new(Term::from_field_text(title, "Sea"), IndexRecordOption::Basic);
    let title_upper_hits = searcher.search(&title_upper, &Count)?;
    println!("TermQuery(title=\"Sea\") hits = {title_upper_hits}");

    let title_lower =
        TermQuery::new(Term::from_field_text(title, "sea"), IndexRecordOption::Basic);
    let title_lower_hits = searcher.search(&title_lower, &Count)?;
    println!("TermQuery(title=\"sea\") hits = {title_lower_hits}");

    let qp_hits = searcher.search(&query_parser.parse_query("title:Sea")?, &Count)?;
    println!("QueryParser(\"title:Sea\") hits = {qp_hits}");

    let price_term_query =
        TermQuery::new(Term::from_field_u64(price, 42u64), IndexRecordOption::Basic);
    match searcher.search(&price_term_query, &Count) {
        Ok(count) => println!("TermQuery(price=42) hits = {count}"),
        Err(err) => println!("TermQuery(price=42) error = {err}"),
    }

    Ok(())
}

