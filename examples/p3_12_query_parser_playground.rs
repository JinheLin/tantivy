use tantivy::collector::Count;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT | STORED);
    schema_builder.add_i64_field("signed", INDEXED);
    schema_builder.add_u64_field("u64_ff", FAST);
    let json = schema_builder.add_json_field("json", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;
    for doc_json in [
        r#"{
          "title": "The Sea Wolf",
          "body": "Jack London",
          "signed": -1,
          "u64_ff": 10,
          "json": { "user": "alice", "tag": "classic" }
        }"#,
        r#"{
          "title": "The Old Man and the Sea",
          "body": "Ernest Hemingway",
          "signed": 3,
          "u64_ff": 80,
          "json": { "user": "bob", "tag": "sea" }
        }"#,
        r#"{
          "title": "For Whom the Bell Tolls",
          "body": "Ernest Hemingway",
          "signed": 100,
          "u64_ff": 50,
          "json": { "user": "alice", "tag": "war" }
        }"#,
    ] {
        let doc = TantivyDocument::parse_json(&schema, doc_json)?;
        index_writer.add_document(doc)?;
    }
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();

    let mut query_parser = QueryParser::for_index(&index, vec![title, body, json]);
    query_parser.allow_regexes();

    let queries = [
        "sea",
        r#"title:"sea wolf""#,
        r#""sea wo"*"#,
        "signed:{-10 TO 10}",
        "u64_ff:[20 TO 70]",
        "title: IN [wolf sea]",
        "title:/.*wolf/",
        "-title:sea",
        "user:alice",
        "title:",
    ];

    for query_str in queries {
        println!("\n=== {query_str} ===");

        let (user_input_ast, grammar_errs) = query_grammar::parse_query_lenient(query_str);
        println!(
            "\n[query-grammar] UserInputAst:\n{}",
            serde_json::to_string_pretty(&user_input_ast).unwrap()
        );
        if !grammar_errs.is_empty() {
            println!(
                "\n[query-grammar] Lenient errors:\n{}",
                serde_json::to_string_pretty(&grammar_errs).unwrap()
            );
        }

        match query_parser.parse_query(query_str) {
            Ok(query) => {
                println!("\n[QueryParser strict] Ok:\n{query:?}");
            }
            Err(err) => {
                println!("\n[QueryParser strict] Err:\n{err}");
            }
        }

        let (query, parser_errs) = query_parser.parse_query_lenient(query_str);
        if parser_errs.is_empty() {
            println!("\n[QueryParser lenient] (no errors)");
        } else {
            println!("\n[QueryParser lenient] Errors:");
            for err in &parser_errs {
                println!("- {err}");
            }
        }
        println!("\n[QueryParser lenient] Query:\n{query:?}");

        let count = searcher.search(&query, &Count)?;
        println!("\n[Searcher] hit_count={count}");
    }

    Ok(())
}
