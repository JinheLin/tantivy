// Test phrase search across multi-valued fields
//
// Question: For a document with multi-valued field ["hello world", "world peace"],
// will a phrase query "world world" match it?
//
// This tests whether phrase positions are continuous across multiple values
// or if there's a gap/separator between them.

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, DocSet};
use tantivy::postings::Postings;

fn main() -> tantivy::Result<()> {
    // Create schema with a text field
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT | STORED);
    let schema = schema_builder.build();

    // Create in-memory index
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add document with multi-valued field: ["hello world", "world peace"]
    // If positions are: hello(0) world(1) | world(2) peace(3)
    // Then "world world" (positions 1,2) should match
    let mut doc = TantivyDocument::default();
    doc.add_text(text_field, "hello world");
    doc.add_text(text_field, "world peace");
    index_writer.add_document(doc)?;

    // Also add a document where "world world" actually appears
    index_writer.add_document(doc!(
        text_field => "world world is here"
    ))?;

    // Add another document with "world" only once
    index_writer.add_document(doc!(
        text_field => "hello world peace"
    ))?;

    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![text_field]);

    println!("=== Multi-value Phrase Search Test ===\n");

    // Test 1: Search for phrase "world world"
    println!("Query: \"world world\" (phrase query)");
    let query = query_parser.parse_query("\"world world\"")?;
    println!("Parsed query: {:?}\n", query);

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    println!("Results ({} hits):", top_docs.len());
    for (score, doc_address) in &top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(*doc_address)?;
        println!("  score={:.4}, doc={}", score, retrieved_doc.to_json(&schema));
    }

    println!("\n---\n");

    // Test 2: Search for phrase "hello world"
    println!("Query: \"hello world\" (phrase query)");
    let query = query_parser.parse_query("\"hello world\"")?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    println!("Results ({} hits):", top_docs.len());
    for (score, doc_address) in &top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(*doc_address)?;
        println!("  score={:.4}, doc={}", score, retrieved_doc.to_json(&schema));
    }

    println!("\n---\n");

    // Test 3: Search for phrase "world peace"
    println!("Query: \"world peace\" (phrase query)");
    let query = query_parser.parse_query("\"world peace\"")?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    println!("Results ({} hits):", top_docs.len());
    for (score, doc_address) in &top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(*doc_address)?;
        println!("  score={:.4}, doc={}", score, retrieved_doc.to_json(&schema));
    }

    println!("\n---\n");

    // Test 4: Let's also check the term positions directly
    println!("=== Checking term positions ===");
    let segment_reader = searcher.segment_reader(0);
    let inverted_index = segment_reader.inverted_index(text_field)?;

    let term = tantivy::Term::from_field_text(text_field, "world");
    if let Some(mut postings) = inverted_index.read_postings(&term, tantivy::schema::IndexRecordOption::WithFreqsAndPositions)? {
        println!("\nTerm 'world' postings:");
        loop {
            let doc_id = postings.doc();
            if doc_id == tantivy::TERMINATED {
                break;
            }
            let freq = postings.term_freq();

            // Get actual positions
            let mut pos_vec = Vec::new();
            postings.positions(&mut pos_vec);
            println!("  doc_id={}, freq={}, positions={:?}", doc_id, freq, pos_vec);

            if postings.advance() == tantivy::TERMINATED {
                break;
            }
        }
    }

    Ok(())
}
