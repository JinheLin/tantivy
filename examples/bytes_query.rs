use tantivy::collector::TopDocs;
use tantivy::query::{RangeQuery, TermQuery};
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, ReloadPolicy};
use tempfile::TempDir;

fn main() -> tantivy::Result<()> {
    let index_path = TempDir::new()?;

    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT);
    schema_builder.add_bytes_field("bytes", INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_dir(&index_path, schema.clone())?;
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();
    let bytes = schema.get_field("bytes").unwrap();
    let mut old_man_doc = TantivyDocument::default();
    old_man_doc.add_text(title, "The Old Man and the Sea");
    old_man_doc.add_text(
        body,
        "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone \
         eighty-four days now without taking a fish.",
    );
    let binary_data0: [u8; 17] = *b"Some bytes here 0";
    old_man_doc.add_bytes(bytes, &binary_data0[..]);
    index_writer.add_document(old_man_doc)?;

    let binary_data1: [u8; 17] = *b"Some bytes here 1";
    index_writer.add_document(doc!(
    title => "Of Mice and Men",
    body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
            bank and runs deep and green. The water is warm too, for it has slipped twinkling \
            over the yellow sands in the sunlight before reaching the narrow pool. On one \
            side of the river the golden foothill slopes curve up to the strong and rocky \
            Gabilan Mountains, but on the valley side the water is lined with trees—willows \
            fresh and green with every spring, carrying in their lower leaf junctures the \
            debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
            limbs and branches that arch over the pool",
    bytes => &binary_data1[..]
    ))?;

    // Multivalued field just need to be repeated.
    let binary_data2: [u8; 17] = *b"Some bytes here 2";
    index_writer.add_document(doc!(
    title => "Frankenstein",
    title => "The Modern Prometheus",
    body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
             enterprise which you have regarded with such evil forebodings.  I arrived here \
             yesterday, and my first task is to assure my dear sister of my welfare and \
             increasing confidence in the success of my undertaking.",
    bytes => &binary_data2[..]
    ))?;

    index_writer.commit()?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    let searcher = reader.searcher();

    // Point query
    let term = Term::from_field_bytes(bytes, &binary_data0);
    let term_query = TermQuery::new(term, IndexRecordOption::Basic);
    let top_docs = searcher
        .search(&term_query, &TopDocs::with_limit(10))
        .unwrap();
    assert_eq!(top_docs.len(), 1);
    let doc = searcher
        .doc::<TantivyDocument>(top_docs[0].1)
        .unwrap()
        .to_json(&schema);
    println!("{}", doc);

    let term = Term::from_field_bytes(bytes, &binary_data1);
    let term_query = TermQuery::new(term, IndexRecordOption::Basic);
    let top_docs = searcher
        .search(&term_query, &TopDocs::with_limit(10))
        .unwrap();
    assert_eq!(top_docs.len(), 1);
    let doc = searcher
        .doc::<TantivyDocument>(top_docs[0].1)
        .unwrap()
        .to_json(&schema);
    println!("{}", doc);

    let term = Term::from_field_bytes(bytes, &binary_data2);
    let term_query = TermQuery::new(term, IndexRecordOption::Basic);
    let top_docs = searcher
        .search(&term_query, &TopDocs::with_limit(10))
        .unwrap();
    assert_eq!(top_docs.len(), 1);
    let doc = searcher
        .doc::<TantivyDocument>(top_docs[0].1)
        .unwrap()
        .to_json(&schema);
    println!("{}", doc);

    // Range query
    let rq = RangeQuery::new(
        std::ops::Bound::Included(Term::from_field_bytes(bytes, &binary_data0)),
        std::ops::Bound::Excluded(Term::from_field_bytes(bytes, &binary_data2)),
    );
    let top_docs = searcher.search(&rq, &TopDocs::with_limit(10)).unwrap();
    assert_eq!(top_docs.len(), 2);
    for (_score, doc_address) in top_docs {
        let doc = searcher
            .doc::<TantivyDocument>(doc_address)
            .unwrap()
            .to_json(&schema);
        println!("{}", doc);
    }

    Ok(())
}
