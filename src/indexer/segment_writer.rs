use common::JsonPathWriter;

use super::doc_id_mapping::{get_doc_id_mapping_from_field, DocIdMapping};
use super::document_indexer::{
    index_document, text_analyzers_for_schema, DocumentIndexingMode, DocumentIndexingOutput,
};
use super::operation::AddOperation;
use crate::fastfield::FastFieldsWriter;
use crate::fieldnorm::{FieldNormReaders, FieldNormsWriter};
use crate::index::Segment;
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::postings::{
    compute_table_memory_size, serialize_postings, IndexingContext, PerFieldPostingsWriter,
    PostingsWriter,
};
use crate::schema::document::Document;
use crate::schema::{Field, Schema, Term};
use crate::store::{StoreReader, StoreWriter};
use crate::tokenizer::TextAnalyzer;
use crate::{DocId, Opstamp, SegmentComponent};

/// Computes the initial size of the hash table.
///
/// Returns the recommended initial table size as a power of 2.
///
/// Note this is a very dumb way to compute log2, but it is easier to proofread that way.
fn compute_initial_table_size(per_thread_memory_budget: usize) -> crate::Result<usize> {
    let table_memory_upper_bound = per_thread_memory_budget / 3;
    (10..20) // We cap it at 2^19 = 512K capacity.
        // TODO: There are cases where this limit causes a
        // reallocation in the hashmap. Check if this affects performance.
        .map(|power| 1 << power)
        .take_while(|capacity| compute_table_memory_size(*capacity) < table_memory_upper_bound)
        .last()
        .ok_or_else(|| {
            crate::TantivyError::InvalidArgument(format!(
                "per thread memory budget (={per_thread_memory_budget}) is too small. Raise the \
                 memory budget or lower the number of threads."
            ))
        })
}

fn remap_doc_opstamps(
    opstamps: Vec<Opstamp>,
    doc_id_mapping_opt: Option<&DocIdMapping>,
) -> Vec<Opstamp> {
    if let Some(doc_id_mapping_opt) = doc_id_mapping_opt {
        doc_id_mapping_opt
            .iter_old_doc_ids()
            .map(|doc| opstamps[doc as usize])
            .collect()
    } else {
        opstamps
    }
}

/// A `SegmentWriter` is in charge of creating segment index from a
/// set of documents.
///
/// They creates the postings list in anonymous memory.
/// The segment is laid on disk when the segment gets `finalized`.
pub struct SegmentWriter {
    pub(crate) max_doc: DocId,
    pub(crate) ctx: IndexingContext,
    pub(crate) per_field_postings_writers: PerFieldPostingsWriter,
    pub(crate) segment_serializer: SegmentSerializer,
    pub(crate) fast_field_writers: FastFieldsWriter,
    pub(crate) fieldnorms_writer: FieldNormsWriter,
    pub(crate) json_path_writer: JsonPathWriter,
    pub(crate) doc_opstamps: Vec<Opstamp>,
    per_field_text_analyzers: Vec<TextAnalyzer>,
    term_buffer: Term,
    schema: Schema,
}

struct SegmentWriterDocumentIndexingOutput<'a> {
    postings_writers: &'a mut PerFieldPostingsWriter,
    fieldnorms_writer: &'a mut FieldNormsWriter,
}

impl DocumentIndexingOutput for SegmentWriterDocumentIndexingOutput<'_> {
    fn postings_writer(&mut self, field: Field) -> &mut dyn PostingsWriter {
        self.postings_writers.get_for_field_mut(field)
    }

    fn record_fieldnorm(&mut self, doc: DocId, field: Field, fieldnorm: u32) {
        self.fieldnorms_writer.record(doc, field, fieldnorm);
    }
}

impl SegmentWriter {
    /// Creates a new `SegmentWriter`
    ///
    /// The arguments are defined as follows
    ///
    /// - memory_budget: most of the segment writer data (terms, and postings lists recorders)
    /// is stored in a memory arena. This makes it possible for the user to define
    /// the flushing behavior as a memory limit.
    /// - segment: The segment being written
    /// - schema
    pub fn for_segment(memory_budget_in_bytes: usize, segment: Segment) -> crate::Result<Self> {
        let schema = segment.schema();
        let tokenizer_manager = segment.index().tokenizers().clone();
        let tokenizer_manager_fast_field = segment.index().fast_field_tokenizer().clone();
        let table_size = compute_initial_table_size(memory_budget_in_bytes)?;
        let segment_serializer = SegmentSerializer::for_segment(segment, false)?;
        let per_field_postings_writers = PerFieldPostingsWriter::for_schema(&schema);
        let per_field_text_analyzers =
            text_analyzers_for_schema(&schema, &tokenizer_manager, DocumentIndexingMode::Normal)?;
        Ok(Self {
            max_doc: 0,
            ctx: IndexingContext::new(table_size),
            per_field_postings_writers,
            fieldnorms_writer: FieldNormsWriter::for_schema(&schema),
            json_path_writer: JsonPathWriter::default(),
            segment_serializer,
            fast_field_writers: FastFieldsWriter::from_schema_and_tokenizer_manager(
                &schema,
                tokenizer_manager_fast_field,
            )?,
            doc_opstamps: Vec::with_capacity(1_000),
            per_field_text_analyzers,
            term_buffer: Term::with_capacity(16),
            schema,
        })
    }

    /// Lay on disk the current content of the `SegmentWriter`
    ///
    /// Finalize consumes the `SegmentWriter`, so that it cannot
    /// be used afterwards.
    pub fn finalize(mut self) -> crate::Result<Vec<u64>> {
        self.fieldnorms_writer.fill_up_to_max_doc(self.max_doc);
        let mapping: Option<DocIdMapping> = self
            .segment_serializer
            .segment()
            .index()
            .settings()
            .sort_by_field
            .clone()
            .map(|sort_by_field| get_doc_id_mapping_from_field(sort_by_field, &self))
            .transpose()?;
        remap_and_write(
            self.schema,
            &self.per_field_postings_writers,
            self.ctx,
            self.fast_field_writers,
            &self.fieldnorms_writer,
            self.segment_serializer,
            mapping.as_ref(),
        )?;
        let doc_opstamps = remap_doc_opstamps(self.doc_opstamps, mapping.as_ref());
        Ok(doc_opstamps)
    }

    /// Returns an estimation of the current memory usage of the segment writer.
    /// If the mem usage exceeds the `memory_budget`, the segment be serialized.
    pub fn mem_usage(&self) -> usize {
        self.ctx.mem_usage()
            + self.fieldnorms_writer.mem_usage()
            + self.fast_field_writers.mem_usage()
            + self.segment_serializer.mem_usage()
    }

    fn index_document<D: Document>(&mut self, doc: &D) -> crate::Result<()> {
        let mut output = SegmentWriterDocumentIndexingOutput {
            postings_writers: &mut self.per_field_postings_writers,
            fieldnorms_writer: &mut self.fieldnorms_writer,
        };
        index_document(
            self.max_doc,
            doc,
            &self.schema,
            DocumentIndexingMode::Normal,
            &mut self.per_field_text_analyzers,
            &mut self.term_buffer,
            &mut self.json_path_writer,
            &mut self.ctx,
            &mut output,
        )
    }

    /// Indexes a new document
    ///
    /// As a user, you should rather use `IndexWriter`'s add_document.
    pub fn add_document<D: Document>(
        &mut self,
        add_operation: AddOperation<D>,
    ) -> crate::Result<()> {
        let AddOperation { document, opstamp } = add_operation;
        self.doc_opstamps.push(opstamp);
        self.fast_field_writers.add_document(&document)?;
        self.index_document(&document)?;
        let doc_writer = self.segment_serializer.get_store_writer();
        doc_writer.store(&document, &self.schema)?;
        self.max_doc += 1;
        Ok(())
    }

    /// Max doc is
    /// - the number of documents in the segment assuming there is no deletes
    /// - the maximum document id (including deleted documents) + 1
    ///
    /// Currently, **tantivy** does not handle deletes anyway,
    /// so `max_doc == num_docs`
    pub fn max_doc(&self) -> u32 {
        self.max_doc
    }

    /// Number of documents in the index.
    /// Deleted documents are not counted.
    ///
    /// Currently, **tantivy** does not handle deletes anyway,
    /// so `max_doc == num_docs`
    #[allow(dead_code)]
    pub fn num_docs(&self) -> u32 {
        self.max_doc
    }
}

/// This method is used as a trick to workaround the borrow checker
/// Writes a view of a segment by pushing information
/// to the `SegmentSerializer`.
///
/// `doc_id_map` is used to map to the new doc_id order.
fn remap_and_write(
    schema: Schema,
    per_field_postings_writers: &PerFieldPostingsWriter,
    ctx: IndexingContext,
    fast_field_writers: FastFieldsWriter,
    fieldnorms_writer: &FieldNormsWriter,
    mut serializer: SegmentSerializer,
    doc_id_map: Option<&DocIdMapping>,
) -> crate::Result<()> {
    debug!("remap-and-write");
    if let Some(fieldnorms_serializer) = serializer.extract_fieldnorms_serializer() {
        fieldnorms_writer.serialize(fieldnorms_serializer, doc_id_map)?;
    }
    let fieldnorm_data = serializer
        .segment()
        .open_read(SegmentComponent::FieldNorms)?;
    let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
    serialize_postings(
        ctx,
        schema,
        per_field_postings_writers,
        fieldnorm_readers,
        doc_id_map,
        serializer.get_postings_serializer(),
    )?;
    debug!("fastfield-serialize");
    fast_field_writers.serialize(serializer.get_fast_field_write(), doc_id_map)?;

    // finalize temp docstore and create version, which reflects the doc_id_map
    if let Some(doc_id_map) = doc_id_map {
        debug!("resort-docstore");
        let store_write = serializer
            .segment_mut()
            .open_write(SegmentComponent::Store)?;
        let settings = serializer.segment().index().settings();
        let store_writer = StoreWriter::new(
            store_write,
            settings.docstore_compression,
            settings.docstore_blocksize,
            settings.docstore_compress_dedicated_thread,
        )?;
        let old_store_writer = std::mem::replace(&mut serializer.store_writer, store_writer);
        old_store_writer.close()?;
        let store_read = StoreReader::open(
            serializer
                .segment()
                .open_read(SegmentComponent::TempStore)?,
            1, /* The docstore is configured to have one doc per block, and each doc is accessed
                * only once: we don't need caching. */
        )?;
        for old_doc_id in doc_id_map.iter_old_doc_ids() {
            let doc_bytes = store_read.get_document_bytes(old_doc_id)?;
            serializer.get_store_writer().store_bytes(&doc_bytes)?;
        }
    }

    debug!("serializer-close");
    serializer.close()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use crate::collector::{Count, TopDocs};
    use crate::core::json_utils::JsonTermWriter;
    use crate::directory::RamDirectory;
    use crate::postings::TermInfo;
    use crate::query::{PhraseQuery, QueryParser};
    use crate::schema::document::Value;
    use crate::schema::{
        Document, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Type, STORED, STRING,
        TEXT,
    };
    use crate::store::{Compressor, StoreReader, StoreWriter};
    use crate::time::format_description::well_known::Rfc3339;
    use crate::time::OffsetDateTime;
    use crate::tokenizer::{PreTokenizedString, Token};
    use crate::{
        DateTime, Directory, DocAddress, DocSet, Index, IndexWriter, Postings, TantivyDocument,
        Term, TERMINATED,
    };

    #[test]
    #[cfg(not(feature = "compare_hash_only"))]
    fn test_hashmap_size() {
        use super::compute_initial_table_size;
        assert_eq!(compute_initial_table_size(100_000).unwrap(), 1 << 12);
        assert_eq!(compute_initial_table_size(1_000_000).unwrap(), 1 << 15);
        assert_eq!(compute_initial_table_size(15_000_000).unwrap(), 1 << 19);
        assert_eq!(compute_initial_table_size(1_000_000_000).unwrap(), 1 << 19);
        assert_eq!(compute_initial_table_size(4_000_000_000).unwrap(), 1 << 19);
    }

    #[test]
    fn test_prepare_for_store() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT | STORED);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        let pre_tokenized_text = PreTokenizedString {
            text: String::from("A"),
            tokens: vec![Token {
                offset_from: 0,
                offset_to: 1,
                position: 0,
                text: String::from("A"),
                position_length: 1,
            }],
        };

        doc.add_pre_tokenized_text(text_field, pre_tokenized_text);
        doc.add_text(text_field, "title");

        let path = Path::new("store");
        let directory = RamDirectory::create();
        let store_wrt = directory.open_write(path).unwrap();

        let mut store_writer = StoreWriter::new(store_wrt, Compressor::None, 0, false).unwrap();
        store_writer.store(&doc, &schema).unwrap();
        store_writer.close().unwrap();

        let reader = StoreReader::open(directory.open_read(path).unwrap(), 0).unwrap();
        let doc = reader.get::<TantivyDocument>(0).unwrap();

        assert_eq!(doc.field_values().len(), 2);
        assert_eq!(doc.field_values()[0].value().as_str(), Some("A"));
        assert_eq!(doc.field_values()[1].value().as_str(), Some("title"));
    }
    #[test]
    fn test_simple_json_indexing() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(json_field=>json!({"my_field": "b"})))
            .unwrap();
        writer
            .add_document(doc!(json_field=>json!({"my_field": "a"})))
            .unwrap();
        writer
            .add_document(doc!(json_field=>json!({"my_field": "b"})))
            .unwrap();
        writer.commit().unwrap();

        let query_parser = QueryParser::for_index(&index, vec![json_field]);
        let text_query = query_parser.parse_query("my_field:a").unwrap();
        let score_docs: Vec<(_, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4))
            .unwrap();
        assert_eq!(score_docs.len(), 1);

        let text_query = query_parser.parse_query("my_field:b").unwrap();
        let score_docs: Vec<(_, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4))
            .unwrap();
        assert_eq!(score_docs.len(), 2);
    }

    #[test]
    fn test_json_indexing() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | TEXT);
        let schema = schema_builder.build();
        let json_val: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{
            "toto": "titi",
            "float": -0.2,
            "bool": true,
            "unsigned": 1,
            "signed": -2,
            "complexobject": {
                "field.with.dot": 1
            },
            "date": "1985-04-12T23:20:50.52Z",
            "my_arr": [2, 3, {"my_key": "two tokens"}, 4]
        }"#,
        )
        .unwrap();
        let doc = doc!(json_field=>json_val.clone());
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let doc = searcher
            .doc::<TantivyDocument>(DocAddress {
                segment_ord: 0u32,
                doc_id: 0u32,
            })
            .unwrap();
        let serdeser_json_val = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
            &doc.to_json(&schema),
        )
        .unwrap()
        .get("json")
        .unwrap()[0]
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(json_val, serdeser_json_val);
        let segment_reader = searcher.segment_reader(0u32);
        let inv_idx = segment_reader.inverted_index(json_field).unwrap();
        let term_dict = inv_idx.terms();

        let mut term = Term::with_type_and_field(Type::Json, json_field);
        let mut term_stream = term_dict.stream().unwrap();

        let mut json_term_writer = JsonTermWriter::wrap(&mut term, false);

        json_term_writer.push_path_segment("bool");
        json_term_writer.set_fast_value(true);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("complexobject");
        json_term_writer.push_path_segment("field.with.dot");
        json_term_writer.set_fast_value(1i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("date");
        json_term_writer.set_fast_value(DateTime::from_utc(
            OffsetDateTime::parse("1985-04-12T23:20:50.52Z", &Rfc3339).unwrap(),
        ));
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("float");
        json_term_writer.set_fast_value(-0.2f64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("my_arr");
        json_term_writer.set_fast_value(2i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.set_fast_value(3i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.set_fast_value(4i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.push_path_segment("my_key");
        json_term_writer.set_str("tokens");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.set_str("two");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("signed");
        json_term_writer.set_fast_value(-2i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("toto");
        json_term_writer.set_str("titi");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );

        json_term_writer.pop_path_segment();
        json_term_writer.push_path_segment("unsigned");
        json_term_writer.set_fast_value(1i64);
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            json_term_writer.term().serialized_value_bytes()
        );
        assert!(!term_stream.advance());
    }

    #[test]
    fn test_json_tokenized_with_position() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | TEXT);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        let json_val: BTreeMap<String, crate::schema::OwnedValue> =
            serde_json::from_str(r#"{"mykey": "repeated token token"}"#).unwrap();
        doc.add_object(json_field, json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let inv_index = segment_reader.inverted_index(json_field).unwrap();
        let mut term = Term::with_type_and_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term, false);
        json_term_writer.push_path_segment("mykey");
        json_term_writer.set_str("token");
        let term_info = inv_index
            .get_term_info(json_term_writer.term())
            .unwrap()
            .unwrap();
        assert_eq!(
            term_info,
            TermInfo {
                doc_freq: 1,
                postings_range: 2..4,
                positions_range: 2..5
            }
        );
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0);
        assert_eq!(postings.term_freq(), 2);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(&positions[..], &[1, 2]);
        assert_eq!(postings.advance(), TERMINATED);
    }

    #[test]
    fn test_json_raw_no_position() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STRING);
        let schema = schema_builder.build();
        let json_val: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"mykey": "two tokens"}"#).unwrap();
        let doc = doc!(json_field=>json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let inv_index = segment_reader.inverted_index(json_field).unwrap();
        let mut term = Term::with_type_and_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term, false);
        json_term_writer.push_path_segment("mykey");
        json_term_writer.set_str("two tokens");
        let term_info = inv_index
            .get_term_info(json_term_writer.term())
            .unwrap()
            .unwrap();
        assert_eq!(
            term_info,
            TermInfo {
                doc_freq: 1,
                postings_range: 0..1,
                positions_range: 0..0
            }
        );
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqs)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0);
        assert_eq!(postings.term_freq(), 1);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(postings.advance(), TERMINATED);
    }

    #[test]
    fn test_position_overlapping_path() {
        // This test checks that we do not end up detecting phrase query due
        // to several string literal in the same json object being overlapping.
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT);
        let schema = schema_builder.build();
        let json_val: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{"mykey": [{"field": "hello happy tax payer"}, {"field": "nothello"}]}"#,
        )
        .unwrap();
        let doc = doc!(json_field=>json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let mut term = Term::with_type_and_field(Type::Json, json_field);
        let mut json_term_writer = JsonTermWriter::wrap(&mut term, false);
        json_term_writer.push_path_segment("mykey");
        json_term_writer.push_path_segment("field");
        json_term_writer.set_str("hello");
        let hello_term = json_term_writer.term().clone();
        json_term_writer.set_str("nothello");
        let nothello_term = json_term_writer.term().clone();
        json_term_writer.set_str("happy");
        let happy_term = json_term_writer.term().clone();
        let phrase_query = PhraseQuery::new(vec![hello_term, happy_term.clone()]);
        assert_eq!(searcher.search(&phrase_query, &Count).unwrap(), 1);
        let phrase_query = PhraseQuery::new(vec![nothello_term, happy_term]);
        assert_eq!(searcher.search(&phrase_query, &Count).unwrap(), 0);
    }

    #[test]
    fn test_json_term_with_numeric_merge_panic_regression_bug_2283() {
        // https://github.com/quickwit-oss/tantivy/issues/2283
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        let doc = json!({"field": "a"});
        writer.add_document(doc!(json=>doc)).unwrap();
        writer.commit().unwrap();
        let doc = json!({"field": "a", "id": 1});
        writer.add_document(doc!(json=>doc.clone())).unwrap();
        writer.commit().unwrap();

        // Force Merge
        writer.wait_merging_threads().unwrap();
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        let segment_ids = index
            .searchable_segment_ids()
            .expect("Searchable segments failed.");
        index_writer.merge(&segment_ids).wait().unwrap();
        assert!(index_writer.wait_merging_threads().is_ok());
    }

    #[test]
    fn test_bug_regression_1629_position_when_array_with_a_field_value_that_does_not_contain_any_token(
    ) {
        // We experienced a bug where we would have a position underflow when computing position
        // delta in an horrible corner case.
        //
        // See the commit with this unit test if you want the details.
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let doc = TantivyDocument::parse_json(&schema, r#"{"text": [ "bbb", "aaa", "", "aaa"]}"#)
            .unwrap();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        // On debug this did panic on the underflow
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "aaa");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        // On release this was [2, 1]. (< note the decreasing values)
        assert_eq!(positions, &[2, 5]);
    }

    #[test]
    fn test_multiple_field_value_and_long_tokens() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        // This is a bit of a contrived example.
        let tokens = PreTokenizedString {
            text: "roller-coaster".to_string(),
            tokens: vec![Token {
                offset_from: 0,
                offset_to: 14,
                position: 0,
                text: "rollercoaster".to_string(),
                position_length: 2,
            }],
        };
        doc.add_pre_tokenized_text(text, tokens.clone());
        doc.add_pre_tokenized_text(text, tokens);
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "rollercoaster");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(positions, &[0, 3]); //< as opposed to 0, 2 if we had a position length of 1.
    }

    #[test]
    fn test_last_token_not_ending_last() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        // This is a bit of a contrived example.
        let tokens = PreTokenizedString {
            text: "contrived-example".to_string(), //< I can't think of a use case where this corner case happens in real life.
            tokens: vec![
                Token {
                    // Not the last token, yet ends after the last token.
                    offset_from: 0,
                    offset_to: 14,
                    position: 0,
                    text: "long_token".to_string(),
                    position_length: 3,
                },
                Token {
                    offset_from: 0,
                    offset_to: 14,
                    position: 1,
                    text: "short".to_string(),
                    position_length: 1,
                },
            ],
        };
        doc.add_pre_tokenized_text(text, tokens);
        doc.add_text(text, "hello");
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "hello");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(positions, &[4]); //< as opposed to 3 if we had a position length of 1.
    }

    #[test]
    fn test_show_error_when_tokenizer_not_registered() {
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("custom_en")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", text_options);
        let schema = schema_builder.build();
        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        Index::create_in_dir(&tempdir_path, schema).unwrap();
        let index = Index::open_in_dir(tempdir_path).unwrap();
        let schema = index.schema();
        let mut index_writer = index.writer(50_000_000).unwrap();
        let title = schema.get_field("title").unwrap();
        let mut document = TantivyDocument::default();
        document.add_text(title, "The Old Man and the Sea");
        index_writer.add_document(document).unwrap();
        let error = index_writer.commit().unwrap_err();
        assert_eq!(
            error.to_string(),
            "Schema error: 'Error getting tokenizer for field: title'"
        );
    }
}
