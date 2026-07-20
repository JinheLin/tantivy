use std::fmt;

use super::term_weight::TermWeight;
use crate::query::bm25::Bm25Weight;
use crate::query::single_document::{effective_record_option, validate_term_info};
use crate::query::{
    DocumentEvaluation, EnableScoring, Explanation, Query, SingleDocument,
    SingleDocumentEvaluationContext, SingleDocumentEvaluator, Weight,
};
use crate::schema::IndexRecordOption;
use crate::{TantivyError, Term};

/// A Term query matches all of the documents
/// containing a specific term.
///
/// The score associated is defined as
/// `idf` *  sqrt(`term_freq` / `field norm`)
/// in which :
/// * `idf`        - inverse document frequency.
/// * `term_freq`  - number of occurrences of the term in the field
/// * `field norm` - number of tokens in the field.
///
/// ```rust
/// use tantivy::collector::{Count, TopDocs};
/// use tantivy::query::TermQuery;
/// use tantivy::schema::{Schema, TEXT, IndexRecordOption};
/// use tantivy::{doc, Index, IndexWriter, Term};
/// # fn test() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
/// {
///     let mut index_writer: IndexWriter = index.writer(15_000_000)?;
///     index_writer.add_document(doc!(
///         title => "The Name of the Wind",
///     ))?;
///     index_writer.add_document(doc!(
///         title => "The Diary of Muadib",
///     ))?;
///     index_writer.add_document(doc!(
///         title => "A Dairy Cow",
///     ))?;
///     index_writer.add_document(doc!(
///         title => "The Diary of a Young Girl",
///     ))?;
///     index_writer.commit()?;
/// }
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
/// let query = TermQuery::new(
///     Term::from_field_text(title, "diary"),
///     IndexRecordOption::Basic,
/// );
/// let (top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count))?;
/// assert_eq!(count, 2);
/// Ok(())
/// # }
/// # assert!(test().is_ok());
/// ```
#[derive(Clone)]
pub struct TermQuery {
    term: Term,
    index_record_option: IndexRecordOption,
}

impl fmt::Debug for TermQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TermQuery({:?})", self.term)
    }
}

impl TermQuery {
    /// Creates a new term query.
    pub fn new(term: Term, segment_postings_options: IndexRecordOption) -> TermQuery {
        TermQuery {
            term,
            index_record_option: segment_postings_options,
        }
    }

    /// The `Term` this query is built out of.
    pub fn term(&self) -> &Term {
        &self.term
    }

    /// Returns a weight object.
    ///
    /// While `.weight(...)` returns a boxed trait object,
    /// this method return a specific implementation.
    /// This is useful for optimization purpose.
    pub fn specialized_weight(
        &self,
        enable_scoring: EnableScoring<'_>,
    ) -> crate::Result<TermWeight> {
        let schema = enable_scoring.schema();
        let field_entry = schema.get_field_entry(self.term.field());
        if !field_entry.is_indexed() {
            let error_msg = format!("Field {:?} is not indexed.", field_entry.name());
            return Err(crate::TantivyError::SchemaError(error_msg));
        }
        let bm25_weight = match enable_scoring {
            EnableScoring::Enabled {
                statistics_provider,
                ..
            } => Bm25Weight::for_terms(statistics_provider, &[self.term.clone()])?,
            EnableScoring::Disabled { .. } => {
                Bm25Weight::new(Explanation::new("<no score>", 1.0f32), 1.0f32)
            }
        };
        let scoring_enabled = enable_scoring.is_scoring_enabled();
        let index_record_option = if scoring_enabled {
            self.index_record_option
        } else {
            IndexRecordOption::Basic
        };

        Ok(TermWeight::new(
            self.term.clone(),
            index_record_option,
            bm25_weight,
            scoring_enabled,
        ))
    }
}

impl Query for TermQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight(enable_scoring)?))
    }

    fn single_document_evaluator(
        &self,
        context: SingleDocumentEvaluationContext<'_>,
    ) -> crate::Result<Box<dyn SingleDocumentEvaluator>> {
        let (record_option, has_fieldnorms) =
            effective_record_option(context.schema(), &self.term, self.index_record_option)?;
        let bm25_weight = context
            .statistics_provider()
            .map(|statistics| -> crate::Result<Bm25Weight> {
                // Caller must supply valid BM25 statistics; see `single_document` module docs.
                Ok(
                    Bm25Weight::for_terms(statistics, std::slice::from_ref(&self.term))?
                        .boost_by(context.boost()),
                )
            })
            .transpose()?;
        Ok(Box::new(TermSingleDocumentEvaluator {
            field: self.term.field(),
            term: self.term.clone(),
            record_option,
            has_fieldnorms,
            bm25_weight,
        }))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        visitor(&self.term, false);
    }
}

struct TermSingleDocumentEvaluator {
    field: crate::schema::Field,
    term: Term,
    record_option: IndexRecordOption,
    has_fieldnorms: bool,
    bm25_weight: Option<Bm25Weight>,
}

impl SingleDocumentEvaluator for TermSingleDocumentEvaluator {
    fn evaluate_impl(
        &mut self,
        document: &dyn SingleDocument,
    ) -> crate::Result<DocumentEvaluation> {
        let Some(term_info) = document.term_info(&self.term) else {
            return Ok(DocumentEvaluation::NoMatch);
        };
        validate_term_info(&self.term, term_info.term_freq)?;

        let Some(bm25_weight) = &self.bm25_weight else {
            return Ok(DocumentEvaluation::Match(1.0));
        };
        let term_freq = if self.record_option.has_freq() {
            term_info.term_freq
        } else {
            1
        };
        let fieldnorm_id = if self.has_fieldnorms {
            document.fieldnorm_id(self.term.field()).ok_or_else(|| {
                TantivyError::InvalidArgument(format!(
                    "SingleDocument did not supply a fieldnorm for field {:?}",
                    self.term.field()
                ))
            })?
        } else {
            // Segment scorers use `FieldNormReader::constant(_, 1)` when fieldnorms are disabled;
            // `fieldnorm_to_id(1)` is also 1.
            1
        };
        let score = bm25_weight.score(fieldnorm_id, term_freq);
        Ok(DocumentEvaluation::Match(score))
    }

    fn required_fields(&self) -> Option<&[crate::schema::Field]> {
        Some(std::slice::from_ref(&self.field))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv6Addr};
    use std::str::FromStr;

    use columnar::MonotonicallyMappableToU128;

    use crate::collector::{Count, TopDocs};
    use crate::query::{Query, QueryParser, TermQuery};
    use crate::schema::{IndexRecordOption, IntoIpv6Addr, Schema, INDEXED, STORED};
    use crate::{Index, IndexWriter, Term};

    #[test]
    fn search_ip_test() {
        let mut schema_builder = Schema::builder();
        let ip_field = schema_builder.add_ip_addr_field("ip", INDEXED | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let ip_addr_1 = IpAddr::from_str("127.0.0.1").unwrap().into_ipv6_addr();
        let ip_addr_2 = Ipv6Addr::from_u128(10);

        {
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            index_writer
                .add_document(doc!(
                    ip_field => ip_addr_1
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    ip_field => ip_addr_2
                ))
                .unwrap();

            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let assert_single_hit = |query| {
            let (_top_docs, count) = searcher
                .search(&query, &(TopDocs::with_limit(2), Count))
                .unwrap();
            assert_eq!(count, 1);
        };
        let query_from_text = |text: String| {
            QueryParser::for_index(&index, vec![ip_field])
                .parse_query(&text)
                .unwrap()
        };

        let query_from_ip = |ip_addr| -> Box<dyn Query> {
            Box::new(TermQuery::new(
                Term::from_field_ip_addr(ip_field, ip_addr),
                IndexRecordOption::Basic,
            ))
        };

        assert_single_hit(query_from_ip(ip_addr_1));
        assert_single_hit(query_from_ip(ip_addr_2));
        assert_single_hit(query_from_text("127.0.0.1".to_string()));
        assert_single_hit(query_from_text("\"127.0.0.1\"".to_string()));
        assert_single_hit(query_from_text(format!("\"{ip_addr_1}\"")));
        assert_single_hit(query_from_text(format!("\"{ip_addr_2}\"")));
    }
}
