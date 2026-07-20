use super::{PhraseMatcher, PhraseWeight};
use crate::query::bm25::Bm25Weight;
use crate::query::single_document::{downgrade_record_option_for_term, validate_term_info};
use crate::query::{
    DocumentEvaluation, EnableScoring, Query, SingleDocument, SingleDocumentEvaluationContext,
    SingleDocumentEvaluator, Weight,
};
use crate::schema::{Field, IndexRecordOption, Term};
use crate::TantivyError;

/// `PhraseQuery` matches a specific sequence of words.
///
/// For instance the phrase query for `"part time"` will match
/// the sentence
///
/// **Alan just got a part time job.**
///
/// On the other hand it will not match the sentence.
///
/// **This is my favorite part of the job.**
///
/// [Slop](PhraseQuery::set_slop) allows leniency in term proximity
/// for some performance tradeof.
///
/// Using a `PhraseQuery` on a field requires positions
/// to be indexed for this field.
#[derive(Clone, Debug)]
pub struct PhraseQuery {
    field: Field,
    phrase_terms: Vec<(usize, Term)>,
    slop: u32,
}

impl PhraseQuery {
    /// Creates a new `PhraseQuery` given a list of terms.
    ///
    /// There must be at least two terms, and all terms
    /// must belong to the same field.
    /// Offset for each term will be same as index in the Vector
    pub fn new(terms: Vec<Term>) -> PhraseQuery {
        let terms_with_offset = terms.into_iter().enumerate().collect();
        PhraseQuery::new_with_offset(terms_with_offset)
    }

    /// Creates a new `PhraseQuery` given a list of terms and their offsets.
    ///
    /// Can be used to provide custom offset for each term.
    pub fn new_with_offset(terms: Vec<(usize, Term)>) -> PhraseQuery {
        PhraseQuery::new_with_offset_and_slop(terms, 0)
    }

    /// Creates a new `PhraseQuery` given a list of terms, their offsets and a slop
    pub fn new_with_offset_and_slop(mut terms: Vec<(usize, Term)>, slop: u32) -> PhraseQuery {
        assert!(
            terms.len() > 1,
            "A phrase query is required to have strictly more than one term."
        );
        terms.sort_by_key(|&(offset, _)| offset);
        let field = terms[0].1.field();
        assert!(
            terms[1..].iter().all(|term| term.1.field() == field),
            "All terms from a phrase query must belong to the same field"
        );
        PhraseQuery {
            field,
            phrase_terms: terms,
            slop,
        }
    }

    /// Slop allowed for the phrase.
    ///
    /// The query will match if its terms are separated by `slop` terms at most.
    /// The slop can be considered a budget between all terms.
    /// E.g. "A B C" with slop 1 allows "A X B C", "A B X C", but not "A X B X C".
    ///
    /// Transposition costs 2, e.g. "A B" with slop 1 will not match "B A" but it would with slop 2
    /// Transposition is not a special case, in the example above A is moved 1 position and B is
    /// moved 1 position, so the slop is 2.
    ///
    /// As a result slop works in both directions, so the order of the terms may changed as long as
    /// they respect the slop.
    ///
    /// By default the slop is 0 meaning query terms need to be adjacent.
    pub fn set_slop(&mut self, value: u32) {
        self.slop = value;
    }

    /// The [`Field`] this `PhraseQuery` is targeting.
    pub fn field(&self) -> Field {
        self.field
    }

    /// `Term`s in the phrase without the associated offsets.
    pub fn phrase_terms(&self) -> Vec<Term> {
        self.phrase_terms
            .iter()
            .map(|(_, term)| term.clone())
            .collect::<Vec<Term>>()
    }

    /// Returns the [`PhraseWeight`] for the given phrase query given a specific `searcher`.
    ///
    /// This function is the same as [`Query::weight()`] except it returns
    /// a specialized type [`PhraseWeight`] instead of a Boxed trait.
    pub(crate) fn phrase_weight(
        &self,
        enable_scoring: EnableScoring<'_>,
    ) -> crate::Result<PhraseWeight> {
        let schema = enable_scoring.schema();
        let field_entry = schema.get_field_entry(self.field);
        let has_positions = field_entry
            .field_type()
            .get_index_record_option()
            .map(IndexRecordOption::has_positions)
            .unwrap_or(false);
        if !has_positions {
            let field_name = field_entry.name();
            return Err(crate::TantivyError::SchemaError(format!(
                "Applied phrase query on field {field_name:?}, which does not have positions \
                 indexed"
            )));
        }
        let terms = self.phrase_terms();
        let bm25_weight_opt = match enable_scoring {
            EnableScoring::Enabled {
                statistics_provider,
                ..
            } => Some(Bm25Weight::for_terms(statistics_provider, &terms)?),
            EnableScoring::Disabled { .. } => None,
        };
        let mut weight = PhraseWeight::new(self.phrase_terms.clone(), bm25_weight_opt);
        if self.slop > 0 {
            weight.slop(self.slop);
        }
        Ok(weight)
    }
}

impl Query for PhraseQuery {
    /// Create the weight associated with a query.
    ///
    /// See [`Weight`].
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let phrase_weight = self.phrase_weight(enable_scoring)?;
        Ok(Box::new(phrase_weight))
    }

    fn single_document_evaluator(
        &self,
        context: SingleDocumentEvaluationContext<'_>,
    ) -> crate::Result<Box<dyn SingleDocumentEvaluator>> {
        if self.field.field_id() as usize >= context.schema().num_fields() {
            return Err(TantivyError::SchemaError(format!(
                "Field id {} does not exist in the schema",
                self.field.field_id()
            )));
        }
        let field_entry = context.schema().get_field_entry(self.field);
        let field_type = field_entry.field_type();
        let schema_record_option = field_type.index_record_option().ok_or_else(|| {
            TantivyError::SchemaError(format!("Field {:?} is not indexed.", field_entry.name()))
        })?;
        if !schema_record_option.has_positions() {
            return Err(TantivyError::SchemaError(format!(
                "Applied phrase query on field {:?}, which does not have positions indexed",
                field_entry.name()
            )));
        }
        for (_, term) in &self.phrase_terms {
            let record_option = downgrade_record_option_for_term(
                field_type,
                term,
                IndexRecordOption::WithFreqsAndPositions,
                schema_record_option,
            );
            if !record_option.has_positions() {
                return Err(TantivyError::UnsupportedQueryForSingleDocumentEvaluation(
                    format!("PhraseQuery term {term:?} does not have position postings"),
                ));
            }
        }

        let terms = self.phrase_terms();
        let bm25_weight = context
            .statistics_provider()
            .map(|statistics| -> crate::Result<Bm25Weight> {
                // Caller must supply valid BM25 statistics; see `single_document` module docs.
                Ok(Bm25Weight::for_terms(statistics, &terms)?.boost_by(context.boost()))
            })
            .transpose()?;
        let max_offset = self
            .phrase_terms
            .iter()
            .map(|(offset, _)| *offset)
            .max()
            .unwrap_or(0);
        Ok(Box::new(PhraseSingleDocumentEvaluator {
            phrase_terms: self.phrase_terms.clone(),
            max_offset,
            adjusted_positions: vec![Vec::new(); self.phrase_terms.len()],
            matcher: PhraseMatcher::new(self.phrase_terms.len(), self.slop),
            field: self.field,
            has_fieldnorms: field_entry.has_fieldnorms(),
            bm25_weight,
        }))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        for (_, term) in &self.phrase_terms {
            visitor(term, true);
        }
    }
}

struct PhraseSingleDocumentEvaluator {
    phrase_terms: Vec<(usize, Term)>,
    max_offset: usize,
    adjusted_positions: Vec<Vec<u32>>,
    matcher: PhraseMatcher,
    field: Field,
    has_fieldnorms: bool,
    bm25_weight: Option<Bm25Weight>,
}

impl SingleDocumentEvaluator for PhraseSingleDocumentEvaluator {
    fn evaluate_impl(
        &mut self,
        document: &dyn SingleDocument,
    ) -> crate::Result<DocumentEvaluation> {
        for (term_index, (offset, term)) in self.phrase_terms.iter().enumerate() {
            let Some(term_info) = document.term_info(term) else {
                return Ok(DocumentEvaluation::NoMatch);
            };
            validate_term_info(term, term_info.term_freq)?;
            let positions = term_info.positions.ok_or_else(|| {
                TantivyError::InvalidArgument(format!(
                    "SingleDocument did not supply positions for {term:?}"
                ))
            })?;
            if positions.len() != term_info.term_freq as usize {
                return Err(TantivyError::InvalidArgument(format!(
                    "SingleDocument supplied {} positions for term frequency {} and term {term:?}",
                    positions.len(),
                    term_info.term_freq
                )));
            }
            if positions.windows(2).any(|window| window[0] > window[1]) {
                return Err(TantivyError::InvalidArgument(format!(
                    "SingleDocument positions are not sorted for {term:?}"
                )));
            }

            let position_offset = u32::try_from(self.max_offset - offset).map_err(|_| {
                TantivyError::InvalidArgument("PhraseQuery offset exceeds u32::MAX".to_string())
            })?;
            let adjusted = &mut self.adjusted_positions[term_index];
            adjusted.clear();
            adjusted.reserve(positions.len());
            for position in positions {
                adjusted.push(position.checked_add(position_offset).ok_or_else(|| {
                    TantivyError::InvalidArgument(
                        "PhraseQuery adjusted position exceeds u32::MAX".to_string(),
                    )
                })?);
            }
        }

        let adjusted_positions = &self.adjusted_positions;
        let load_positions = |index: usize, output: &mut Vec<u32>| {
            output.clear();
            output.extend_from_slice(&adjusted_positions[index]);
        };
        let Some(bm25_weight) = &self.bm25_weight else {
            return if self.matcher.phrase_exists(load_positions) {
                Ok(DocumentEvaluation::Match(1.0))
            } else {
                Ok(DocumentEvaluation::NoMatch)
            };
        };

        let phrase_count = self.matcher.phrase_count(load_positions);
        if phrase_count == 0 {
            return Ok(DocumentEvaluation::NoMatch);
        }
        let fieldnorm_id = if self.has_fieldnorms {
            document.fieldnorm_id(self.field).ok_or_else(|| {
                TantivyError::InvalidArgument(format!(
                    "SingleDocument did not supply a fieldnorm for field {:?}",
                    self.field
                ))
            })?
        } else {
            // Segment scorers use `FieldNormReader::constant(_, 1)` when fieldnorms are disabled;
            // `fieldnorm_to_id(1)` is also 1.
            1
        };
        let score = bm25_weight.score(fieldnorm_id, phrase_count);
        Ok(DocumentEvaluation::Match(score))
    }

    fn required_fields(&self) -> Option<&[Field]> {
        Some(std::slice::from_ref(&self.field))
    }
}
