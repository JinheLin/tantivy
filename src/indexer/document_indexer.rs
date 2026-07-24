use columnar::MonotonicallyMappableToU64;
use common::JsonPathWriter;
use itertools::Itertools;
use tokenizer_api::BoxTokenStream;

use crate::core::json_utils::index_json_values;
use crate::postings::{IndexingContext, IndexingPosition, PostingsWriter};
use crate::schema::document::{Document, ReferenceValue, Value};
use crate::schema::{Field, FieldEntry, FieldType, Schema, Term, DATE_TIME_PRECISION_INDEXED};
use crate::tokenizer::{
    FacetTokenizer, PreTokenizedStream, TextAnalyzer, Tokenizer, TokenizerManager,
};
use crate::{DocId, TantivyError};

/// Receives the postings and fieldnorms emitted while indexing a document.
pub(crate) trait DocumentIndexingOutput {
    fn postings_writer(&mut self, field: Field) -> &mut dyn PostingsWriter;

    fn record_fieldnorm(&mut self, doc: DocId, field: Field, fieldnorm: u32);
}

#[derive(Clone, Copy)]
pub(crate) enum DocumentIndexingMode<'a> {
    /// Index every document field with the regular indexing semantics.
    Normal,
    /// Index only the sorted query field set and ignore its top-level Null values.
    QueryAware(&'a [Field]),
}

impl DocumentIndexingMode<'_> {
    fn should_index_field(self, field: Field) -> bool {
        match self {
            Self::Normal => true,
            Self::QueryAware(fields) => fields.binary_search(&field).is_ok(),
        }
    }

    fn should_index_field_value(self, field: Field, is_null: bool) -> bool {
        match self {
            Self::Normal => true,
            Self::QueryAware(fields) => fields.binary_search(&field).is_ok() && !is_null,
        }
    }
}

pub(crate) fn text_analyzers_for_schema(
    schema: &Schema,
    tokenizer_manager: &TokenizerManager,
    mode: DocumentIndexingMode<'_>,
) -> crate::Result<Vec<TextAnalyzer>> {
    schema
        .fields()
        .map(|(field, field_entry): (_, &FieldEntry)| {
            if !mode.should_index_field(field) {
                return Ok(TextAnalyzer::default());
            }
            let text_options = match field_entry.field_type() {
                FieldType::Str(text_options) => text_options.get_indexing_options(),
                FieldType::JsonObject(json_object_options) => {
                    json_object_options.get_text_indexing_options()
                }
                _ => None,
            };
            let tokenizer_name = text_options
                .map(|text_index_option| text_index_option.tokenizer())
                .unwrap_or("default");

            tokenizer_manager.get(tokenizer_name).ok_or_else(|| {
                TantivyError::SchemaError(format!(
                    "Error getting tokenizer for field: {}",
                    field_entry.name()
                ))
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn index_document<D: Document>(
    doc_id: DocId,
    doc: &D,
    schema: &Schema,
    mode: DocumentIndexingMode<'_>,
    per_field_text_analyzers: &mut [TextAnalyzer],
    term_buffer: &mut Term,
    json_path_writer: &mut JsonPathWriter,
    ctx: &mut IndexingContext,
    output: &mut impl DocumentIndexingOutput,
) -> crate::Result<()> {
    // TODO: Can this be optimised a bit?
    let vals_grouped_by_field = doc
        .iter_fields_and_values()
        .filter(|(field, value)| mode.should_index_field_value(*field, value.is_null()))
        .sorted_by_key(|(field, _)| *field)
        .group_by(|(field, _)| *field);

    for (field, field_values) in &vals_grouped_by_field {
        let values = field_values.map(|el| el.1);

        let field_entry = schema.get_field_entry(field);
        let make_schema_error = || {
            TantivyError::SchemaError(format!(
                "Expected a {:?} for field {:?}",
                field_entry.field_type().value_type(),
                field_entry.name()
            ))
        };
        if !field_entry.is_indexed() {
            continue;
        }

        term_buffer.clear_with_field_and_type(field_entry.field_type().value_type(), field);

        match field_entry.field_type() {
            FieldType::Facet(_) => {
                let mut facet_tokenizer = FacetTokenizer::default();
                let postings_writer = output.postings_writer(field);
                for value_access in values {
                    let value = value_access as D::Value<'_>;

                    let facet = value.as_facet().ok_or_else(make_schema_error)?;
                    let facet_str = facet.encoded_str();
                    let mut facet_tokenizer = facet_tokenizer.token_stream(facet_str);
                    let mut indexing_position = IndexingPosition::default();
                    postings_writer.index_text(
                        doc_id,
                        &mut facet_tokenizer,
                        term_buffer,
                        ctx,
                        &mut indexing_position,
                    );
                }
            }
            FieldType::Str(_) => {
                let mut indexing_position = IndexingPosition::default();
                {
                    let postings_writer = output.postings_writer(field);
                    for value_access in values {
                        let value = value_access as D::Value<'_>;

                        let mut token_stream = if let Some(text) = value.as_str() {
                            let text_analyzer =
                                &mut per_field_text_analyzers[field.field_id() as usize];
                            text_analyzer.token_stream(text)
                        } else if let Some(tok_str) = value.as_pre_tokenized_text() {
                            BoxTokenStream::new(PreTokenizedStream::from(tok_str.clone()))
                        } else {
                            continue;
                        };

                        assert!(term_buffer.is_empty());
                        postings_writer.index_text(
                            doc_id,
                            &mut *token_stream,
                            term_buffer,
                            ctx,
                            &mut indexing_position,
                        );
                    }
                }
                if field_entry.has_fieldnorms() {
                    output.record_fieldnorm(doc_id, field, indexing_position.num_tokens);
                }
            }
            FieldType::U64(_) => {
                let mut num_vals = 0;
                {
                    let postings_writer = output.postings_writer(field);
                    for value_access in values {
                        let value = value_access as D::Value<'_>;

                        num_vals += 1;
                        let u64_val = value.as_u64().ok_or_else(make_schema_error)?;
                        term_buffer.set_u64(u64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                if field_entry.has_fieldnorms() {
                    output.record_fieldnorm(doc_id, field, num_vals);
                }
            }
            FieldType::Date(_) => {
                let mut num_vals = 0;
                {
                    let postings_writer = output.postings_writer(field);
                    for value_access in values {
                        let value_access = value_access as D::Value<'_>;
                        let value = value_access.as_value();

                        num_vals += 1;
                        let date_val = value.as_datetime().ok_or_else(make_schema_error)?;
                        term_buffer
                            .set_u64(date_val.truncate(DATE_TIME_PRECISION_INDEXED).to_u64());
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                if field_entry.has_fieldnorms() {
                    output.record_fieldnorm(doc_id, field, num_vals);
                }
            }
            FieldType::I64(_) => {
                let mut num_vals = 0;
                {
                    let postings_writer = output.postings_writer(field);
                    for value_access in values {
                        let value = value_access as D::Value<'_>;

                        num_vals += 1;
                        let i64_val = value.as_i64().ok_or_else(make_schema_error)?;
                        term_buffer.set_i64(i64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                if field_entry.has_fieldnorms() {
                    output.record_fieldnorm(doc_id, field, num_vals);
                }
            }
            FieldType::F64(_) => {
                let mut num_vals = 0;
                {
                    let postings_writer = output.postings_writer(field);
                    for value_access in values {
                        let value = value_access as D::Value<'_>;

                        num_vals += 1;
                        let f64_val = value.as_f64().ok_or_else(make_schema_error)?;
                        term_buffer.set_f64(f64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                if field_entry.has_fieldnorms() {
                    output.record_fieldnorm(doc_id, field, num_vals);
                }
            }
            FieldType::Bool(_) => {
                let mut num_vals = 0;
                {
                    let postings_writer = output.postings_writer(field);
                    for value_access in values {
                        let value = value_access as D::Value<'_>;

                        num_vals += 1;
                        let bool_val = value.as_bool().ok_or_else(make_schema_error)?;
                        term_buffer.set_bool(bool_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                if field_entry.has_fieldnorms() {
                    output.record_fieldnorm(doc_id, field, num_vals);
                }
            }
            FieldType::Bytes(_) => {
                let mut num_vals = 0;
                {
                    let postings_writer = output.postings_writer(field);
                    for value_access in values {
                        let value = value_access as D::Value<'_>;

                        num_vals += 1;
                        let bytes = value.as_bytes().ok_or_else(make_schema_error)?;
                        term_buffer.set_bytes(bytes);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                if field_entry.has_fieldnorms() {
                    output.record_fieldnorm(doc_id, field, num_vals);
                }
            }
            FieldType::JsonObject(json_options) => {
                let text_analyzer = &mut per_field_text_analyzers[field.field_id() as usize];
                let json_values_it = values.map(|value_access| {
                    let value_access = value_access as D::Value<'_>;
                    let value = value_access.as_value();

                    match value {
                        ReferenceValue::Object(object_iter) => Ok(object_iter),
                        _ => Err(make_schema_error()),
                    }
                });
                let postings_writer = output.postings_writer(field);
                index_json_values::<D::Value<'_>>(
                    doc_id,
                    json_values_it,
                    text_analyzer,
                    json_options.is_expand_dots_enabled(),
                    term_buffer,
                    postings_writer,
                    json_path_writer,
                    ctx,
                )?;
            }
            FieldType::IpAddr(_) => {
                let mut num_vals = 0;
                {
                    let postings_writer = output.postings_writer(field);
                    for value_access in values {
                        let value = value_access as D::Value<'_>;

                        num_vals += 1;
                        let ip_addr = value.as_ip_addr().ok_or_else(make_schema_error)?;
                        term_buffer.set_ip_addr(ip_addr);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                }
                if field_entry.has_fieldnorms() {
                    output.record_fieldnorm(doc_id, field, num_vals);
                }
            }
        }
    }
    Ok(())
}
