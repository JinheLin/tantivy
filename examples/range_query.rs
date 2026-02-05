use postcard::fixint::le;
use query_grammar::Occur;
use tantivy::collector::{DocSetCollector, TopDocs};
use tantivy::query::{BooleanQuery, Query, QueryParser, RangeQuery, TermQuery};
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, ReloadPolicy};
use tempfile::TempDir;

use anyhow::Result;
use std::ops::Bound;
use std::path::PathBuf;
use std::time::Instant;


    const FRAGS_PATH: &str = "/DATA/disk1/jinhelin/s_101";

fn get_frags(path: &str) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let entries = std::fs::read_dir(path)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn load_indexes() -> Result<Vec<Index>> {
    let frags = get_frags(FRAGS_PATH)?;
    let mut indexes = Vec::new();
    for frag in frags {
        let index_path = frag.join("index");
        let index = Index::open_in_dir(&index_path)?;
        indexes.push(index);
    }
    Ok(indexes)
}

const TOKEN0_ADDRESS: Field = Field::from_field_id(3);
const PLATFORM: Field = Field::from_field_id(4);
const ANCHOR: Field = Field::from_field_id(5);
const TS: Field = Field::from_field_id(9);

fn query0() -> Box<dyn Query> {
    let token0_address_term = Term::from_field_text(TOKEN0_ADDRESS, "112MeuMYHY9DYJaGNcVRcavmG4oPs5Zw7BuaVnrpump");
    let platform_term = Term::from_field_i64(PLATFORM, 16);

    let inner_query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(TermQuery::new(token0_address_term, IndexRecordOption::Basic))),
        (Occur::Must, Box::new(TermQuery::new(platform_term, IndexRecordOption::Basic))),
    ]);

    let anchor_term = Term::from_field_i64(ANCHOR, 0);
    let mid_query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(inner_query)),
        (Occur::Must, Box::new(TermQuery::new(anchor_term, IndexRecordOption::Basic))),
    ]);

    let ts_lower = Term::from_field_u64(TS, 1853179174378799104);
    let ts_range_lower = RangeQuery::new(
        Bound::Included(ts_lower),
        Bound::Unbounded,
    );

    let outer_query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(mid_query)),
        (Occur::Must, Box::new(ts_range_lower)),
    ]);

    let ts_upper = Term::from_field_u64(TS, 1853185771448565760);
    let ts_range_upper = RangeQuery::new(
        Bound::Unbounded,
        Bound::Included(ts_upper),
    );

    let final_query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(outer_query)),
        (Occur::Must, Box::new(ts_range_upper)),
    ]);

    Box::new(final_query)
}

fn query1() -> Box<dyn Query> {
    let token0_address_term = Term::from_field_text(TOKEN0_ADDRESS, "112MeuMYHY9DYJaGNcVRcavmG4oPs5Zw7BuaVnrpump");
    let platform_term = Term::from_field_i64(PLATFORM, 16);

    let inner_query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(TermQuery::new(token0_address_term, IndexRecordOption::Basic))),
        (Occur::Must, Box::new(TermQuery::new(platform_term, IndexRecordOption::Basic))),
    ]);

    let anchor_term = Term::from_field_i64(ANCHOR, 0);
    let mid_query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(inner_query)),
        (Occur::Must, Box::new(TermQuery::new(anchor_term, IndexRecordOption::Basic))),
    ]);

    let ts_lower = Term::from_field_u64(TS, 1853179174378799104);
    let ts_upper = Term::from_field_u64(TS, 1853185771448565760);
    let ts_range = RangeQuery::new(
        Bound::Included(ts_lower),
        Bound::Included(ts_upper),
    );

    let final_query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(mid_query)),
        (Occur::Must, Box::new(ts_range)),
    ]);

    Box::new(final_query)
}

fn query2() -> Box<dyn Query> {
    let token0_address_term = Term::from_field_text(TOKEN0_ADDRESS, "112MeuMYHY9DYJaGNcVRcavmG4oPs5Zw7BuaVnrpump");
    let platform_term = Term::from_field_i64(PLATFORM, 16);
    let anchor_term = Term::from_field_i64(ANCHOR, 0);


    let ts_lower = Term::from_field_u64(TS, 1853179174378799104);
    let ts_upper = Term::from_field_u64(TS, 1853185771448565760);
    let ts_range = RangeQuery::new(
        Bound::Included(ts_lower),
        Bound::Included(ts_upper),
    );

    let final_query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(TermQuery::new(token0_address_term, IndexRecordOption::Basic))),
        (Occur::Must, Box::new(TermQuery::new(platform_term, IndexRecordOption::Basic))),
        (Occur::Must, Box::new(TermQuery::new(anchor_term, IndexRecordOption::Basic))),
        (Occur::Must, Box::new(ts_range)),
    ]);

    Box::new(final_query)
}

fn main() -> Result<()> {

    let indexes = load_indexes()?;
    let schema = indexes[0].schema();
    println!("schema: {:?}", schema);

    let readers = indexes.iter().map(|index| index.reader()).collect::<Result<Vec<_>, _>>()?;
    let searchers = readers.iter().map(|reader| reader.searcher()).collect::<Vec<_>>();

    tracing_subscriber::fmt::init();

    let query = query0();
    let docs = searchers.iter().map(|searcher| {
        searcher.search(query.as_ref(), &DocSetCollector)
    }).collect::<Result<Vec<_>, _>>()?;
    let mut cost = Vec::with_capacity(searchers.len() * 100);
    let start = Instant::now();
    for _ in 0..100 {
        let _ = searchers.iter().map(|searcher| {
            let start = Instant::now();
            let docs = searcher.search(query.as_ref(), &DocSetCollector).unwrap();
            cost.push((docs.len(), start.elapsed()));
            docs
        }).collect::<Vec<_>>();
    }
    let duration = start.elapsed();
    println!("docs: {}, duration: {:?}, cost: {:?}", docs.len(), duration, cost);

    let query = query1();
    let docs = searchers.iter().map(|searcher| {
        searcher.search(query.as_ref(), &DocSetCollector)
    }).collect::<Result<Vec<_>, _>>()?;
    let start = Instant::now();
    for _ in 0..100 {
        let _ = searchers.iter().map(|searcher| {
            searcher.search(query.as_ref(), &DocSetCollector)
        }).collect::<Result<Vec<_>, _>>()?;
    }
    let duration = start.elapsed();
    println!("docs: {}, duration: {:?}", docs.len(), duration);

    let query = query2();
    let docs = searchers.iter().map(|searcher| {
        searcher.search(query.as_ref(), &DocSetCollector)
    }).collect::<Result<Vec<_>, _>>()?;
    let start = Instant::now();
    for _ in 0..100 {
        let _ = searchers.iter().map(|searcher| {
            searcher.search(query.as_ref(), &DocSetCollector)
        }).collect::<Result<Vec<_>, _>>()?;
    }
    let duration = start.elapsed();
    println!("docs: {}, duration: {:?}", docs.len(), duration);

    Ok(())
}
