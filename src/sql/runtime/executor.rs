use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use super::{DDLJob, DMLJob};
use crate::{
    core::{tuple::Tuple, Datum, ErrorKind, SQLError},
    sql::{
        expression::{
            aggregate::{AggregateFunction, AggregateState},
            Expression,
        },
        session::context::QueryContext,
    },
    storage::relation::ScanState,
};

#[allow(clippy::upper_case_acronyms)]
pub enum Executor {
    Project(ProjectExecutor),
    Filter(FilterExecutor),
    Map(MapExecutor),
    NestedLoopJoin(NestedLoopJoinExecutor),
    HashAggregate(HashAggregateExecutor),
    Scan(ScanExecutor),

    DDL(DDLExecutor),
    DML(DMLExecutor),
    Use(String),

    Values(ValuesExecutor),
}

/// Executor is responsible for executing a query plan.
/// It is implemented in a pull-based manner, i.e. Volcano-style.
/// The executor will be `open()`-ed, then `next()` will be called repeatedly
/// until it returns None, then `close()` will be called.
impl Executor {
    pub fn open(&mut self, ctx: &mut QueryContext) -> Result<(), SQLError> {
        match self {
            Executor::DDL(ddl_exec) => ddl_exec.open(ctx),
            Executor::DML(dml_exec) => dml_exec.open(ctx),
            Executor::NestedLoopJoin(nlj_exec) => nlj_exec.open(ctx),
            Executor::Use(schema_name) => {
                ctx.current_schema = schema_name.clone();
                Ok(())
            }
            _ => {
                for child in self.children_mut() {
                    child.open(ctx)?;
                }
                Ok(())
            }
        }
    }

    pub fn next(&mut self, ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        match self {
            Executor::Filter(filter_exec) => filter_exec.next(ctx),
            Executor::Map(map_exec) => map_exec.next(ctx),
            Executor::Project(project_exec) => project_exec.next(ctx),
            Executor::Scan(scan_exec) => scan_exec.next(ctx),
            Executor::DDL(ddl_exec) => ddl_exec.next(ctx),
            Executor::NestedLoopJoin(nlj_exec) => nlj_exec.next(ctx),
            Executor::HashAggregate(hash_aggr_exec) => hash_aggr_exec.next(ctx),
            Executor::Values(values_exec) => values_exec.next(ctx),
            _ => Ok(None),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn close(&mut self, ctx: &mut QueryContext) -> Result<(), SQLError> {
        {
            for child in self.children_mut() {
                child.close(ctx)?;
            }
            Ok(())
        }
    }

    pub fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Executor> + '_> {
        match self {
            Executor::Project(project_exec) => {
                Box::new(std::iter::once(project_exec.child.as_mut()))
            }
            Executor::Filter(filter_exec) => Box::new(std::iter::once(filter_exec.child.as_mut())),
            Executor::Map(map_exec) => Box::new(std::iter::once(map_exec.child.as_mut())),
            Executor::NestedLoopJoin(nlj_exec) => Box::new(
                Box::new(std::iter::once(nlj_exec.outer_table.as_mut()))
                    .chain(Box::new(std::iter::once(nlj_exec.inner_table.as_mut()))),
            ),
            Executor::HashAggregate(hash_aggr_exec) => {
                Box::new(std::iter::once(hash_aggr_exec.input_executor.as_mut()))
            }

            Executor::Use(_)
            | Executor::Values(_)
            | Executor::Scan(_)
            | Executor::DML(_)
            | Executor::DDL(_) => Box::new(std::iter::empty()),
        }
    }
}

pub struct ValuesExecutor {
    pub values: VecDeque<Tuple>,
}

impl ValuesExecutor {
    pub fn new(values: Vec<Tuple>) -> Self {
        Self {
            values: values.into(),
        }
    }

    pub fn next(&mut self, _ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        Ok(self.values.pop_front())
    }
}

pub struct ScanExecutor {
    schema_name: String,
    table_name: String,
    scan_state: ScanState,
}

impl ScanExecutor {
    pub fn new(schema_name: &str, table_name: &str) -> Self {
        Self {
            scan_state: ScanState::default(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
        }
    }

    pub fn next(&mut self, ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        let table = ctx
            .storage_mgr
            .get_relation(&self.schema_name, &self.table_name)
            .ok_or_else(|| SQLError::new(ErrorKind::UnknownError, "cannot find storage"))?;
        let tuple = table.scan(&mut self.scan_state);
        Ok(tuple)
    }
}

pub struct ProjectExecutor {
    pub child: Box<Executor>,
    pub projections: Vec<usize>,
}

impl ProjectExecutor {
    pub fn new(child: Box<Executor>, projections: Vec<usize>) -> Self {
        Self { child, projections }
    }

    pub fn next(&mut self, ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        let tuple = self.child.next(ctx)?;
        Ok(tuple.map(|tuple| tuple.project(&self.projections)))
    }
}

pub struct FilterExecutor {
    pub child: Box<Executor>,
    pub predicate: Box<dyn Fn(Tuple) -> bool>,
}

impl FilterExecutor {
    pub fn new(child: Box<Executor>, predicate: Box<dyn Fn(Tuple) -> bool>) -> Self {
        Self { child, predicate }
    }

    pub fn next(&mut self, ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        loop {
            let tuple = self.child.next(ctx)?;
            if let Some(tuple) = tuple {
                if (self.predicate)(tuple.clone()) {
                    return Ok(Some(tuple));
                }
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct MapExecutor {
    pub child: Box<Executor>,
    pub map_fn: Box<dyn Fn(Tuple) -> Tuple>,
}

impl MapExecutor {
    pub fn new(child: Box<Executor>, map_fn: Box<dyn Fn(Tuple) -> Tuple>) -> Self {
        Self { child, map_fn }
    }

    pub fn next(&mut self, ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        let tuple = self.child.next(ctx)?;
        Ok(tuple.map(|tuple| (self.map_fn)(tuple)))
    }
}

struct NestedLoopJoinState {
    inner_tuples: Vec<Tuple>,
    inner_tuple_idx: usize,
    outer_tuple: Option<Tuple>,
}

/// Nested-loop join executor.
/// We will always choose the right table to be the inner table,
/// since we will generate a left-deep tree for the join plan.
///
/// So the outer table probing can be pipelined.
pub struct NestedLoopJoinExecutor {
    /// Inner table is the table to be rescanned for each tuple of the outer table.
    pub inner_table: Box<Executor>,
    /// Outer table is the table that is iterated over.
    pub outer_table: Box<Executor>,

    /// State of the nested-loop join executor.
    /// Will be initialized when the executor is opened.
    state: Option<NestedLoopJoinState>,
}

impl NestedLoopJoinExecutor {
    pub fn new(inner_table: Box<Executor>, outer_table: Box<Executor>) -> NestedLoopJoinExecutor {
        NestedLoopJoinExecutor {
            inner_table,
            outer_table,
            state: None,
        }
    }

    pub fn open(&mut self, ctx: &mut QueryContext) -> Result<(), SQLError> {
        self.inner_table.open(ctx)?;
        self.outer_table.open(ctx)?;

        let mut state = NestedLoopJoinState {
            inner_tuples: vec![],
            inner_tuple_idx: 0,
            outer_tuple: None,
        };

        // Drain the inner table.
        while let Some(tuple) = self.inner_table.next(ctx)? {
            state.inner_tuples.push(tuple);
        }

        self.state = Some(state);
        Ok(())
    }

    pub fn next(&mut self, ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        let state = self.state.as_mut().unwrap();

        loop {
            if state.inner_tuples.is_empty() {
                return Ok(None);
            }

            if state.outer_tuple.is_none() {
                // Try to get the next outer tuple.
                let outer_tuple = self.outer_table.next(ctx)?;
                if let Some(outer_tuple) = outer_tuple {
                    state.outer_tuple = Some(outer_tuple);
                } else {
                    return Ok(None);
                }
            }

            if state.inner_tuple_idx == state.inner_tuples.len() {
                // Join of previous outer tuple is done.
                state.inner_tuple_idx = 0;
                state.outer_tuple = None;
                continue;
            }

            let inner_tuple = state.inner_tuples[state.inner_tuple_idx].clone();
            state.inner_tuple_idx += 1;
            return Ok(Some(Self::combine_tuple(
                state.outer_tuple.clone().unwrap(),
                inner_tuple,
            )));
        }
    }

    fn combine_tuple(left_tuple: Tuple, right_tuple: Tuple) -> Tuple {
        let mut combined_tuple = left_tuple;
        combined_tuple.values.extend(right_tuple.values);
        combined_tuple
    }
}

#[derive(Default)]
struct HashAggregateState {
    hash_table: HashMap<Vec<Datum>, Vec<AggregateState>>,
    /// A single group is used for scalar aggregates.
    single_group: Option<Vec<AggregateState>>,
    result_tuples: Option<VecDeque<Tuple>>,
}

pub struct HashAggregateExecutor {
    pub group_by: Vec<Expression>,
    pub aggregates: Vec<(Arc<AggregateFunction>, Vec<Expression>)>,
    pub input_executor: Box<Executor>,

    state: HashAggregateState,
}

impl HashAggregateExecutor {
    pub fn new(
        input: Box<Executor>,
        group_by: Vec<Expression>,
        aggregates: Vec<(Arc<AggregateFunction>, Vec<Expression>)>,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            state: HashAggregateState::default(),
            input_executor: input,
        }
    }

    pub fn next(&mut self, ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        // At the first run we drain the input executor and build the hash table.
        while let Some(tuple) = self.input_executor.next(ctx)? {
            let hash_key = self
                .group_by
                .iter()
                .map(|expr| expr.eval(&tuple))
                .collect::<Result<Vec<_>, _>>()?;

            let aggregate_states = if self.group_by.is_empty() {
                self.state.single_group.get_or_insert_with(|| {
                    self.aggregates
                        .iter()
                        .map(|(agg, _)| agg.default_state.clone())
                        .collect()
                })
            } else {
                self.state.hash_table.entry(hash_key).or_insert_with(|| {
                    self.aggregates
                        .iter()
                        .map(|(agg, _)| agg.default_state.clone())
                        .collect()
                })
            };

            for (i, (agg, args)) in self.aggregates.iter().enumerate() {
                let arg_values = args
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()?;

                // Accumulate current tuple into the aggregate state.
                aggregate_states[i] = (agg.accumulate)(&arg_values, &aggregate_states[i]);
            }
        }

        // Hash table is finished, we can start to produce the result tuples.
        if self.state.result_tuples.is_none() {
            let mut result_tuples = VecDeque::new();

            if self.group_by.is_empty() {
                let mut result_tuple = Tuple::default();
                let aggregate_states = self.state.single_group.get_or_insert_with(|| {
                    self.aggregates
                        .iter()
                        .map(|(agg, _)| agg.default_state.clone())
                        .collect()
                });

                // Add aggregate function result to result tuple
                result_tuple
                    .values
                    .extend(aggregate_states.iter().map(|s| s.finalize()));

                result_tuples.push_back(result_tuple);
            } else {
                for (hash_key, aggregate_states) in self.state.hash_table.iter() {
                    let mut result_tuple = Tuple::default();

                    // Add group keys to result tuple
                    result_tuple.values.extend(hash_key.iter().cloned());
                    // Add aggregate function result to result tuple
                    result_tuple
                        .values
                        .extend(aggregate_states.iter().map(|s| s.finalize()));

                    result_tuples.push_back(result_tuple);
                }
            }

            self.state.result_tuples = Some(result_tuples);
        }

        Ok(self.state.result_tuples.as_mut().unwrap().pop_front())
    }
}

pub struct DDLExecutor {
    pub job: DDLJob,
    pub result_buffer: VecDeque<Tuple>,
}

impl DDLExecutor {
    pub fn new(job: DDLJob) -> Self {
        Self {
            job,
            result_buffer: VecDeque::new(),
        }
    }

    pub fn open(&mut self, ctx: &mut QueryContext) -> Result<(), SQLError> {
        match &self.job {
            DDLJob::CreateSchema(schema_name) => {
                ctx.catalog.create_schema(schema_name)?;
            }
            DDLJob::DropSchemas(names) => {
                for name in names.iter() {
                    ctx.catalog.drop_schema(name)?;
                }
            }
            DDLJob::CreateTable(schema_name, table_def) => {
                ctx.catalog.create_table(schema_name.as_str(), table_def)?;
                ctx.storage_mgr
                    .create_relation(schema_name, &table_def.name);
            }
            DDLJob::DropTables(names) => {
                for (schema_name, table_name) in names.iter() {
                    ctx.catalog.drop_table(schema_name, table_name)?;
                    ctx.storage_mgr.drop_relation(schema_name, table_name);
                }
            }
            DDLJob::ShowTables(schema_name) => {
                let tables = ctx.catalog.list_tables(schema_name)?;
                self.result_buffer.extend(tables.iter().map(|table| {
                    let mut tuple = Tuple::default();
                    tuple.append(Datum::String(table.clone()));
                    tuple
                }));
            }
        }

        Ok(())
    }

    pub fn next(&mut self, _ctx: &mut QueryContext) -> Result<Option<Tuple>, SQLError> {
        Ok(self.result_buffer.pop_front())
    }
}

pub struct DMLExecutor {
    pub job: DMLJob,
    pub result_buffer: VecDeque<Tuple>,
}

impl DMLExecutor {
    pub fn new(job: DMLJob) -> Self {
        Self {
            job,
            result_buffer: VecDeque::new(),
        }
    }

    pub fn open(&mut self, ctx: &mut QueryContext) -> Result<(), SQLError> {
        match &self.job {
            DMLJob::Insert((schema_name, table_name), insert_data) => {
                let table = ctx
                    .storage_mgr
                    .get_relation_mut(schema_name, table_name)
                    .ok_or_else(|| SQLError::new(ErrorKind::UnknownError, "cannot find storage"))?;
                for tuple in insert_data {
                    table.insert(tuple.clone());
                }
            }
        }

        Ok(())
    }
}
