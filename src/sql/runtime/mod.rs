pub mod builder;
mod ddl;
pub mod dml;
pub mod executor;

pub use ddl::*;
pub use dml::*;

use self::builder::ExecutorBuilder;
use super::{planner::Plan, session::context::QueryContext};
use crate::core::{SQLError, Tuple};

pub fn execute_plan(ctx: &mut QueryContext, plan: &Plan) -> Result<Vec<Tuple>, SQLError> {
    let mut executor = ExecutorBuilder::new(ctx).build(plan)?;

    executor.open(ctx)?;

    let mut result = vec![];
    while let Some(tuple) = executor.next(ctx)? {
        result.push(tuple);
    }

    executor.close(ctx)?;

    Ok(result)
}
