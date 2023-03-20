pub mod context;

use log::info;
use pgwire::api::results::FieldInfo;
use sqlparser::ast::Statement;

use self::context::QueryContext;
use super::{parser::parse_sql, planner::binder::Binder, runtime::execute_plan};
use crate::core::{SQLError, Tuple};

/// Kind of SQL statement, used for Postgres protocol
pub enum SQLKind {
    Query,
    Execute,
}

pub struct QueryResult {
    pub fields: Vec<FieldInfo>,
    pub data: Vec<Tuple>,
    pub kind: SQLKind,
}

pub struct Session {
    ctx: QueryContext,
}

impl Session {
    pub fn new(ctx: QueryContext) -> Self {
        Self { ctx }
    }

    pub fn execute(&mut self, sql_text: &str) -> Result<QueryResult, SQLError> {
        info!("Executing SQL: {}", sql_text);

        let statement = parse_sql(sql_text)?;

        let kind = match statement {
            Statement::Query(_) => SQLKind::Query,
            _ => SQLKind::Execute,
        };

        let mut binder = Binder::new(&mut self.ctx);
        let (plan, scope) = binder.bind_statement(&statement)?;

        let result = execute_plan(&mut self.ctx, &plan)?;

        let field_infos = scope
            .variables
            .iter()
            .map(|variable| {
                FieldInfo::new(
                    variable.name.to_string(),
                    None,
                    None,
                    pgwire::api::Type::VARCHAR,
                    pgwire::api::results::FieldFormat::Text,
                )
            })
            .collect::<Vec<_>>();

        let result = QueryResult {
            fields: field_infos,
            data: result,
            kind,
        };

        Ok(result)
    }
}
