use std::fmt::Display;

use super::runtime::{DDLJob, DMLJob};
use crate::core::Datum;

pub mod aggregate;
pub mod bind_context;
pub mod binder;
pub mod scalar;
pub mod scope;

#[derive(Debug)]
pub struct QualifiedObjectName {
    pub names: Vec<String>,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub enum Plan {
    Get {
        schema_name: String,
        table_name: String,
    },
    Map {
        scalars: Vec<ScalarExpr>,
        input: Box<Plan>,
    },
    Project {
        projections: Vec<usize>,
        input: Box<Plan>,
    },
    Filter {
        predicate: ScalarExpr,
        input: Box<Plan>,
    },
    Join {
        // Cross join
        left: Box<Plan>,
        right: Box<Plan>,
    },
    Aggregate {
        group_by: Vec<ScalarExpr>,
        /// (agg_func_name, arguments)
        aggregates: Vec<(String, Vec<ScalarExpr>)>,
        input: Box<Plan>,
    },

    /// Data definition language (DDL)
    DDL(DDLJob),
    DML(DMLJob),
    Explain(String),
    Use(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub index: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarExpr {
    FunctionCall(String, Vec<ScalarExpr>),
    Column(Column),
    Literal(Datum),
}

impl Display for ScalarExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarExpr::FunctionCall(name, args) => write!(
                f,
                "{}({})",
                name,
                args.iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ScalarExpr::Column(col) => write!(f, "#{}", col.index),
            ScalarExpr::Literal(v) => write!(f, "{}", v),
        }
    }
}

impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        indent_format_plan(f, self, 0)
    }
}

const DEFAULT_FORMAT_INDENT_SIZE: usize = 4;

fn indent_format_plan(f: &mut std::fmt::Formatter, plan: &Plan, indent: usize) -> std::fmt::Result {
    let indent_str = " ".repeat(indent);
    match plan {
        Plan::Get {
            schema_name,
            table_name,
        } => write!(f, "{}Get: {}.{}", indent_str, schema_name, table_name),

        Plan::Map { scalars, input } => {
            write!(
                f,
                "{}Map: {}",
                indent_str,
                scalars
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;

            writeln!(f)?;

            indent_format_plan(f, input, indent + DEFAULT_FORMAT_INDENT_SIZE)
        }
        Plan::Project { projections, input } => {
            write!(
                f,
                "{}Project: {}",
                indent_str,
                projections
                    .iter()
                    .map(|v| format!("#{}", v))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            writeln!(f)?;

            indent_format_plan(f, input, indent + DEFAULT_FORMAT_INDENT_SIZE)
        }
        Plan::Filter { predicate, input } => {
            write!(f, "{}Filter: {}", indent_str, predicate)?;
            writeln!(f)?;

            indent_format_plan(f, input, indent + DEFAULT_FORMAT_INDENT_SIZE)
        }
        Plan::Join { left, right } => {
            write!(f, "{}Join: ", indent_str)?;
            writeln!(f)?;

            indent_format_plan(f, left, indent + DEFAULT_FORMAT_INDENT_SIZE)?;
            writeln!(f)?;

            indent_format_plan(f, right, indent + DEFAULT_FORMAT_INDENT_SIZE)
        }
        Plan::DDL(job) => {
            write!(
                f,
                "{}{}",
                indent_str,
                match job {
                    DDLJob::CreateSchema(_) => "CreateSchema",
                    DDLJob::DropSchemas(_) => "DropSchema",
                    DDLJob::CreateTable(_, _) => "CreateTable",
                    DDLJob::DropTables(_) => "DropTable",
                    DDLJob::ShowTables(_) => "ShowTables",
                }
            )
        }
        Plan::DML(job) => {
            write!(
                f,
                "{}{}",
                indent_str,
                match job {
                    DMLJob::Insert(_, _) => "Insert",
                }
            )
        }
        Plan::Explain(_) => write!(f, "{}Explain", indent_str),

        Plan::Aggregate {
            group_by,
            aggregates,
            input,
        } => {
            write!(
                f,
                "{}Aggregate: group_by: {}, aggregates: {}",
                indent_str,
                group_by
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                aggregates
                    .iter()
                    .map(|(name, args)| {
                        format!(
                            "{}({})",
                            name,
                            args.iter()
                                .map(|arg| arg.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            writeln!(f)?;

            indent_format_plan(f, input, indent + DEFAULT_FORMAT_INDENT_SIZE)
        }
        Plan::Use(_) => write!(f, "{}Use", indent_str),
    }
}
