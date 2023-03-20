use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Function, Visitor};

use crate::{core::SQLError, sql::expression::aggregate::AggregateFunctionRegistry};

pub struct AggregateFunctionVisitor {
    pub aggregates: Vec<Function>,
    pub error: Option<SQLError>,
}

impl AggregateFunctionVisitor {
    pub fn new() -> Self {
        Self {
            aggregates: vec![],
            error: None,
        }
    }
}

impl Visitor for AggregateFunctionVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Function(func)
                if func.name.0.len() == 1
                    && AggregateFunctionRegistry::builtin()
                        .contains(&func.name.0[0].to_string()) =>
            {
                self.aggregates.push(func.clone());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}
