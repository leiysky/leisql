use sqlparser::ast::{self, Expr, Function, FunctionArgExpr, Ident};

use super::{bind_context::BindContext, scope::Scope, ScalarExpr};
use crate::core::{Datum, ErrorKind, SQLError};

pub fn bind_scalar(
    ctx: &mut BindContext,
    scope: &Scope,
    expr: &Expr,
) -> Result<ScalarExpr, SQLError> {
    if let Some(scalar) = scope.resolve_expr(expr) {
        return Ok(scalar);
    }

    match expr {
        Expr::Identifier(ident) => bind_ident(ctx, scope, &[ident.clone()]),
        Expr::CompoundIdentifier(idents) => bind_ident(ctx, scope, idents),
        Expr::Value(literal) => bind_literal(literal),
        Expr::Function(func) => bind_function(ctx, scope, func),

        Expr::BinaryOp { left, op, right } => bind_binary_op(ctx, scope, left, op, right),

        _ => unimplemented!(),
    }
}

pub fn bind_ident(
    _ctx: &mut BindContext,
    scope: &Scope,
    qualified_ident: &[Ident],
) -> Result<ScalarExpr, SQLError> {
    if let Some(column) = scope.resolve_column(qualified_ident)? {
        let expr = ScalarExpr::Column(column);
        Ok(expr)
    } else {
        Err(SQLError::new(
            ErrorKind::PlannerError,
            format!(
                "column not found: {}",
                qualified_ident
                    .iter()
                    .map(Ident::to_string)
                    .collect::<Vec<_>>()
                    .join(".")
            ),
        ))
    }
}

pub fn bind_literal(literal: &ast::Value) -> Result<ScalarExpr, SQLError> {
    Ok(ScalarExpr::Literal(Datum::try_from(literal)?))
}

pub fn bind_function(
    ctx: &mut BindContext,
    scope: &Scope,
    func: &Function,
) -> Result<ScalarExpr, SQLError> {
    let args = func
        .args
        .iter()
        .map(|arg| match arg {
            ast::FunctionArg::Unnamed(arg) => match arg {
                FunctionArgExpr::Expr(arg) => bind_scalar(ctx, scope, arg),
                _ => unimplemented!(),
            },
            ast::FunctionArg::Named { .. } => unimplemented!(),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let func = ScalarExpr::FunctionCall(func.name.to_string(), args);

    Ok(func)
}

pub fn bind_aggregate_function(
    ctx: &mut BindContext,
    scope: &Scope,
    func: &Function,
) -> Result<(String, Vec<ScalarExpr>), SQLError> {
    if func.distinct {
        unimplemented!();
    }

    if func.name.to_string().to_lowercase() == "count" {
        if func.args.len() > 1 {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                "cannot find function count with given arguments",
            ));
        }

        if let Some(arg) = func.args.get(0) {
            match arg {
                ast::FunctionArg::Unnamed(arg) => match arg {
                    // Rewrite count(*) to count()
                    FunctionArgExpr::Wildcard => return Ok(("count".to_string(), vec![])),
                    _ => unimplemented!(),
                },
                _ => unimplemented!(),
            }
        }

        return Ok(("count".to_string(), vec![]));
    }

    let args = func
        .args
        .iter()
        .map(|arg| match arg {
            ast::FunctionArg::Unnamed(arg) => match arg {
                FunctionArgExpr::Expr(arg) => bind_scalar(ctx, scope, arg),
                _ => unimplemented!(),
            },
            ast::FunctionArg::Named { .. } => unimplemented!(),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok((func.name.to_string(), args))
}

fn bind_binary_op(
    ctx: &mut BindContext,
    scope: &Scope,
    left: &Expr,
    op: &ast::BinaryOperator,
    right: &Expr,
) -> Result<ScalarExpr, SQLError> {
    let func_name = match op {
        ast::BinaryOperator::Plus => "+",
        ast::BinaryOperator::Minus => "-",
        ast::BinaryOperator::Gt => ">",
        ast::BinaryOperator::Lt => "<",
        ast::BinaryOperator::GtEq => ">=",
        ast::BinaryOperator::LtEq => "<=",
        ast::BinaryOperator::Eq => "=",
        ast::BinaryOperator::NotEq => "<>",
        _ => unimplemented!(),
    };

    let left = bind_scalar(ctx, scope, left)?;
    let right = bind_scalar(ctx, scope, right)?;

    let func = ScalarExpr::FunctionCall(func_name.to_string(), vec![left, right]);

    Ok(func)
}
