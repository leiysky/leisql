use std::sync::Arc;

use super::{
    aggregate::{AggregateFunction, AggregateFunctionRegistry},
    function::ScalarFunctionRegistry,
    Expression,
};
use crate::{
    core::{ErrorKind, SQLError, Type},
    sql::planner::{Column, ScalarExpr},
};

lazy_static! {
    static ref AUTO_CAST: Vec<(Type, Type)> = vec![
        (Type::Int, Type::Float),
        (Type::Int, Type::String),
        (Type::Int, Type::Boolean),

        (Type::Float, Type::Int),
        (Type::Float, Type::String),

        (Type::Boolean, Type::Int),
        (Type::Boolean, Type::String),

        // Null can be cast to any type
        (Type::Null, Type::Int),
        (Type::Null, Type::Float),
        (Type::Null, Type::Boolean),
        (Type::Null, Type::String),

        // Any type can be cast to Any
        (Type::Int, Type::Any),
        (Type::Float, Type::Any),
        (Type::Boolean, Type::Any),
        (Type::String, Type::Any),
    ];
}

pub fn can_auto_cast_to(from: &Type, to: &Type) -> bool {
    AUTO_CAST.contains(&(from.clone(), to.clone()))
}

pub trait ColumnTypeResolver {
    fn resolve_column_type(&self, column: &Column) -> Result<Type, SQLError>;
}

pub fn type_check<Ctxt: ColumnTypeResolver>(
    ctx: &Ctxt,
    scalar: &ScalarExpr,
) -> Result<Expression, SQLError> {
    match scalar {
        ScalarExpr::Column(column) => Ok(Expression::Column(
            column.index,
            ctx.resolve_column_type(column)?,
        )),
        ScalarExpr::Literal(value) => Ok(Expression::Literal(value.clone(), value.typ())),
        ScalarExpr::FunctionCall(func, args) => {
            let args = args
                .iter()
                .map(|arg| type_check(ctx, arg))
                .collect::<Result<Vec<_>, _>>()?;

            let func = type_check_function(func, &args, ScalarFunctionRegistry::builtin())?;

            Ok(func)
        }
    }
}

fn type_check_function(
    name: &str,
    args: &[Expression],
    registry: &ScalarFunctionRegistry,
) -> Result<Expression, SQLError> {
    let candidates = registry.search_candidates(name);

    for candidate in candidates.iter() {
        if candidate.arg_types.len() != args.len() {
            continue;
        }

        // We may add some cast for arguments if auto cast is available
        let mut arguments = args.to_vec();

        let mut matched = true;
        for (i, arg) in args.iter().enumerate() {
            if candidate.arg_types[i] == Type::Any {
                continue;
            }

            if arg.typ() != &candidate.arg_types[i]
                && !can_auto_cast_to(arg.typ(), &candidate.arg_types[i])
            {
                matched = false;
                break;
            }
            // Wrap cast since there is auto cast rule
            arguments[i] = wrap_cast(arguments[i].clone(), candidate.arg_types[i].clone());
        }

        if matched {
            return Ok(Expression::Function(candidate.clone(), arguments));
        }
    }

    Err(SQLError::new(
        ErrorKind::CatalogError,
        format!(
            "cannot find overload of function with given types: {}",
            name
        ),
    ))
}

fn wrap_cast(expr: Expression, target_type: Type) -> Expression {
    let original_type = expr.typ();
    if original_type == &target_type {
        expr
    } else {
        let cast_func_name = match target_type {
            Type::Int => "to_int",
            Type::Float => "to_float",
            Type::String => "to_string",
            Type::Boolean => "to_boolean",
            _ => unreachable!(),
        };
        let func = ScalarFunctionRegistry::builtin().search_candidates(cast_func_name)[0].clone();
        Expression::Function(func, vec![expr])
    }
}

pub fn type_check_aggregate_function(
    name: &str,
    args: &[Expression],
    registry: &AggregateFunctionRegistry,
) -> Result<(Arc<AggregateFunction>, Vec<Expression>), SQLError> {
    let candidates = registry.search_candidates(name);

    for candidate in candidates.iter() {
        if candidate.arg_types.len() != args.len() {
            continue;
        }

        let mut matched = true;
        for (i, arg) in args.iter().enumerate() {
            if candidate.arg_types[i] == Type::Any {
                continue;
            }

            if arg.typ() != &candidate.arg_types[i] {
                matched = false;
                break;
            }
        }

        if matched {
            return Ok((candidate.clone(), args.to_vec()));
        }
    }

    Err(SQLError::new(
        ErrorKind::CatalogError,
        format!(
            "cannot find overload of function with given types: {}",
            name
        ),
    ))
}
