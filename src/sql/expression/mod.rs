pub mod aggregate;
pub mod function;
pub mod type_check;

use std::sync::Arc;

use function::ScalarFunction;

use crate::core::{Datum, ErrorKind, SQLError, Tuple, Type};

#[derive(Clone)]
pub enum Expression {
    Column(usize, Type),
    Literal(Datum, Type),
    Function(Arc<ScalarFunction>, Vec<Expression>),
}

impl Expression {
    pub fn typ(&self) -> &Type {
        match self {
            Expression::Column(_, ty) => ty,
            Expression::Literal(_, ty) => ty,
            Expression::Function(func, _) => &func.ret_type,
        }
    }

    pub fn eval(&self, tuple: &Tuple) -> Result<Datum, SQLError> {
        match self {
            Expression::Column(index, _) => Ok(tuple.get(*index).ok_or_else(|| {
                SQLError::new(
                    ErrorKind::RuntimeError,
                    format!("cannot find column at index: {index}"),
                )
            })?),
            Expression::Literal(value, _) => Ok(value.clone()),
            Expression::Function(func, args) => {
                let args = args
                    .iter()
                    .map(|arg| arg.eval(tuple))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((func.eval)(args.as_slice()))
            }
        }
    }
}
