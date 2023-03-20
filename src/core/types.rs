use sqlparser::ast::DataType;

use super::{ErrorKind, SQLError};

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Int,
    Float,
    String,
    Boolean,

    Null,

    /// Any is the top type, everything is a subtype of it.
    /// This is only used for type checking.
    Any,
    /// Never is the bottom type, nothing is a subtype of it.
    /// This is only used for type checking.
    Never,
}

impl TryFrom<&DataType> for Type {
    type Error = SQLError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Int(_)
            | DataType::Integer(_)
            | DataType::BigInt(_)
            | DataType::SmallInt(_)
            | DataType::TinyInt(_) => Ok(Type::Int),

            DataType::Varchar(_) | DataType::Char(_) | DataType::String => Ok(Type::String),

            DataType::Boolean => Ok(Type::Boolean),

            _ => Err(SQLError::new(
                ErrorKind::TypeError,
                format!("Unknown data type: {:?}", value),
            )),
        }
    }
}
