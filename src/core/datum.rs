use std::{fmt::Display, hash::Hash};

use enum_as_inner::EnumAsInner;
use sqlparser::ast;

use super::{ErrorKind, SQLError, Type};

/// A single datum value.
#[derive(Debug, Clone, EnumAsInner)]
pub enum Datum {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),

    Null,
}

impl Display for Datum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Datum::Int(v) => write!(f, "{}", v),
            Datum::Float(v) => write!(f, "{}", v),
            Datum::String(v) => write!(f, "{}", v),
            Datum::Boolean(v) => write!(f, "{}", if *v { "TRUE" } else { "FALSE" }),
            Datum::Null => write!(f, "NULL"),
        }
    }
}

impl TryFrom<&ast::Value> for Datum {
    type Error = SQLError;

    fn try_from(value: &ast::Value) -> Result<Self, Self::Error> {
        match value {
            ast::Value::Number(v, _) => {
                Ok(Datum::Int(v.parse().map_err(|e| {
                    SQLError::new(ErrorKind::ParseError, format!("{}", e))
                })?))
            }
            ast::Value::SingleQuotedString(v) => Ok(Datum::String(v.to_string())),
            ast::Value::Null => Ok(Datum::Null),
            _ => unimplemented!(),
        }
    }
}

impl Datum {
    pub fn typ(&self) -> Type {
        match self {
            Datum::Int(_) => Type::Int,
            Datum::Float(_) => Type::Float,
            Datum::String(_) => Type::String,
            Datum::Boolean(_) => Type::Boolean,
            Datum::Null => Type::Null,
        }
    }

    pub fn cast(&self, dest_typ: &Type) -> Self {
        match (self, dest_typ) {
            (Datum::Int(v), Type::Int) => Datum::Int(*v),
            (Datum::Int(v), Type::String) => Datum::String(v.to_string()),
            (Datum::Int(v), Type::Boolean) => Datum::Boolean(*v != 0),
            (Datum::Int(v), Type::Float) => Datum::Float(*v as f64),

            (Datum::String(v), Type::Int) => v.parse().map_or(Datum::Null, Datum::Int),
            (Datum::String(v), Type::Float) => v.parse().map_or(Datum::Null, Datum::Float),
            (Datum::String(v), Type::String) => Datum::String(v.to_string()),
            (Datum::String(v), Type::Boolean) => {
                let v = v.to_lowercase();
                if matches!(v.as_str(), "true" | "t") {
                    Datum::Boolean(true)
                } else if matches!(v.as_str(), "false" | "f") {
                    Datum::Boolean(false)
                } else {
                    Datum::Null
                }
            }

            (Datum::Null, _) => self.clone(),

            (Datum::Boolean(v), Type::Int) => Datum::Int(if *v { 1 } else { 0 }),
            (Datum::Boolean(_v), Type::Float) => Datum::Null,
            (Datum::Boolean(_v), Type::String) => Datum::String(self.to_string()),
            (Datum::Boolean(_), Type::Boolean) => self.clone(),

            (Datum::Float(v), Type::Int) => Datum::Int(*v as i64),
            (Datum::Float(_), Type::Float) => self.clone(),
            (Datum::Float(v), Type::String) => Datum::String(v.to_string()),
            (Datum::Float(v), Type::Boolean) => Datum::Boolean(*v != 0.0),

            _ => unreachable!(),
        }
    }
}

impl Hash for Datum {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Datum::Int(v) => v.hash(state),
            Datum::Float(v) => v.to_bits().hash(state),
            Datum::String(v) => v.hash(state),
            Datum::Boolean(v) => v.hash(state),
            // TODO: maybe we should use a different hash for null so
            // that it doesn't collide with other values
            Datum::Null => 0.hash(state),
        }
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(l0), Self::Int(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0.to_bits() == r0.to_bits(),
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Boolean(l0), Self::Boolean(r0)) => l0 == r0,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Datum {
    fn assert_receiver_is_total_eq(&self) {}
}
