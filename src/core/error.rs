use std::{error::Error, fmt::Display};

#[derive(Clone, Debug)]
pub struct SQLError {
    pub kind: ErrorKind,
    pub message: String,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug)]
pub enum ErrorKind {
    ParseError,
    PlannerError,
    CatalogError,
    TypeError,
    RuntimeError,
    UnknownError,
}

impl Error for SQLError {}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::ParseError => write!(f, "Parse Error"),
            ErrorKind::PlannerError => write!(f, "Planner Error"),
            ErrorKind::CatalogError => write!(f, "Catalog Error"),
            ErrorKind::TypeError => write!(f, "Type Error"),
            ErrorKind::RuntimeError => write!(f, "Runtime Error"),
            ErrorKind::UnknownError => write!(f, "Unknown Error"),
        }
    }
}

impl Display for SQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl SQLError {
    pub fn new(kind: ErrorKind, message: impl AsRef<str>) -> Self {
        Self {
            kind,
            message: message.as_ref().to_string(),
        }
    }
}
