use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use crate::core::{ErrorKind, SQLError};

/// Parse SQL string into AST
pub fn parse_sql(sql_text: &str) -> Result<Statement, SQLError> {
    let parser = Parser::new(&PostgreSqlDialect {});

    let statement = parser
        .try_with_sql(sql_text)
        .and_then(|mut parser| parser.parse_statement())
        .map_err(|e| SQLError::new(ErrorKind::ParseError, e.to_string()))?;

    Ok(statement)
}
