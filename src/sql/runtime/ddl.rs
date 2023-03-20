use crate::catalog::defs::TableDefinition;

#[derive(Debug, Clone)]
pub enum DDLJob {
    /// Create schema with the given name.
    CreateSchema(String),
    /// Drop schema with the given name.
    DropSchemas(Vec<String>),
    /// Create table with the given definition.
    CreateTable(String, TableDefinition),
    /// Drop table with the given name (schema_name, table_name).
    DropTables(Vec<(String, String)>),
    /// Show tables (schema_name)
    ShowTables(String),
}
