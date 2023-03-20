use crate::core::Type;

#[derive(Clone, Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: Type,
    pub null: bool,
}

#[derive(Clone, Debug)]
pub struct TableDefinition {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Clone, Debug)]
pub struct SchemaDefinition {
    pub name: String,
    pub tables: Vec<TableDefinition>,
}
