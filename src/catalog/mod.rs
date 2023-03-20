use self::defs::{SchemaDefinition, TableDefinition};
use crate::core::{ErrorKind, SQLError};

pub mod defs;

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub schemas: Vec<SchemaDefinition>,
}

impl Catalog {
    pub fn new() -> Self {
        let mut empty = Self::default();
        empty.create_schema("default").unwrap();
        empty
    }

    pub fn create_schema(&mut self, schema_name: &str) -> Result<(), SQLError> {
        if self.exists_schema(schema_name)? {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                "schema already exists",
            ));
        }

        self.schemas.push(SchemaDefinition {
            name: schema_name.to_string(),
            tables: vec![],
        });

        Ok(())
    }

    pub fn drop_schema(&mut self, schema_name: &str) -> Result<(), SQLError> {
        if !self.exists_schema(schema_name)? {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                "schema does not exist",
            ));
        }

        self.schemas.retain(|schema| schema.name != schema_name);

        Ok(())
    }

    pub fn exists_schema(&self, schema_name: &str) -> Result<bool, SQLError> {
        Ok(self.schemas.iter().any(|schema| schema.name == schema_name))
    }

    #[allow(dead_code)]
    pub fn list_schemas(&self) -> Vec<String> {
        self.schemas
            .iter()
            .map(|schema| schema.name.clone())
            .collect()
    }

    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_def: &TableDefinition,
    ) -> Result<(), SQLError> {
        if !self.exists_schema(schema_name)? {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                "schema does not exist",
            ));
        }

        if self
            .find_table_by_name(schema_name, &table_def.name)?
            .is_some()
        {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                "table already exists",
            ));
        }

        if let Some(schema) = self.schemas.iter_mut().find(|v| v.name == schema_name) {
            schema.tables.push(table_def.clone());
        }

        Ok(())
    }

    pub fn list_tables(&self, schema_name: &str) -> Result<Vec<String>, SQLError> {
        if !self.exists_schema(schema_name)? {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                "schema does not exist",
            ));
        }

        Ok(self
            .schemas
            .iter()
            .find(|v| v.name == schema_name)
            .map(|schema| {
                schema
                    .tables
                    .iter()
                    .map(|table| table.name.clone())
                    .collect()
            })
            .unwrap())
    }

    /// Find a table by qualified names
    pub fn find_table_by_name(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableDefinition>, SQLError> {
        let mut candidates = vec![];
        // Schema name is not specified
        for schema in &self.schemas {
            if schema.name == schema_name {
                if let Some(table) = schema.tables.iter().find(|table| table.name == table_name) {
                    candidates.push(table.clone());
                }
            }
        }

        if candidates.len() > 1 {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                format!(
                    "ambiguous table name: {}",
                    [schema_name.to_string(), table_name.to_string()].join(".")
                ),
            ));
        }

        if candidates.len() == 1 {
            Ok(Some(candidates.remove(0)))
        } else {
            Ok(None)
        }
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> Result<(), SQLError> {
        if !self.exists_schema(schema_name)? {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                "schema does not exist",
            ));
        }

        let schema = self
            .schemas
            .iter_mut()
            .find(|v| v.name == schema_name)
            .unwrap();

        if !schema.tables.iter().any(|table| table.name == table_name) {
            return Err(SQLError::new(
                ErrorKind::CatalogError,
                "table does not exist",
            ));
        }

        schema.tables.retain(|table| table.name != table_name);

        Ok(())
    }
}
