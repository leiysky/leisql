use std::collections::HashMap;

use self::relation::HeapTable;

pub mod relation;

#[derive(Default)]
pub struct StorageManager {
    pub relations: HashMap<(String, String), HeapTable>,
}

impl StorageManager {
    pub fn get_relation(&self, schema_name: &str, table_name: &str) -> Option<&HeapTable> {
        self.relations
            .get(&(schema_name.to_string(), table_name.to_string()))
    }

    pub fn get_relation_mut(
        &mut self,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&mut HeapTable> {
        self.relations
            .get_mut(&(schema_name.to_string(), table_name.to_string()))
    }

    pub fn create_relation(&mut self, schema_name: &str, table_name: &str) {
        self.relations.insert(
            (schema_name.to_string(), table_name.to_string()),
            HeapTable::new(),
        );
    }

    pub fn drop_relation(&mut self, schema_name: &str, table_name: &str) {
        self.relations
            .remove(&(schema_name.to_string(), table_name.to_string()));
    }
}
