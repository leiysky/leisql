use crate::{catalog::Catalog, storage::StorageManager};

/// The context stores all the information needed to execute a query.
pub struct QueryContext {
    pub catalog: Catalog,
    pub storage_mgr: StorageManager,
    pub current_schema: String,
}
