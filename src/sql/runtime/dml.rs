use crate::core::Tuple;

#[derive(Debug, Clone)]
pub enum DMLJob {
    /// `INSERT INTO` statement, insert a series of tuples into a table.
    Insert((String, String), Vec<Tuple>),
}
