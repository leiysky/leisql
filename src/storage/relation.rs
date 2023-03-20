use crate::core::Tuple;

#[derive(Debug, Clone, Default)]
pub struct HeapTable {
    pub tuples: Vec<Tuple>,
}

impl HeapTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, tuple: Tuple) {
        self.tuples.push(tuple);
    }

    #[allow(dead_code)]
    pub fn truncate(&mut self) {
        self.tuples.clear();
    }

    pub fn scan(&self, scan_state: &mut ScanState) -> Option<Tuple> {
        if scan_state.cursor >= self.tuples.len() {
            return None;
        }

        let tuple = self.tuples[scan_state.cursor].clone();
        scan_state.cursor += 1;

        Some(tuple)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScanState {
    cursor: usize,
}
