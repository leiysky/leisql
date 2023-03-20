use std::fmt::Display;

use super::Datum;

#[derive(Debug, Clone, Default)]
pub struct Tuple {
    pub values: Vec<Datum>,
}

impl Tuple {
    pub fn new(values: Vec<Datum>) -> Self {
        Self { values }
    }

    pub fn append(&mut self, value: Datum) {
        self.values.push(value);
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.values.clear();
    }

    pub fn get(&self, index: usize) -> Option<Datum> {
        self.values.get(index).cloned()
    }

    pub fn project(&self, indices: &[usize]) -> Tuple {
        let values = indices
            .iter()
            .map(|i| self.values[*i].clone())
            .collect::<Vec<_>>();

        Tuple::new(values)
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let result = self
            .values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "{}", result)
    }
}
