use super::scope::Scope;

pub struct BindContext {
    pub scopes: Vec<Scope>,
}

#[allow(dead_code)]
impl BindContext {
    pub fn push(&mut self, scope: Scope) {
        self.scopes.push(scope);
    }

    pub fn current_scope(&self) -> &Scope {
        self.scopes.last().unwrap()
    }
}
