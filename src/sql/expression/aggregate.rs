use std::{collections::HashMap, sync::Arc};

use enum_as_inner::EnumAsInner;

use crate::core::{Datum, Type};

lazy_static! {
    static ref BUILTIN_AGGREGATE_FUNCTIONS: AggregateFunctionRegistry = {
        let mut registry = AggregateFunctionRegistry::default();
        register_count(&mut registry);
        register_sum(&mut registry);
        register_avg(&mut registry);
        register_min_max(&mut registry);
        registry
    };
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum AggregateState {
    Count(u64),
    Sum(Datum),
    Avg(Datum, usize),
    MinMax(Datum),
}

impl AggregateState {
    pub fn finalize(&self) -> Datum {
        match self {
            AggregateState::Count(count) => Datum::Int(*count as i64),
            AggregateState::Sum(value) => value.clone(),
            AggregateState::Avg(value, count) => {
                if *count == 0 {
                    Datum::Null
                } else {
                    value.clone()
                }
            }
            AggregateState::MinMax(value) => value.clone(),
        }
    }
}

#[allow(clippy::type_complexity)]
pub struct AggregateFunction {
    pub name: String,
    pub arg_types: Vec<Type>,
    pub ret_type: Type,
    pub default_state: AggregateState,
    pub accumulate: Box<dyn Fn(&[Datum], &AggregateState) -> AggregateState + Send + Sync>,
}

#[derive(Default)]
pub struct AggregateFunctionRegistry {
    pub functions: HashMap<String, Vec<Arc<AggregateFunction>>>,
}

impl AggregateFunctionRegistry {
    pub fn builtin() -> &'static AggregateFunctionRegistry {
        &BUILTIN_AGGREGATE_FUNCTIONS
    }

    pub fn contains(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }

    pub fn register_skip_null<F>(
        &mut self,
        name: &str,
        arg_types: &[Type],
        ret_type: Type,
        default_state: AggregateState,
        accumulate: F,
    ) where
        F: Fn(&[Datum], &AggregateState) -> AggregateState + Send + Sync + 'static,
    {
        let null_skipper = move |args: &[Datum], state: &AggregateState| {
            if args.iter().any(|arg| arg.is_null()) {
                state.clone()
            } else {
                accumulate(args, state)
            }
        };

        let func = Arc::new(AggregateFunction {
            name: name.to_string(),
            arg_types: arg_types.to_vec(),
            ret_type,
            default_state,
            accumulate: Box::new(null_skipper),
        });
        self.functions
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push(func);
    }

    pub fn search_candidates(&self, name: &str) -> Vec<Arc<AggregateFunction>> {
        self.functions.get(name).cloned().unwrap_or_default()
    }
}

fn register_count(registry: &mut AggregateFunctionRegistry) {
    registry.register_skip_null(
        "count",
        &[],
        Type::Int,
        AggregateState::Count(0),
        |_: &[Datum], state: &AggregateState| {
            let state = state.as_count().unwrap();

            AggregateState::Count(*state + 1)
        },
    );
    registry.register_skip_null(
        "count",
        &[Type::Any],
        Type::Int,
        AggregateState::Count(0),
        |_: &[Datum], state: &AggregateState| {
            let state = state.as_count().unwrap();

            AggregateState::Count(*state + 1)
        },
    );
}

fn register_sum(registry: &mut AggregateFunctionRegistry) {
    registry.register_skip_null(
        "sum",
        &[Type::Int],
        Type::Int,
        AggregateState::Sum(Datum::Null),
        |args: &[Datum], state: &AggregateState| {
            let mut state = state.as_sum().unwrap().clone();

            if matches!(state, Datum::Null) {
                state = Datum::Int(0);
            }

            let arg = args[0].as_int().unwrap();

            AggregateState::Sum(Datum::Int(*state.as_int().unwrap() + arg))
        },
    );
    registry.register_skip_null(
        "sum",
        &[Type::Float],
        Type::Float,
        AggregateState::Sum(Datum::Null),
        |args: &[Datum], state: &AggregateState| {
            let mut state = state.as_sum().unwrap().clone();

            if matches!(state, Datum::Null) {
                state = Datum::Float(0f64);
            }

            let arg = args[0].as_float().unwrap();

            AggregateState::Sum(Datum::Float(*state.as_float().unwrap() + arg))
        },
    );
}

fn register_avg(registry: &mut AggregateFunctionRegistry) {
    registry.register_skip_null(
        "avg",
        &[Type::Int],
        Type::Float,
        AggregateState::Avg(Datum::Null, 0),
        |args: &[Datum], state: &AggregateState| {
            let mut state = state.clone().into_avg().unwrap();

            if matches!(state.0, Datum::Null) {
                state.0 = Datum::Float(0f64);
            }

            let arg = args[0].as_int().unwrap();

            AggregateState::Avg(
                Datum::Float(*state.0.as_float().unwrap() + *arg as f64),
                state.1 + 1,
            )
        },
    );
    registry.register_skip_null(
        "avg",
        &[Type::Float],
        Type::Float,
        AggregateState::Avg(Datum::Null, 0),
        |args: &[Datum], state: &AggregateState| {
            let mut state = state.clone().into_avg().unwrap();

            if matches!(state.0, Datum::Null) {
                state.0 = Datum::Float(0f64);
            }

            let arg = args[0].as_float().unwrap();

            AggregateState::Avg(
                Datum::Float(*state.0.as_float().unwrap() + arg),
                state.1 + 1,
            )
        },
    );
}

fn register_min_max(registry: &mut AggregateFunctionRegistry) {
    // Min
    registry.register_skip_null(
        "min",
        &[Type::Int],
        Type::Int,
        AggregateState::MinMax(Datum::Null),
        |args: &[Datum], state: &AggregateState| {
            let s = state.as_min_max().unwrap();

            let arg = args[0].as_int().unwrap();

            if matches!(state, AggregateState::MinMax(Datum::Null)) {
                return AggregateState::MinMax(Datum::Int(*arg));
            };

            if arg < s.as_int().unwrap() {
                AggregateState::MinMax(Datum::Int(*arg))
            } else {
                AggregateState::MinMax(s.clone())
            }
        },
    );
    registry.register_skip_null(
        "min",
        &[Type::Float],
        Type::Float,
        AggregateState::MinMax(Datum::Null),
        |args: &[Datum], state: &AggregateState| {
            let s = state.as_min_max().unwrap();

            let arg = args[0].as_float().unwrap();

            if matches!(state, AggregateState::MinMax(Datum::Null)) {
                return AggregateState::MinMax(Datum::Float(*arg));
            };

            if arg < s.as_float().unwrap() {
                AggregateState::MinMax(Datum::Float(*arg))
            } else {
                AggregateState::MinMax(s.clone())
            }
        },
    );
    registry.register_skip_null(
        "min",
        &[Type::String],
        Type::String,
        AggregateState::MinMax(Datum::Null),
        |args: &[Datum], state: &AggregateState| {
            let s = state.as_min_max().unwrap();

            let arg = args[0].as_string().unwrap();

            if matches!(state, AggregateState::MinMax(Datum::Null)) {
                return AggregateState::MinMax(Datum::String(arg.clone()));
            };

            if arg < s.as_string().unwrap() {
                AggregateState::MinMax(Datum::String(arg.clone()))
            } else {
                AggregateState::MinMax(s.clone())
            }
        },
    );

    // Max
    registry.register_skip_null(
        "max",
        &[Type::Int],
        Type::Int,
        AggregateState::MinMax(Datum::Null),
        |args: &[Datum], state: &AggregateState| {
            let s = state.as_min_max().unwrap();

            let arg = args[0].as_int().unwrap();

            if matches!(state, AggregateState::MinMax(Datum::Null)) {
                return AggregateState::MinMax(Datum::Int(*arg));
            };

            if arg > s.as_int().unwrap() {
                AggregateState::MinMax(Datum::Int(*arg))
            } else {
                AggregateState::MinMax(s.clone())
            }
        },
    );
    registry.register_skip_null(
        "max",
        &[Type::Float],
        Type::Float,
        AggregateState::MinMax(Datum::Null),
        |args: &[Datum], state: &AggregateState| {
            let s = state.as_min_max().unwrap();

            let arg = args[0].as_float().unwrap();

            if matches!(state, AggregateState::MinMax(Datum::Null)) {
                return AggregateState::MinMax(Datum::Float(*arg));
            };

            if arg > s.as_float().unwrap() {
                AggregateState::MinMax(Datum::Float(*arg))
            } else {
                AggregateState::MinMax(s.clone())
            }
        },
    );
    registry.register_skip_null(
        "max",
        &[Type::String],
        Type::String,
        AggregateState::MinMax(Datum::Null),
        |args: &[Datum], state: &AggregateState| {
            let s = state.as_min_max().unwrap();

            let arg = args[0].as_string().unwrap();

            if matches!(state, AggregateState::MinMax(Datum::Null)) {
                return AggregateState::MinMax(Datum::String(arg.clone()));
            };

            if arg > s.as_string().unwrap() {
                AggregateState::MinMax(Datum::String(arg.clone()))
            } else {
                AggregateState::MinMax(s.clone())
            }
        },
    );
}
