use std::{collections::HashMap, sync::Arc};

use crate::core::{Datum, Type};

lazy_static! {
    static ref BUILTIN_SCALAR_FUNCTIONS: ScalarFunctionRegistry = {
        let mut registry = ScalarFunctionRegistry::default();

        register_arithmetic_functions(&mut registry);
        register_comparison_functions(&mut registry);
        register_cast_functions(&mut registry);

        registry
    };
}

#[allow(clippy::type_complexity)]
pub struct ScalarFunction {
    pub name: String,
    pub arg_types: Vec<Type>,
    pub ret_type: Type,
    pub eval: Box<dyn Fn(&[Datum]) -> Datum + Send + Sync>,
}

#[derive(Default)]
pub struct ScalarFunctionRegistry {
    /// function name -> overloads
    pub functions: HashMap<String, Vec<Arc<ScalarFunction>>>,
}

impl ScalarFunctionRegistry {
    pub fn builtin() -> &'static ScalarFunctionRegistry {
        &BUILTIN_SCALAR_FUNCTIONS
    }

    #[allow(dead_code)]
    pub fn contains(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }

    #[allow(dead_code)]
    pub fn register<F>(&mut self, name: &str, arg_types: &[Type], ret_type: Type, func: F)
    where
        F: Fn(&[Datum]) -> Datum + Send + Sync + 'static,
    {
        let scalar_func = ScalarFunction {
            name: name.to_string(),
            arg_types: arg_types.to_vec(),
            ret_type,
            eval: Box::new(func),
        };

        self.functions
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push(Arc::new(scalar_func));
    }

    pub fn register_null_passthrough<F>(
        &mut self,
        name: &str,
        arg_types: &[Type],
        ret_type: Type,
        func: F,
    ) where
        F: Fn(&[Datum]) -> Datum + Send + Sync + 'static,
    {
        let null_passthrough_func = move |args: &[Datum]| {
            if args.iter().any(|arg| arg.is_null()) {
                return Datum::Null;
            }

            func(args)
        };

        let scalar_func = ScalarFunction {
            name: name.to_string(),
            arg_types: arg_types.to_vec(),
            ret_type,
            eval: Box::new(null_passthrough_func),
        };

        self.functions
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push(Arc::new(scalar_func));
    }

    pub fn search_candidates(&self, name: &str) -> Vec<Arc<ScalarFunction>> {
        self.functions.get(name).cloned().unwrap_or_default()
    }
}

pub fn register_arithmetic_functions(registry: &mut ScalarFunctionRegistry) {
    // Plus
    registry.register_null_passthrough("+", &[Type::Int, Type::Int], Type::Int, |args| {
        let left = args[0].as_int().unwrap();
        let right = args[1].as_int().unwrap();

        Datum::Int(left + right)
    });
    registry.register_null_passthrough("+", &[Type::Float, Type::Float], Type::Float, |args| {
        let left = args[0].as_float().unwrap();
        let right = args[1].as_float().unwrap();

        Datum::Float(left + right)
    });

    // Minus
    registry.register_null_passthrough("-", &[Type::Int, Type::Int], Type::Int, |args| {
        let left = args[0].as_int().unwrap();
        let right = args[1].as_int().unwrap();

        Datum::Int(left + right)
    });
    registry.register_null_passthrough("-", &[Type::Float, Type::Float], Type::Float, |args| {
        let left = args[0].as_float().unwrap();
        let right = args[1].as_float().unwrap();

        Datum::Float(left - right)
    });
}

pub fn register_comparison_functions(registry: &mut ScalarFunctionRegistry) {
    // Equal
    registry.register_null_passthrough("=", &[Type::Int, Type::Int], Type::Boolean, |args| {
        let left = args[0].as_int().unwrap();
        let right = args[1].as_int().unwrap();

        Datum::Boolean(left == right)
    });
    registry.register_null_passthrough("=", &[Type::Float, Type::Float], Type::Boolean, |args| {
        let left = args[0].as_float().unwrap();
        let right = args[1].as_float().unwrap();

        Datum::Boolean(left == right)
    });
    registry.register_null_passthrough("=", &[Type::String, Type::String], Type::Boolean, |args| {
        let left = args[0].as_string().unwrap();
        let right = args[1].as_string().unwrap();

        Datum::Boolean(left == right)
    });
    registry.register_null_passthrough(
        "=",
        &[Type::Boolean, Type::Boolean],
        Type::Boolean,
        |args| {
            let left = args[0].as_boolean().unwrap();
            let right = args[1].as_boolean().unwrap();

            Datum::Boolean(left == right)
        },
    );

    // Not equal
    registry.register_null_passthrough("<>", &[Type::Int, Type::Int], Type::Boolean, |args| {
        let left = args[0].as_int().unwrap();
        let right = args[1].as_int().unwrap();

        Datum::Boolean(left != right)
    });
    registry.register_null_passthrough("<>", &[Type::Float, Type::Float], Type::Boolean, |args| {
        let left = args[0].as_float().unwrap();
        let right = args[1].as_float().unwrap();

        Datum::Boolean(left != right)
    });
    registry.register_null_passthrough(
        "<>",
        &[Type::String, Type::String],
        Type::Boolean,
        |args| {
            let left = args[0].as_string().unwrap();
            let right = args[1].as_string().unwrap();

            Datum::Boolean(left != right)
        },
    );
    registry.register_null_passthrough(
        "<>",
        &[Type::Boolean, Type::Boolean],
        Type::Boolean,
        |args| {
            let left = args[0].as_boolean().unwrap();
            let right = args[1].as_boolean().unwrap();

            Datum::Boolean(left != right)
        },
    );

    // Less than
    registry.register_null_passthrough("<", &[Type::Int, Type::Int], Type::Boolean, |args| {
        let left = args[0].as_int().unwrap();
        let right = args[1].as_int().unwrap();

        Datum::Boolean(left < right)
    });
    registry.register_null_passthrough("<", &[Type::Float, Type::Float], Type::Boolean, |args| {
        let left = args[0].as_float().unwrap();
        let right = args[1].as_float().unwrap();

        Datum::Boolean(left < right)
    });
    registry.register_null_passthrough("<", &[Type::String, Type::String], Type::Boolean, |args| {
        let left = args[0].as_string().unwrap();
        let right = args[1].as_string().unwrap();

        Datum::Boolean(left < right)
    });

    // Less than or equal
    registry.register_null_passthrough("<=", &[Type::Int, Type::Int], Type::Boolean, |args| {
        let left = args[0].as_int().unwrap();
        let right = args[1].as_int().unwrap();

        Datum::Boolean(left <= right)
    });
    registry.register_null_passthrough("<=", &[Type::Float, Type::Float], Type::Boolean, |args| {
        let left = args[0].as_float().unwrap();
        let right = args[1].as_float().unwrap();

        Datum::Boolean(left <= right)
    });
    registry.register_null_passthrough(
        "<=",
        &[Type::String, Type::String],
        Type::Boolean,
        |args| {
            let left = args[0].as_string().unwrap();
            let right = args[1].as_string().unwrap();

            Datum::Boolean(left <= right)
        },
    );

    // Greater than
    registry.register_null_passthrough(">", &[Type::Int, Type::Int], Type::Boolean, |args| {
        let left = args[0].as_int().unwrap();
        let right = args[1].as_int().unwrap();

        Datum::Boolean(left > right)
    });
    registry.register_null_passthrough(">", &[Type::Float, Type::Float], Type::Boolean, |args| {
        let left = args[0].as_float().unwrap();
        let right = args[1].as_float().unwrap();

        Datum::Boolean(left > right)
    });
    registry.register_null_passthrough(">", &[Type::String, Type::String], Type::Boolean, |args| {
        let left = args[0].as_string().unwrap();
        let right = args[1].as_string().unwrap();

        Datum::Boolean(left > right)
    });

    // Greater than or equal
    registry.register_null_passthrough(">=", &[Type::Int, Type::Int], Type::Boolean, |args| {
        let left = args[0].as_int().unwrap();
        let right = args[1].as_int().unwrap();

        Datum::Boolean(left >= right)
    });
    registry.register_null_passthrough(">=", &[Type::Float, Type::Float], Type::Boolean, |args| {
        let left = args[0].as_float().unwrap();
        let right = args[1].as_float().unwrap();

        Datum::Boolean(left >= right)
    });
    registry.register_null_passthrough(
        ">=",
        &[Type::String, Type::String],
        Type::Boolean,
        |args| {
            let left = args[0].as_string().unwrap();
            let right = args[1].as_string().unwrap();

            Datum::Boolean(left >= right)
        },
    );
}

fn register_cast_functions(registry: &mut ScalarFunctionRegistry) {
    // Cast as int
    // registry.register_null_passthrough("to_int", &[Type::String], Type::Int, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::Int)
    // });
    // registry.register_null_passthrough("to_int", &[Type::Float], Type::Int, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::Int)
    // });
    // registry.register_null_passthrough("to_int", &[Type::Boolean], Type::Int, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::Int)
    // });
    // registry.register_null_passthrough("to_int", &[Type::Null], Type::Int, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::Int)
    // });
    registry.register_null_passthrough("to_int", &[Type::Any], Type::Int, |args| {
        let value = &args[0];

        value.cast(&Type::Int)
    });

    // Cast as float
    registry.register_null_passthrough("to_float", &[Type::Any], Type::Float, |args| {
        let value = &args[0];

        value.cast(&Type::Float)
    });

    // Cast as string
    // registry.register_null_passthrough("to_string", &[Type::Int], Type::String, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::String)
    // });
    // registry.register_null_passthrough("to_string", &[Type::Boolean], Type::String, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::String)
    // });
    // registry.register_null_passthrough("to_string", &[Type::Boolean], Type::String, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::String)
    // });
    // registry.register_null_passthrough("to_string", &[Type::Null], Type::String, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::String)
    // });
    registry.register_null_passthrough("to_string", &[Type::Any], Type::String, |args| {
        let value = &args[0];

        value.cast(&Type::String)
    });

    // Cast as boolean
    // registry.register_null_passthrough("to_boolean", &[Type::Int], Type::Boolean, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::String)
    // });
    // registry.register_null_passthrough("to_boolean", &[Type::String], Type::Boolean, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::String)
    // });
    // registry.register_null_passthrough("to_boolean", &[Type::String], Type::Boolean, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::String)
    // });
    // registry.register_null_passthrough("to_boolean", &[Type::Null], Type::Boolean, |args| {
    //     let value = &args[0];

    //     value.cast(&Type::String)
    // });
    registry.register_null_passthrough("to_boolean", &[Type::Any], Type::Boolean, |args| {
        let value = &args[0];

        value.cast(&Type::Boolean)
    });
}
