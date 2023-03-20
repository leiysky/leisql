#[macro_use]
extern crate lazy_static;

use std::sync::{Arc, Mutex};

use catalog::Catalog;
use log::{info, LevelFilter};
use pgwire::{
    api::{
        auth::noop::NoopStartupHandler, query::PlaceholderExtendedQueryHandler, MakeHandler,
        StatelessMakeHandler,
    },
    tokio::process_socket,
};
use server::PostgresHandler;
use sql::{session::context::QueryContext, Session};
use storage::StorageManager;
use tokio::net::TcpListener;
use util::SimpleLogger;

mod catalog;
mod core;
mod server;
mod sql;
mod storage;
mod util;

static LOGGER: SimpleLogger = SimpleLogger;

#[tokio::main]
pub async fn main() {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    // Initialize database
    let catalog = Catalog::new();
    let storage_mgr = StorageManager::default();
    let query_ctx = QueryContext {
        catalog,
        current_schema: "default".to_string(),
        storage_mgr,
    };
    let session = Arc::new(Mutex::new(Session::new(query_ctx)));

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(PostgresHandler {
        session,
    })));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    info!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(process_socket(
            incoming_socket.0,
            None,
            authenticator_ref,
            processor_ref,
            placeholder_ref,
        ));
    }
}
