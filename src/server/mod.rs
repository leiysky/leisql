use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{stream, StreamExt};

use pgwire::{
    api::{
        query::SimpleQueryHandler,
        results::{query_response, DataRowEncoder, Response, Tag},
        ClientInfo,
    },
    error::PgWireResult,
};

use crate::sql::{
    session::{QueryResult, SQLKind},
    Session,
};

pub struct PostgresHandler {
    pub session: Arc<Mutex<Session>>,
}

#[async_trait]
impl SimpleQueryHandler for PostgresHandler {
    async fn do_query<'b, C>(&self, _client: &C, query: &'b str) -> PgWireResult<Vec<Response<'b>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let mut session = self.session.lock().unwrap();

        let QueryResult {
            fields,
            data: tuples,
            kind,
        } = session
            .execute(query)
            .map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;

        match kind {
            SQLKind::Query => {
                let data_row_stream = stream::iter(tuples.into_iter()).map(|tuple| {
                    let mut encoder = DataRowEncoder::new(2);
                    for datum in tuple.values.iter() {
                        encoder.encode_text_format_field(Some(datum))?;
                    }

                    encoder.finish()
                });

                Ok(vec![Response::Query(query_response(
                    Some(fields),
                    data_row_stream,
                ))])
            }
            SQLKind::Execute => {
                return Ok(vec![Response::Execution(Tag::new_for_execution(
                    "Something good happened",
                    None,
                ))])
            }
        }
    }
}
