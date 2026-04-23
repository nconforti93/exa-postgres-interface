use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, stream};
use pgwire::api::auth::{
    DefaultServerParameterProvider, StartupHandler, finish_authentication, protocol_negotiation,
    save_startup_parameters_to_metadata,
};
use pgwire::api::portal::Portal;
use pgwire::api::query::{
    ExtendedQueryHandler, SimpleQueryHandler, send_execution_response, send_query_response,
};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, DEFAULT_NAME, METADATA_USER, PgWireConnectionState,
    PgWireServerHandlers, PidSecretKeyGenerator, RandomPidSecretKeyGenerator, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::extendedquery::Execute;
use pgwire::messages::response::EmptyQueryResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use tokio::task;
use tracing::{debug, warn};

use crate::config::AppConfig;
use crate::exasol::{ExasolError, ExasolResult, ExasolSession};
use crate::policy::{StatementPlan, classify_statement};

struct SessionState {
    exasol: Mutex<ExasolSession>,
    extended_results: Mutex<HashMap<String, GatewayResponse>>,
}

#[derive(Clone, Debug)]
enum GatewayResponse {
    Empty,
    Query {
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
    Execution {
        command: String,
        rows: Option<usize>,
    },
    TransactionStart {
        command: String,
    },
    TransactionEnd {
        command: String,
    },
    Error {
        sqlstate: String,
        message: String,
    },
}

pub struct ExasolPgWireHandler {
    config: Arc<AppConfig>,
    query_parser: Arc<GatewayQueryParser>,
    parameters: DefaultServerParameterProvider,
    pid_secret_key_generator: RandomPidSecretKeyGenerator,
}

impl ExasolPgWireHandler {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let mut parameters = DefaultServerParameterProvider::default();
        parameters.server_version = "16.6-exasol-gateway".to_owned();
        parameters.is_superuser = false;
        parameters.default_transaction_read_only = true;

        Self {
            config,
            query_parser: Arc::new(GatewayQueryParser),
            parameters,
            pid_secret_key_generator: RandomPidSecretKeyGenerator,
        }
    }

    async fn connect_exasol(
        &self,
        username: String,
        password: String,
    ) -> PgWireResult<SessionState> {
        let config = self.config.clone();
        task::spawn_blocking(move || {
            let mut session = ExasolSession::connect(&config.exasol, &username, &password)?;
            if config.translation.enabled && !config.translation.sql_preprocessor_script.is_empty()
            {
                session.initialize(
                    &config.translation.session_init_sql,
                    &config.translation.sql_preprocessor_script,
                )?;
            }
            Ok::<_, ExasolError>(SessionState {
                exasol: Mutex::new(session),
                extended_results: Mutex::new(HashMap::new()),
            })
        })
        .await
        .map_err(|err| pg_error("58000", format!("Exasol connection task failed: {err}")))?
        .map_err(map_exasol_connection_error)
    }

    async fn execute_sql<C>(&self, client: &mut C, sql: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.execute_statement(client, sql)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect()
    }

    async fn execute_statement<C>(
        &self,
        client: &mut C,
        sql: &str,
    ) -> PgWireResult<Vec<GatewayResponse>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match classify_statement(sql) {
            StatementPlan::Empty => Ok(vec![GatewayResponse::Empty]),
            StatementPlan::ClientSet => Ok(vec![GatewayResponse::Execution {
                command: "SET".to_owned(),
                rows: None,
            }]),
            StatementPlan::ClientTransactionStart => Ok(vec![GatewayResponse::TransactionStart {
                command: "BEGIN".to_owned(),
            }]),
            StatementPlan::ClientTransactionEnd { command } => {
                Ok(vec![GatewayResponse::TransactionEnd {
                    command: command.to_owned(),
                }])
            }
            StatementPlan::ClientShow { name, value } => Ok(vec![GatewayResponse::Query {
                columns: vec![name],
                rows: vec![vec![Some(value)]],
            }]),
            StatementPlan::ClientSelect { columns, rows } => {
                Ok(vec![GatewayResponse::Query { columns, rows }])
            }
            StatementPlan::Reject { sqlstate, message } => {
                warn!(%sqlstate, %message, "rejecting unsupported statement");
                Ok(vec![GatewayResponse::Error {
                    sqlstate: sqlstate.to_owned(),
                    message,
                }])
            }
            StatementPlan::Read => {
                let state = client
                    .session_extensions()
                    .get::<SessionState>()
                    .ok_or_else(|| pg_error("08003", "Exasol session is not connected"))?;
                let sql = sql.to_owned();
                let result = task::spawn_blocking(move || {
                    let mut session = state.exasol.lock().map_err(|_| {
                        ExasolError::Connection("Exasol session lock poisoned".to_owned())
                    })?;
                    session.execute(&sql)
                })
                .await
                .map_err(|err| pg_error("58000", format!("Exasol execution task failed: {err}")))?
                .map_err(map_exasol_execution_error)?;
                map_exasol_result(result)
            }
        }
    }

    async fn execute_simple_query<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statements = split_simple_query(query);
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let mut responses = Vec::new();
        for statement in statements {
            let mut statement_responses = self.execute_sql(client, &statement).await?;
            let should_stop = statement_responses
                .iter()
                .any(|response| matches!(response, Response::Error(_)));
            responses.append(&mut statement_responses);
            if should_stop {
                break;
            }
        }
        Ok(responses)
    }
}

#[async_trait]
impl StartupHandler for ExasolPgWireHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(startup) => {
                protocol_negotiation(client, &startup).await?;
                save_startup_parameters_to_metadata(client, &startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(password) => {
                let password = password.into_password()?;
                let username = client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .ok_or(PgWireError::UserNameRequired)?;
                let session = self
                    .connect_exasol(username.clone(), password.password)
                    .await?;
                client.session_extensions().insert(session);

                let (pid, secret_key) = self.pid_secret_key_generator.generate(client);
                client.set_pid_and_secret_key(pid, secret_key);
                debug!(%username, "authenticated PostgreSQL client against Exasol");
                finish_authentication(client, &self.parameters).await?;
            }
            _ => {}
        }

        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for ExasolPgWireHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.execute_simple_query(client, query).await
    }
}

#[async_trait]
impl ExtendedQueryHandler for ExasolPgWireHandler {
    type Statement = String;
    type QueryParser = GatewayQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }

        let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        let portal = client
            .portal_store()
            .get_portal(portal_name)
            .ok_or_else(|| PgWireError::PortalNotFound(portal_name.to_owned()))?;

        client.set_state(PgWireConnectionState::QueryInProgress);
        let cached_response = take_cached_extended_result(client, portal_name)?;
        let was_described = cached_response.is_some();
        let response = if let Some(response) = cached_response {
            Response::try_from(response)?
        } else {
            ExtendedQueryHandler::do_query(self, client, portal.as_ref(), message.max_rows as usize)
                .await?
        };
        let send_describe = !matches!(response, Response::Query(_)) || !was_described;

        match response {
            Response::EmptyQuery => {
                client
                    .send(PgWireBackendMessage::EmptyQueryResponse(
                        EmptyQueryResponse::new(),
                    ))
                    .await?;
            }
            Response::Query(results) => {
                send_query_response(client, results, send_describe).await?;
            }
            Response::Execution(tag) => {
                send_execution_response(client, tag).await?;
            }
            Response::TransactionStart(tag) => {
                send_execution_response(client, tag).await?;
                client
                    .set_transaction_status(client.transaction_status().to_in_transaction_state());
            }
            Response::TransactionEnd(tag) => {
                send_execution_response(client, tag).await?;
                client.set_transaction_status(client.transaction_status().to_idle_state());
            }
            Response::Error(error) => {
                client
                    .send(PgWireBackendMessage::ErrorResponse((*error).into()))
                    .await?;
                client.set_transaction_status(client.transaction_status().to_error_state());
            }
            Response::CopyIn(_) | Response::CopyOut(_) | Response::CopyBoth(_) => {
                return Err(pg_error("0A000", "COPY protocol is not implemented"));
            }
        }

        client.set_state(PgWireConnectionState::ReadyForQuery);
        if portal_name == DEFAULT_NAME {
            client.portal_store().rm_portal(portal_name);
        }
        Ok(())
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribeStatementResponse::new(vec![], vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if target.parameter_len() > 0 {
            return Err(pg_error(
                "0A000",
                "prepared statement parameters are not implemented",
            ));
        }

        let mut responses = self
            .execute_statement(client, &target.statement.statement)
            .await?;
        let response = responses.pop().unwrap_or(GatewayResponse::Empty);
        let fields = response.fields();
        cache_extended_result(client, &target.name, response)?;
        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if portal.parameter_len() > 0 {
            return Ok(Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "0A000".to_owned(),
                "prepared statement parameters are not implemented".to_owned(),
            ))));
        }

        let mut responses = self
            .execute_statement(client, &portal.statement.statement)
            .await?;
        Response::try_from(responses.pop().unwrap_or(GatewayResponse::Empty))
    }
}

#[derive(Debug)]
pub struct GatewayQueryParser;

#[async_trait]
impl QueryParser for GatewayQueryParser {
    type Statement = String;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if split_simple_query(sql).len() > 1 {
            return Err(pg_error(
                "42601",
                "extended query protocol accepts one statement per Parse message",
            ));
        }
        Ok(sql.to_owned())
    }

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(vec![])
    }

    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(vec![])
    }
}

pub struct ExasolPgWireFactory {
    pub handler: Arc<ExasolPgWireHandler>,
}

impl PgWireServerHandlers for ExasolPgWireFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }
}

fn map_exasol_result(result: ExasolResult) -> PgWireResult<Vec<GatewayResponse>> {
    match result {
        ExasolResult::ResultSet { columns, rows } => {
            let names = columns
                .into_iter()
                .map(|column| {
                    let _ = column.data_type;
                    column.name
                })
                .collect();
            Ok(vec![GatewayResponse::Query {
                columns: names,
                rows,
            }])
        }
        ExasolResult::RowCount(rows) => Ok(vec![GatewayResponse::Execution {
            command: "OK".to_owned(),
            rows: Some(rows),
        }]),
    }
}

impl GatewayResponse {
    fn fields(&self) -> Vec<FieldInfo> {
        match self {
            GatewayResponse::Query { columns, .. } => columns
                .iter()
                .cloned()
                .map(|name| FieldInfo::new(name, None, None, Type::TEXT, FieldFormat::Text))
                .collect(),
            _ => Vec::new(),
        }
    }
}

impl TryFrom<GatewayResponse> for Response {
    type Error = PgWireError;

    fn try_from(response: GatewayResponse) -> Result<Self, PgWireError> {
        Ok(match response {
            GatewayResponse::Empty => Response::EmptyQuery,
            GatewayResponse::Query { columns, rows } => {
                Response::Query(query_response(columns, rows)?)
            }
            GatewayResponse::Execution { command, rows } => {
                let tag = if let Some(rows) = rows {
                    Tag::new(&command).with_rows(rows)
                } else {
                    Tag::new(&command)
                };
                Response::Execution(tag)
            }
            GatewayResponse::TransactionStart { command } => {
                Response::TransactionStart(Tag::new(&command))
            }
            GatewayResponse::TransactionEnd { command } => {
                Response::TransactionEnd(Tag::new(&command))
            }
            GatewayResponse::Error { sqlstate, message } => Response::Error(Box::new(
                ErrorInfo::new("ERROR".to_owned(), sqlstate, message),
            )),
        })
    }
}

fn cache_extended_result<C>(
    client: &C,
    portal_name: &str,
    response: GatewayResponse,
) -> PgWireResult<()>
where
    C: ClientInfo,
{
    let state = client
        .session_extensions()
        .get::<SessionState>()
        .ok_or_else(|| pg_error("08003", "Exasol session is not connected"))?;
    let mut cache = state
        .extended_results
        .lock()
        .map_err(|_| pg_error("58000", "extended result cache lock poisoned"))?;
    cache.insert(portal_name.to_owned(), response);
    Ok(())
}

fn take_cached_extended_result<C>(
    client: &C,
    portal_name: &str,
) -> PgWireResult<Option<GatewayResponse>>
where
    C: ClientInfo,
{
    let Some(state) = client.session_extensions().get::<SessionState>() else {
        return Ok(None);
    };
    let mut cache = state
        .extended_results
        .lock()
        .map_err(|_| pg_error("58000", "extended result cache lock poisoned"))?;
    Ok(cache.remove(portal_name))
}

fn query_response(
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
) -> PgWireResult<QueryResponse> {
    let fields = columns
        .into_iter()
        .map(|name| FieldInfo::new(name, None, None, Type::TEXT, FieldFormat::Text))
        .collect::<Vec<_>>();
    let schema = Arc::new(fields);
    let schema_for_rows = schema.clone();
    let column_count = schema_for_rows.len();
    let mut encoder = DataRowEncoder::new(schema_for_rows.clone());
    let row_stream = stream::iter(rows).map(move |row| {
        for idx in 0..column_count {
            let value = row.get(idx).cloned().unwrap_or(None);
            encoder.encode_field(&value)?;
        }
        Ok(encoder.take_row())
    });

    Ok(QueryResponse::new(schema, row_stream))
}

fn map_exasol_connection_error(error: ExasolError) -> PgWireError {
    pg_error(
        "28000",
        format!("Exasol authentication or connection failed: {error}"),
    )
}

fn map_exasol_execution_error(error: ExasolError) -> PgWireError {
    pg_error("XX000", format!("{error}"))
}

fn pg_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.into(),
    )))
}

fn split_simple_query(query: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    let mut chars = query.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' if !in_double_quote => {
                if in_single_quote && matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                } else {
                    in_single_quote = !in_single_quote;
                }
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
            }
            ';' if !in_single_quote && !in_double_quote => {
                let statement = query[start..idx].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_owned());
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    let statement = query[start..].trim();
    if !statement.is_empty() {
        statements.push(statement.to_owned());
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_simple_query_batches() {
        assert_eq!(
            split_simple_query("SET a = 1; SELECT 1;"),
            vec!["SET a = 1", "SELECT 1"]
        );
        assert_eq!(split_simple_query("SELECT ';'"), vec!["SELECT ';'"]);
        assert_eq!(
            split_simple_query("SELECT 'it''s'; SELECT 2"),
            vec!["SELECT 'it''s'", "SELECT 2"]
        );
    }
}
