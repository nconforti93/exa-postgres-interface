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
use crate::metadata::{MetadataPlan, detect as detect_metadata, map_exasol_column_type};
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
        debug!(sql = %sql, "handling PostgreSQL statement");
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

    async fn execute_metadata_query<C>(
        &self,
        client: &mut C,
        sql: &str,
    ) -> PgWireResult<Option<GatewayResponse>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let Some(plan) = detect_metadata(sql) else {
            return Ok(None);
        };

        let response = match plan {
            MetadataPlan::JdbcSchemas => {
                let mut schemas = self
                    .fetch_query_rows(
                        client,
                        "SELECT SCHEMA_NAME FROM SYS.EXA_SCHEMAS ORDER BY SCHEMA_NAME",
                    )
                    .await?;
                schemas.push(vec![Some("information_schema".to_owned())]);
                schemas.push(vec![Some("pg_catalog".to_owned())]);
                schemas.sort();
                GatewayResponse::Query {
                    columns: vec!["TABLE_SCHEM".to_owned(), "TABLE_CATALOG".to_owned()],
                    rows: schemas
                        .into_iter()
                        .map(|row| vec![row.first().cloned().unwrap_or(None), Some("exasol".to_owned())])
                        .collect(),
                }
            }
            MetadataPlan::PgNamespace => {
                let mut schemas = self
                    .fetch_query_rows(
                        client,
                        "SELECT SCHEMA_NAME FROM SYS.EXA_SCHEMAS ORDER BY SCHEMA_NAME",
                    )
                    .await?;
                schemas.push(vec![Some("information_schema".to_owned())]);
                schemas.push(vec![Some("pg_catalog".to_owned())]);
                schemas.sort();
                GatewayResponse::Query {
                    columns: vec![
                        "oid".to_owned(),
                        "nspname".to_owned(),
                        "nspowner".to_owned(),
                        "nspacl".to_owned(),
                    ],
                    rows: schemas
                        .into_iter()
                        .enumerate()
                        .map(|(idx, row)| {
                            vec![
                                Some((2200 + idx as i32).to_string()),
                                row.first().cloned().unwrap_or(None),
                                Some("10".to_owned()),
                                None,
                            ]
                        })
                        .collect(),
                }
            }
            MetadataPlan::JdbcTables {
                schema_pattern,
                table_pattern,
            } => self
                .execute_exasol_query(
                    client,
                    &format!(
                        "SELECT \
                            'exasol' AS \"TABLE_CAT\", \
                            object_schema AS \"TABLE_SCHEM\", \
                            object_name AS \"TABLE_NAME\", \
                            object_type AS \"TABLE_TYPE\", \
                            remarks AS \"REMARKS\", \
                            '' AS \"TYPE_CAT\", \
                            '' AS \"TYPE_SCHEM\", \
                            '' AS \"TYPE_NAME\", \
                            '' AS \"SELF_REFERENCING_COL_NAME\", \
                            '' AS \"REF_GENERATION\" \
                         FROM ( \
                            SELECT TABLE_SCHEMA AS object_schema, TABLE_NAME AS object_name, 'TABLE' AS object_type, COALESCE(TABLE_COMMENT, '') AS remarks \
                            FROM SYS.EXA_ALL_TABLES \
                            UNION ALL \
                            SELECT VIEW_SCHEMA AS object_schema, VIEW_NAME AS object_name, 'VIEW' AS object_type, COALESCE(VIEW_COMMENT, '') AS remarks \
                            FROM SYS.EXA_ALL_VIEWS \
                         ) objects \
                         WHERE object_schema LIKE {schema_pattern} AND object_name LIKE {table_pattern} \
                         ORDER BY \"TABLE_TYPE\", \"TABLE_SCHEM\", \"TABLE_NAME\"",
                        schema_pattern = sql_string_literal(&schema_pattern),
                        table_pattern = sql_string_literal(&table_pattern),
                    ),
                )
                .await?,
            MetadataPlan::JdbcColumns {
                schema_pattern,
                table_pattern,
                column_pattern,
            } => {
                let rows = self
                    .fetch_query_rows(
                        client,
                        &format!(
                            "SELECT COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE, COLUMN_IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT, COLUMN_ORDINAL_POSITION \
                             FROM SYS.EXA_ALL_COLUMNS \
                             WHERE COLUMN_OBJECT_TYPE IN ('TABLE', 'VIEW') \
                               AND COLUMN_SCHEMA LIKE {schema_pattern} \
                               AND COLUMN_TABLE LIKE {table_pattern} \
                               AND COLUMN_NAME LIKE {column_pattern} \
                             ORDER BY COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_ORDINAL_POSITION",
                            schema_pattern = sql_string_literal(&schema_pattern),
                            table_pattern = sql_string_literal(&table_pattern),
                            column_pattern = sql_string_literal(&column_pattern),
                        ),
                    )
                    .await?;

                GatewayResponse::Query {
                    columns: vec![
                        "current_database".to_owned(),
                        "nspname".to_owned(),
                        "relname".to_owned(),
                        "attname".to_owned(),
                        "atttypid".to_owned(),
                        "attnotnull".to_owned(),
                        "atttypmod".to_owned(),
                        "attlen".to_owned(),
                        "typtypmod".to_owned(),
                        "attnum".to_owned(),
                        "attidentity".to_owned(),
                        "attgenerated".to_owned(),
                        "adsrc".to_owned(),
                        "description".to_owned(),
                        "typbasetype".to_owned(),
                        "typtype".to_owned(),
                    ],
                    rows: rows
                        .into_iter()
                        .map(|row| {
                            let exa_type = row.get(3).and_then(|value| value.as_deref()).unwrap_or("VARCHAR(2000) UTF8");
                            let type_info = map_exasol_column_type(exa_type);
                            let nullable = row.get(4).and_then(|value| value.as_deref()) == Some("false");
                            vec![
                                Some("exasol".to_owned()),
                                row.first().cloned().unwrap_or(None),
                                row.get(1).cloned().unwrap_or(None),
                                row.get(2).cloned().unwrap_or(None),
                                Some(type_info.oid.to_string()),
                                Some(if nullable { "true" } else { "false" }.to_owned()),
                                Some(type_info.typmod.to_string()),
                                Some(type_info.typlen.to_string()),
                                Some(type_info.typmod.to_string()),
                                row.get(7).cloned().unwrap_or(Some("0".to_owned())),
                                None,
                                None,
                                row.get(5).cloned().unwrap_or(None),
                                row.get(6).cloned().unwrap_or(None),
                                Some(type_info.typbasetype.to_string()),
                                Some(type_info.typtype.to_owned()),
                            ]
                        })
                        .collect(),
                }
            }
            MetadataPlan::PgTables {
                schema_exclude,
                table_name,
            } => self
                .execute_exasol_query(
                    client,
                    &format!(
                        "SELECT \
                            TABLE_SCHEMA AS schemaname, \
                            TABLE_NAME AS tablename, \
                            TABLE_OWNER AS tableowner, \
                            '' AS tablespace, \
                            false AS hasindexes, \
                            false AS hasrules, \
                            false AS hastriggers, \
                            false AS rowsecurity \
                         FROM SYS.EXA_ALL_TABLES \
                         WHERE TABLE_SCHEMA != {schema_exclude} {table_filter} \
                         ORDER BY schemaname, tablename",
                        schema_exclude = sql_string_literal(schema_exclude.as_deref().unwrap_or("pg_catalog")),
                        table_filter = table_name
                            .map(|value| format!("AND TABLE_NAME = {}", sql_string_literal(&value)))
                            .unwrap_or_default(),
                    ),
                )
                .await?,
            MetadataPlan::InfoSchemaTableNames { catalog, schema } => {
                if !catalog.eq_ignore_ascii_case("exasol") {
                    empty_query(vec!["TABLE_NAME"])
                } else {
                    self.execute_exasol_query(
                        client,
                        &format!(
                            "SELECT TABLE_NAME \
                             FROM ( \
                                SELECT 'exasol' AS TABLE_CATALOG, TABLE_SCHEMA AS TABLE_SCHEMA, TABLE_NAME AS TABLE_NAME FROM SYS.EXA_ALL_TABLES \
                                UNION ALL \
                                SELECT 'exasol' AS TABLE_CATALOG, VIEW_SCHEMA AS TABLE_SCHEMA, VIEW_NAME AS TABLE_NAME FROM SYS.EXA_ALL_VIEWS \
                             ) objects \
                             WHERE TABLE_CATALOG = 'exasol' AND TABLE_SCHEMA = {schema} \
                             ORDER BY TABLE_NAME",
                            schema = sql_string_literal(&schema),
                        ),
                    )
                    .await?
                }
            }
            MetadataPlan::InfoSchemaColumnNames {
                catalog,
                schema,
                table,
            } => {
                if !catalog.eq_ignore_ascii_case("exasol") {
                    empty_query(vec!["COLUMN_NAME"])
                } else {
                    self.execute_exasol_query(
                        client,
                        &format!(
                            "SELECT COLUMN_NAME \
                             FROM SYS.EXA_ALL_COLUMNS \
                             WHERE COLUMN_SCHEMA = {schema} AND COLUMN_TABLE = {table} \
                             ORDER BY COLUMN_NAME",
                            schema = sql_string_literal(&schema),
                            table = sql_string_literal(&table),
                        ),
                    )
                    .await?
                }
            }
            MetadataPlan::PgUser => GatewayResponse::Query {
                columns: vec![
                    "usename".to_owned(),
                    "usesysid".to_owned(),
                    "usecreatedb".to_owned(),
                    "usesuper".to_owned(),
                    "userepl".to_owned(),
                    "usebypassrls".to_owned(),
                    "passwd".to_owned(),
                    "valuntil".to_owned(),
                    "useconfig".to_owned(),
                ],
                rows: vec![vec![
                    Some("sys".to_owned()),
                    Some("10".to_owned()),
                    Some("true".to_owned()),
                    Some("true".to_owned()),
                    Some("false".to_owned()),
                    Some("true".to_owned()),
                    None,
                    None,
                    None,
                ]],
            },
            MetadataPlan::PgGroup => empty_query(vec!["groname", "grosysid", "grolist"]),
            MetadataPlan::PgStatActivity => empty_query(vec![
                "datid",
                "datname",
                "pid",
                "leader_pid",
                "usesysid",
                "usename",
                "application_name",
                "client_addr",
                "client_hostname",
                "client_port",
                "backend_start",
                "xact_start",
                "query_start",
                "state_change",
                "wait_event_type",
                "wait_event",
                "state",
                "backend_xid",
                "backend_xmin",
                "query_id",
                "query",
                "backend_type",
            ]),
            MetadataPlan::PgLocks => empty_query(vec![
                "locktype",
                "database",
                "relation",
                "page",
                "tuple",
                "virtualxid",
                "transactionid",
                "classid",
                "objid",
                "objsubid",
                "virtualtransaction",
                "pid",
                "mode",
                "granted",
                "fastpath",
                "waitstart",
            ]),
        };

        Ok(Some(response))
    }

    async fn execute_exasol_query<C>(
        &self,
        client: &mut C,
        sql: &str,
    ) -> PgWireResult<GatewayResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = self.execute_exasol_sql(client, sql).await?;
        let mut responses = map_exasol_result(result)?;
        Ok(responses.pop().unwrap_or(GatewayResponse::Empty))
    }

    async fn fetch_query_rows<C>(
        &self,
        client: &mut C,
        sql: &str,
    ) -> PgWireResult<Vec<Vec<Option<String>>>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self.execute_exasol_sql(client, sql).await? {
            ExasolResult::ResultSet { rows, .. } => Ok(rows),
            ExasolResult::RowCount(_) => Err(pg_error(
                "XX000",
                "metadata query unexpectedly returned a row count",
            )),
        }
    }

    async fn execute_exasol_sql<C>(&self, client: &mut C, sql: &str) -> PgWireResult<ExasolResult>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = client
            .session_extensions()
            .get::<SessionState>()
            .ok_or_else(|| pg_error("08003", "Exasol session is not connected"))?;
        let sql = sql.to_owned();
        task::spawn_blocking(move || {
            let mut session = state
                .exasol
                .lock()
                .map_err(|_| ExasolError::Connection("Exasol session lock poisoned".to_owned()))?;
            session.execute(&sql)
        })
        .await
        .map_err(|err| pg_error("58000", format!("Exasol execution task failed: {err}")))?
        .map_err(map_exasol_execution_error)
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
        let sql = render_portal_sql(target)?;
        let mut responses = self.execute_statement(client, &sql).await?;
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
        let sql = match render_portal_sql(portal) {
            Ok(sql) => sql,
            Err(error) => return Ok(Response::Error(Box::new(error_info(error)))),
        };
        let mut responses = self.execute_statement(client, &sql).await?;
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

fn render_portal_sql(portal: &Portal<String>) -> PgWireResult<String> {
    let mut sql = portal.statement.statement.clone();
    for idx in (0..portal.parameter_len()).rev() {
        let placeholder = format!("${}", idx + 1);
        let value = render_portal_parameter(portal, idx)?;
        sql = sql.replace(&placeholder, &value);
    }
    Ok(sql)
}

fn render_portal_parameter(portal: &Portal<String>, idx: usize) -> PgWireResult<String> {
    let value = portal
        .parameters
        .get(idx)
        .ok_or_else(|| pg_error("08P01", format!("missing portal parameter {}", idx + 1)))?;

    let Some(bytes) = value else {
        return Ok("NULL".to_owned());
    };

    if portal.parameter_format.is_binary(idx) {
        return Err(pg_error(
            "0A000",
            "binary prepared statement parameters are not implemented",
        ));
    }

    let text = String::from_utf8(bytes.to_vec())
        .map_err(|err| pg_error("08P01", format!("invalid UTF-8 parameter: {err}")))?;
    Ok(sql_string_literal(&text))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn error_info(error: PgWireError) -> ErrorInfo {
    match error {
        PgWireError::UserError(info) => *info,
        other => ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), other.to_string()),
    }
}

fn empty_query(columns: Vec<&str>) -> GatewayResponse {
    GatewayResponse::Query {
        columns: columns.into_iter().map(str::to_owned).collect(),
        rows: Vec::new(),
    }
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
