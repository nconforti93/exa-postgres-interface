from __future__ import annotations

import logging

from .backend import BackendError, BackendSession
from .messages import CommandResult, QueryResult
from .policy import StatementCategory, classify_statement

LOG = logging.getLogger(__name__)


class GatewayError(RuntimeError):
    def __init__(self, message: str, sqlstate: str = "0A000") -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


class StatementGateway:
    def __init__(self, backend: BackendSession) -> None:
        self.backend = backend

    def execute(self, sql: str) -> QueryResult | CommandResult:
        decision = classify_statement(sql)
        if decision.category is StatementCategory.EMPTY:
            return CommandResult("")
        if decision.category is StatementCategory.CLIENT_SESSION:
            LOG.info("acknowledging PostgreSQL client session command locally")
            return CommandResult("SET")
        if not decision.allowed:
            LOG.warning("rejecting unsupported SQL: category=%s reason=%s", decision.category, decision.reason)
            raise GatewayError(decision.reason)

        try:
            return self.backend.execute_read(sql)
        except BackendError as exc:
            raise GatewayError(str(exc), exc.sqlstate) from exc
