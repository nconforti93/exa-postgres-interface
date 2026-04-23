# Implementation Notes

## Runtime Selection

The first prototype uses Python 3.11.

Reasons:

* The repository started without source files, so a compact Python scaffold keeps
  the first implementation inspectable.
* The SQL translation requirement already depends on Python and `sqlglot`.
* The PostgreSQL simple-query protocol can be implemented directly for the
  smoke-test scope while keeping protocol boundaries explicit.
* The Exasol connection is isolated behind a backend interface so the runtime
  can be replaced later if broader protocol compatibility requires a different
  stack.

Tradeoffs:

* This is not a full PostgreSQL server implementation.
* Extended query protocol and rich PostgreSQL metadata behavior remain future
  work.
* Production packaging is represented by Python console-script packaging plus a
  systemd unit template, not a standalone native binary.

## Architecture

The implementation separates:

* PostgreSQL wire messages in `messages.py`.
* Startup/authentication/query loop in `server.py`.
* Statement policy in `policy.py`.
* Statement routing and error mapping in `gateway.py`.
* Exasol connection/session handling in `backend.py`.

The execution result model distinguishes row-returning query results from
command-completion results so future write support can add DML/DDL behavior
without replacing the protocol response path.
