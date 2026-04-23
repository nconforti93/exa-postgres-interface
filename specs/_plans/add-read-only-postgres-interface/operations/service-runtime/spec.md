# Feature: Service Runtime

The prototype SHOULD be installable as a long-running server process on Linux. The preferred operating model is a binary managed by systemd with external configuration and observable logs.

## Background

* The application runs between PostgreSQL-compatible clients and Exasol.
* The prototype is expected to run on Linux.
* Secrets SHALL NOT be committed to the repository.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Operator starts the protocol server as a binary

* *GIVEN* the application has been built for the target Linux environment
* *WHEN* the operator starts the server binary with a valid configuration
* *THEN* the server SHALL listen on the configured PostgreSQL protocol address and port
* *AND* the server SHALL log startup configuration details that are safe to expose
* *AND* the server SHALL NOT log plaintext passwords or secrets

<!-- DELTA:NEW -->
### Scenario: Operator configures Exasol connectivity

* *GIVEN* the operator provides server configuration
* *WHEN* the server starts
* *THEN* the configuration SHALL include the Exasol endpoint needed to create client sessions
* *AND* the configuration SHOULD allow client-supplied credentials to be passed through to Exasol
* *AND* the configuration SHALL identify how the Python SQL preprocessor is installed, selected, or initialized

<!-- DELTA:NEW -->
### Scenario: Operator runs the protocol server through systemd

* *GIVEN* the server binary and configuration have been installed on a Linux host
* *WHEN* the operator enables and starts the provided systemd service
* *THEN* systemd SHOULD manage the server process lifecycle
* *AND* service logs SHOULD be available through standard Linux service logging tools
* *AND* the service definition SHALL keep deployment-specific secrets outside the repository
