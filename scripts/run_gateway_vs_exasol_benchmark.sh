#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 6 ]]; then
    cat <<'USAGE' >&2
usage: scripts/run_gateway_vs_exasol_benchmark.sh \
  <gateway-jdbc-url> <gateway-user> <gateway-password> \
  <direct-exasol-jdbc-url> <direct-user> <direct-password> [benchmark-args...]

Example:
  scripts/run_gateway_vs_exasol_benchmark.sh \
    'jdbc:postgresql://127.0.0.1:15432/exasol?preferQueryMode=extended' \
    sys \
    'EXASOL_PASSWORD' \
    'jdbc:exa:127.0.0.1:8563' \
    sys \
    'EXASOL_PASSWORD' \
    --warmup=3 \
    --iterations=10
USAGE
    exit 1
fi

if ! command -v java >/dev/null 2>&1; then
    echo "java is required to run the benchmark" >&2
    exit 1
fi

if ! command -v javac >/dev/null 2>&1; then
    echo "javac is required to compile the benchmark" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${TMPDIR:-/tmp}/exa-postgres-interface-benchmark"
PG_JDBC_JAR="${PG_JDBC_JAR:-${BUILD_DIR}/postgresql-42.7.8.jar}"
EXASOL_JDBC_JAR="${EXASOL_JDBC_JAR:-${BUILD_DIR}/exasol-jdbc-25.2.4.jar}"

mkdir -p "${BUILD_DIR}"

if [[ ! -f "${PG_JDBC_JAR}" ]]; then
    curl -fsSL -o "${PG_JDBC_JAR}" https://jdbc.postgresql.org/download/postgresql-42.7.8.jar
fi

if [[ ! -f "${EXASOL_JDBC_JAR}" ]]; then
    curl -fsSL -o "${EXASOL_JDBC_JAR}" https://repo1.maven.org/maven2/com/exasol/exasol-jdbc/25.2.4/exasol-jdbc-25.2.4.jar
fi

javac \
    -cp "${PG_JDBC_JAR}:${EXASOL_JDBC_JAR}" \
    -d "${BUILD_DIR}" \
    "${REPO_ROOT}/tests/jdbc/GatewayVsDirectExasolBenchmark.java"

java \
    -cp "${PG_JDBC_JAR}:${EXASOL_JDBC_JAR}:${BUILD_DIR}" \
    GatewayVsDirectExasolBenchmark \
    "$@"
