#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROFILE="${EXAPUMP_PROFILE:-nc-personal-2}"

exapump sql --profile "${PROFILE}" < "${REPO_ROOT}/sql/sample_data.sql"

cat <<'MSG'
Demo environment installed.

Smoke query:
SELECT
  order_id,
  order_ts::DATE AS order_date,
  amount::DECIMAL(18, 2) AS amount_eur
FROM pg_demo.orders
WHERE customer_name ILIKE 'acme%'
ORDER BY order_id
LIMIT 3;

Additional browse checks:
SELECT schema_name, schema_owner
FROM sys.exa_dba_schemas
WHERE schema_name LIKE 'DEMO_%' OR schema_name = 'PG_DEMO'
ORDER BY schema_name;

SELECT object_type, COUNT(*) AS object_count
FROM sys.exa_dba_objects
WHERE root_name IN ('DEMO_SALES', 'DEMO_FINANCE', 'DEMO_SUPPORT', 'DEMO_SHARED', 'DEMO_SANDBOX', 'PG_DEMO')
GROUP BY object_type
ORDER BY object_type;
MSG
