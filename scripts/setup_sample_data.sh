#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROFILE="${EXAPUMP_PROFILE:-nc-personal-2}"

exapump sql --profile "${PROFILE}" < "${REPO_ROOT}/sql/sample_data.sql"

cat <<'MSG'
Sample data installed.

Smoke query:
SELECT
  order_id,
  order_ts::DATE AS order_date,
  amount::DECIMAL(18, 2) AS amount_eur
FROM pg_demo.orders
WHERE customer_name ILIKE 'acme%'
ORDER BY order_id
LIMIT 3;
MSG
