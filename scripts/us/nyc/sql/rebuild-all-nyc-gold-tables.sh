#!/usr/bin/env bash
# =============================================================================
# Rebuild all NYC gold tables (US only — not France).
# Runs SQL scripts in dependency order.
#
# Prerequisites:
#   - Google Cloud SDK installed: gcloud, bq
#   - Authenticated: gcloud auth application-default login
#   - BIGQUERY_PROJECT_ID env var set (or edit PROJECT below)
#   - us_nyc_raw_sales table populated (2000+ data) in streetiq_gold dataset
#   - us_nyc_pluto_normalized table populated (required — PLUTO-driven ETL)
#
# Usage (Git Bash or WSL on Windows):
#   BIGQUERY_PROJECT_ID=streetiq-bigquery \
#     bash scripts/us/nyc/sql/rebuild-all-nyc-gold-tables.sh
#
# Usage (PowerShell — pipe each file individually):
#   See PowerShell commands below in comments.
# =============================================================================

set -euo pipefail

PROJECT="${BIGQUERY_PROJECT_ID:-streetiq-bigquery}"
LOCATION="EU"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BQ="bq query --use_legacy_sql=false --project_id=${PROJECT} --location=${LOCATION}"

echo "========================================"
echo " NYC Gold Table Rebuild"
echo " Project : ${PROJECT}"
echo " Location: ${LOCATION}"
echo " Scripts : ${SCRIPT_DIR}"
echo "========================================"
echo ""

# ── Step 1: PLUTO-driven building truth ──────────────────────────────────────
echo "[1/3] us_nyc_building_truth_v3 ..."
${BQ} < "${SCRIPT_DIR}/build-us-nyc-building-truth-v3.sql"
echo "      Done."

# ── Step 2: Card output (every building_truth row survives) ──────────────────
echo "[2/3] us_nyc_card_output_v5 ..."
${BQ} < "${SCRIPT_DIR}/build-us-nyc-card-output-v5.sql"
echo "      Done."

# ── Step 3: Last transaction engine (2000+ history) ──────────────────────────
echo "[3/3] us_nyc_last_transaction_engine_v3 ..."
${BQ} < "${SCRIPT_DIR}/build-us-nyc-last-transaction-engine-v3.sql"
echo "      Done."

echo ""
echo "========================================"
echo " All NYC gold tables rebuilt."
echo "========================================"
echo ""

# ── Row-count verification ────────────────────────────────────────────────────
echo "--- Row counts ---"
${BQ} --format=prettyjson "
  SELECT
    'us_nyc_building_truth_v3'         AS table_name,
    COUNT(*)                           AS row_count
  FROM \`${PROJECT}.streetiq_gold.us_nyc_building_truth_v3\`
  UNION ALL
  SELECT
    'us_nyc_card_output_v5',
    COUNT(*)
  FROM \`${PROJECT}.streetiq_gold.us_nyc_card_output_v5\`
  UNION ALL
  SELECT
    'us_nyc_last_transaction_engine_v3',
    COUNT(*)
  FROM \`${PROJECT}.streetiq_gold.us_nyc_last_transaction_engine_v3\`"

echo ""

# ── Address spot-checks ───────────────────────────────────────────────────────
echo "--- Address spot-checks (card_output_v5) ---"
for addr in \
  "234 CENTRAL PARK WEST, NEW YORK, NY 10024" \
  "234 CENTRAL PARK W, NEW YORK, NY 10024" \
  "40 WEST 86TH STREET, NEW YORK, NY 10024" \
  "40 W 86TH STREET, NEW YORK, NY 10024" \
  "40 WEST 86 STREET, NEW YORK, NY 10024" \
  "245 EAST 63RD STREET, NEW YORK, NY 10065" \
  "245 E 63RD STREET, NEW YORK, NY 10065" \
  "245 EAST 63 STREET, NEW YORK, NY 10065"
do
  echo ""
  echo "  ${addr}"
  ${BQ} --format=prettyjson "
    SELECT full_address, building_type, unit_count, estimated_value, final_match_level
    FROM \`${PROJECT}.streetiq_gold.us_nyc_card_output_v5\`
    WHERE full_address = '${addr}'
    LIMIT 1"
done

echo ""
echo "--- Last transaction spot-checks ---"
for addr in \
  "234 CENTRAL PARK WEST, NEW YORK, NY 10024" \
  "40 WEST 86TH STREET, NEW YORK, NY 10024" \
  "245 EAST 63RD STREET, NEW YORK, NY 10065"
do
  echo ""
  echo "  ${addr}"
  ${BQ} --format=prettyjson "
    SELECT full_address, latest_sale_price, latest_sale_date, final_last_transaction_text
    FROM \`${PROJECT}.streetiq_gold.us_nyc_last_transaction_engine_v3\`
    WHERE full_address = '${addr}'
    LIMIT 1"
done

echo ""
echo "========================================"
echo " Verification complete."
echo "========================================"
