-- =============================================================================
-- Build: streetiq_gold.us_nyc_last_transaction_engine_v3
-- US / NYC only — not used by France.
--
-- Source: streetiq_gold.us_nyc_raw_sales  (2000-01-01 onward)
--   Join key: full_address → us_nyc_card_output_v5.full_address
--
-- Purpose:
--   For every building address alias in card_output_v5, find the most recent
--   market-rate sale (price > 10000) from 2000 onward and format a
--   human-readable last-transaction line.
--
-- Coverage guarantee:
--   Address alias rows (CPW W↔WEST, ordinals, no-zip, directional short) are
--   generated here so every alias in card_output_v5 resolves a transaction row.
--
-- Transaction match levels:
--   'exact'            – sale record contains a unit number matching the query
--   'building_similar' – building-level sale (no unit in the record)
--
-- Run:
--   bq query --use_legacy_sql=false --project_id=streetiq-bigquery --location=EU \
--     < scripts/us/nyc/sql/build-us-nyc-last-transaction-engine-v3.sql
-- =============================================================================

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.us_nyc_last_transaction_engine_v3` AS

WITH

  -- ── 1. Raw sales from 2000-01-01 onward, market-rate only ────────────────
  sales_base AS (
    SELECT
      TRIM(SPLIT(NULLIF(TRIM(CAST(ADDRESS AS STRING)), ''), ',')[SAFE_OFFSET(0)])
        AS raw_building_addr,
      SAFE_CAST(ZIP_CODE            AS STRING)  AS zip_code,
      SAFE_CAST(SALE_PRICE          AS FLOAT64) AS sale_price,
      SAFE_CAST(GROSS_SQUARE_FEET   AS FLOAT64) AS gross_sqft,
      TRIM(CAST(BUILDING_CLASS_AT_TIME_OF_SALE AS STRING)) AS bldg_class,
      DATE(SAFE_CAST(SALE_DATE      AS TIMESTAMP)) AS sale_date,
      TRIM(CAST(APARTMENT_NUMBER    AS STRING))  AS apt_number
    FROM `streetiq-bigquery.streetiq_gold.us_nyc_raw_sales`
    WHERE ADDRESS IS NOT NULL
      AND TRIM(CAST(ADDRESS AS STRING)) != ''
      AND SAFE_CAST(SALE_PRICE AS FLOAT64) > 10000
      AND DATE(SAFE_CAST(SALE_DATE AS TIMESTAMP)) >= DATE('2000-01-01')
  ),

  -- ── 2. Address normalization (same pipeline as building_truth) ────────────
  normalized AS (
    SELECT
      *,
      TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          UPPER(TRIM(raw_building_addr)),
                          r'\s+', ' '),
                        -- street type abbreviations (RE2-safe: capture group)
                        r'(?i)\bST\.?(\s|$)',  'STREET\\1'),
                      r'(?i)\bAVE\.?(\s|$)',   'AVENUE\\1'),
                    -- CPW normalization
                    r'(?i)\bCENTRAL\s+PARK\s+W\b', 'CENTRAL PARK WEST'),
                  r'(?i)\bCENTRAL\s+PARK\s+E\b',   'CENTRAL PARK EAST'),
                -- directional expansion
                r'(?i)(^|\s)W\s+(\d)', 'WEST \\2'),
              r'(?i)(^|\s)E\s+(\d)',   'EAST \\2'),
            r'(?i)(^|\s)N\s+(\d)',     'NORTH \\2'),
          r'(?i)(^|\s)S\s+(\d)',       'SOUTH \\2')
      ) AS addr_canonical
    FROM sales_base
    WHERE raw_building_addr IS NOT NULL
      AND TRIM(raw_building_addr) != ''
  ),

  with_full_address AS (
    SELECT
      *,
      CASE
        WHEN zip_code IS NOT NULL AND TRIM(zip_code) NOT IN ('', '0')
          THEN CONCAT(addr_canonical, ', NEW YORK, NY ', TRIM(zip_code))
        ELSE CONCAT(addr_canonical, ', NEW YORK, NY')
      END AS full_address_canonical,
      CASE
        WHEN apt_number IS NOT NULL AND TRIM(apt_number) != '' THEN 'unit_exact'
        ELSE 'building_similar'
      END AS tx_match_level
    FROM normalized
  ),

  -- ── 3. Pick the most recent sale per canonical address ────────────────────
  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY full_address_canonical
        ORDER BY sale_date DESC, sale_price DESC
      ) AS rn
    FROM with_full_address
    WHERE full_address_canonical IS NOT NULL
  ),

  latest_per_canonical AS (
    SELECT * FROM ranked WHERE rn = 1
  ),

  -- ── 4. Format last transaction text ───────────────────────────────────────
  formatted AS (
    SELECT
      full_address_canonical                          AS full_address,
      sale_price,
      sale_date,
      tx_match_level,
      apt_number,

      CONCAT(
        'Last sale: $',
        CAST(CAST(ROUND(sale_price) AS INT64) AS STRING),
        IF(apt_number IS NOT NULL AND TRIM(apt_number) != '',
           CONCAT(' (Unit ', apt_number, ')'), ''),
        ' on ',
        FORMAT_DATE('%b %d, %Y', sale_date)
      ) AS final_last_transaction_text,

      CASE
        WHEN tx_match_level = 'unit_exact' THEN 'exact'
        ELSE 'building_similar'
      END AS final_transaction_match_level

    FROM latest_per_canonical
  ),

  -- ── 5. Address alias rows (mirrors card_output alias set) ─────────────────

  canonical_tx AS (
    SELECT * FROM formatted
  ),

  -- CPW: CENTRAL PARK WEST ↔ W
  cpw_tx_alias AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(full_address, r'\bCENTRAL PARK WEST\b', 'CENTRAL PARK W'),
        r'\bCENTRAL PARK EAST\b', 'CENTRAL PARK E') AS full_address,
      sale_price, sale_date, tx_match_level, apt_number,
      final_last_transaction_text, final_transaction_match_level
    FROM formatted
    WHERE REGEXP_CONTAINS(full_address, r'\bCENTRAL PARK (WEST|EAST)\b')
      AND REGEXP_REPLACE(
            REGEXP_REPLACE(full_address, r'\bCENTRAL PARK WEST\b', 'CENTRAL PARK W'),
            r'\bCENTRAL PARK EAST\b', 'CENTRAL PARK E') != full_address
  ),

  -- Ordinal stripped: 86TH → 86
  ordinal_stripped_tx AS (
    SELECT
      REGEXP_REPLACE(full_address, r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1') AS full_address,
      sale_price, sale_date, tx_match_level, apt_number,
      final_last_transaction_text, final_transaction_match_level
    FROM formatted
    WHERE REGEXP_CONTAINS(full_address, r'\b\d{1,3}(ST|ND|RD|TH)\b')
      AND REGEXP_REPLACE(full_address, r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1') != full_address
  ),

  -- No-zip
  no_zip_tx AS (
    SELECT
      REGEXP_REPLACE(full_address, r'\s+\d{5}\s*$', '') AS full_address,
      sale_price, sale_date, tx_match_level, apt_number,
      final_last_transaction_text, final_transaction_match_level
    FROM formatted
    WHERE REGEXP_CONTAINS(full_address, r'\s+\d{5}\s*$')
  ),

  -- CPW + no ordinal + no zip
  cpw_no_ordinal_no_zip_tx AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(full_address,
              r'\bCENTRAL PARK WEST\b', 'CENTRAL PARK W'),
            r'\bCENTRAL PARK EAST\b', 'CENTRAL PARK E'),
          r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1'),
        r'\s+\d{5}\s*$', '') AS full_address,
      sale_price, sale_date, tx_match_level, apt_number,
      final_last_transaction_text, final_transaction_match_level
    FROM formatted
    WHERE REGEXP_CONTAINS(full_address, r'\bCENTRAL PARK (WEST|EAST)\b')
  ),

  -- Directional short: WEST → W before street number
  dir_short_tx AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(full_address,
              r'\bWEST\s+(\d)',  'W \\1'),
            r'\bEAST\s+(\d)',   'E \\1'),
          r'\bNORTH\s+(\d)',  'N \\1'),
        r'\bSOUTH\s+(\d)',   'S \\1') AS full_address,
      sale_price, sale_date, tx_match_level, apt_number,
      final_last_transaction_text, final_transaction_match_level
    FROM formatted
    WHERE REGEXP_CONTAINS(full_address, r'^\d+\s+(WEST|EAST|NORTH|SOUTH)\s+\d')
      AND NOT REGEXP_CONTAINS(full_address, r'\bCENTRAL PARK\b')
  ),

  -- Directional short + ordinal stripped
  dir_short_no_ordinal_tx AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(full_address,
                r'\bWEST\s+(\d)',  'W \\1'),
              r'\bEAST\s+(\d)',   'E \\1'),
            r'\bNORTH\s+(\d)',  'N \\1'),
          r'\bSOUTH\s+(\d)',   'S \\1'),
        r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1') AS full_address,
      sale_price, sale_date, tx_match_level, apt_number,
      final_last_transaction_text, final_transaction_match_level
    FROM formatted
    WHERE REGEXP_CONTAINS(full_address, r'^\d+\s+(WEST|EAST|NORTH|SOUTH)\s+\d')
      AND NOT REGEXP_CONTAINS(full_address, r'\bCENTRAL PARK\b')
  ),

  all_tx AS (
    SELECT * FROM canonical_tx
    UNION ALL SELECT * FROM cpw_tx_alias
    UNION ALL SELECT * FROM ordinal_stripped_tx
    UNION ALL SELECT * FROM no_zip_tx
    UNION ALL SELECT * FROM cpw_no_ordinal_no_zip_tx
    UNION ALL SELECT * FROM dir_short_tx
    UNION ALL SELECT * FROM dir_short_no_ordinal_tx
  )

-- ── 6. Deduplicate by full_address, keep the most recent sale row ──────────
--   ANY_VALUE is safe here because all alias rows for one canonical address
--   share the same latest sale (derived from latest_per_canonical above).
SELECT
  full_address,
  MAX(sale_price)                          AS latest_sale_price,
  MAX(sale_date)                           AS latest_sale_date,
  ANY_VALUE(final_last_transaction_text)   AS final_last_transaction_text,
  ANY_VALUE(final_transaction_match_level) AS final_transaction_match_level
FROM all_tx
WHERE full_address IS NOT NULL
  AND TRIM(full_address) != ''
GROUP BY full_address;
