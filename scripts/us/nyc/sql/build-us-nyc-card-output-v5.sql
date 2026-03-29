-- =============================================================================
-- Build: streetiq_gold.us_nyc_card_output_v5
-- US / NYC only — not used by France.
--
-- Source: streetiq_gold.us_nyc_building_truth_v3  (must be built first)
--
-- Architecture: every row from building_truth_v3 survives into card_output.
--   No building is dropped because estimated_value / avg_sale_price is NULL.
--   Null-safe badge and display-text logic handles buildings with no sales.
--
-- Multi-unit pending-unit-prompt logic:
--   When unit_count > 1, final_match_level = 'building'.
--   The app (us-nyc-precomputed-card.ts / us-nyc-main-payload.ts) must gate
--   unit-sensitive fields (estimated_value, latest_sale_price, price_per_sqft)
--   behind the pending-unit-prompt check — NOT this SQL.
--   This table always emits all fields so the app can decide what to show.
--
-- Address alias strategy:
--   Generates MULTIPLE full_address rows per building so every candidate
--   string produced by us-nyc-address-normalize.ts hits an exact match:
--     canonical, CPW W↔WEST, ordinal stripped, no-zip, directional short, combinations.
--
-- Run:
--   bq query --use_legacy_sql=false --project_id=streetiq-bigquery --location=EU \
--     < scripts/us/nyc/sql/build-us-nyc-card-output-v5.sql
-- =============================================================================

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.us_nyc_card_output_v5` AS

WITH

  -- ── 1. Base: every row from building_truth (no filter on sales fields) ────
  base AS (
    SELECT
      full_address                          AS full_address_canonical,
      bbl,
      building_type,
      CAST(COALESCE(unit_count, 1) AS INT64) AS unit_count,
      avg_sale_price,
      max_sale_price,
      latest_sale_date,
      COALESCE(sale_count, 0)               AS sale_count,
      avg_ppsf
    FROM `streetiq-bigquery.streetiq_gold.us_nyc_building_truth_v3`
    WHERE full_address IS NOT NULL
      AND TRIM(full_address) != ''
  ),

  -- ── 2. Extract zip for alias generation ──────────────────────────────────
  parts AS (
    SELECT
      *,
      REGEXP_EXTRACT(full_address_canonical, r'\b(\d{5})\s*$') AS zip_extracted
    FROM base
  ),

  -- ── 3. Badge + display fields ─────────────────────────────────────────────
  with_display AS (
    SELECT
      *,

      -- badge_1: building category
      CASE building_type
        WHEN 'single_family'     THEN 'Single Family'
        WHEN 'two_family'        THEN 'Two Family'
        WHEN 'small_multifamily' THEN 'Walk-Up Apartments'
        WHEN 'large_multifamily' THEN 'Elevator Apartments'
        WHEN 'co_op'             THEN 'Co-op Building'
        WHEN 'condo'             THEN 'Condo Building'
        WHEN 'mixed_use'         THEN 'Mixed Use'
        WHEN 'vacant'            THEN 'Vacant Land'
        ELSE                          'Residential Building'
      END AS badge_1,

      -- badge_2: unit count
      CASE
        WHEN unit_count IS NULL OR unit_count <= 0 THEN NULL
        WHEN unit_count = 1                        THEN '1 Unit'
        WHEN unit_count <= 20                      THEN CONCAT(CAST(unit_count AS STRING), ' Units')
        ELSE                                            CONCAT(CAST(unit_count AS STRING), '+ Units')
      END AS badge_2,

      -- badge_3: estimated value (rounded to nearest $10k); NULL when no sales
      CASE
        WHEN avg_sale_price IS NULL OR avg_sale_price <= 0 THEN NULL
        WHEN avg_sale_price < 1000000
          THEN CONCAT('~$', CAST(CAST(ROUND(avg_sale_price / 10000) * 10 AS INT64) AS STRING), 'K')
        ELSE CONCAT('~$', FORMAT('%.*f', 2, avg_sale_price / 1000000), 'M')
      END AS badge_3,

      -- badge_4: sale count
      CASE
        WHEN sale_count IS NULL OR sale_count = 0 THEN NULL
        WHEN sale_count = 1                        THEN '1 Sale on Record'
        ELSE                                            CONCAT(CAST(sale_count AS STRING), ' Sales on Record')
      END AS badge_4,

      -- estimated_value: INT64 or NULL (app gates display for multi-unit + no-unit)
      CASE
        WHEN avg_sale_price IS NOT NULL AND avg_sale_price > 0
          THEN CAST(ROUND(avg_sale_price) AS INT64)
        ELSE NULL
      END AS estimated_value,

      -- estimated_value_subtext
      CASE
        WHEN avg_sale_price IS NOT NULL AND avg_sale_price > 0 AND sale_count > 1
          THEN CONCAT('Based on ', CAST(sale_count AS STRING), ' recorded sales')
        WHEN avg_sale_price IS NOT NULL AND avg_sale_price > 0
          THEN 'Based on last recorded sale'
        ELSE NULL
      END AS estimated_value_subtext,

      -- price_per_sqft_text
      CASE
        WHEN avg_ppsf IS NOT NULL AND avg_ppsf > 0
          THEN CONCAT('$', CAST(CAST(ROUND(avg_ppsf) AS INT64) AS STRING), '/sqft avg')
        ELSE NULL
      END AS price_per_sqft_text,

      -- final_match_level
      CASE
        WHEN unit_count IS NOT NULL AND unit_count > 1 THEN 'building'
        ELSE 'property'
      END AS final_match_level

    FROM parts
  ),

  -- ── 4. Canonical rows ─────────────────────────────────────────────────────
  canonical_rows AS (
    SELECT
      full_address_canonical AS full_address,
      badge_1, badge_2, badge_3, badge_4,
      estimated_value, estimated_value_subtext,
      price_per_sqft_text, final_match_level,
      building_type, unit_count
    FROM with_display
  ),

  -- ── 5. CPW alias: CENTRAL PARK WEST ↔ W ─────────────────────────────────
  cpw_alias_rows AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(full_address_canonical,
          r'\bCENTRAL PARK WEST\b', 'CENTRAL PARK W'),
        r'\bCENTRAL PARK EAST\b', 'CENTRAL PARK E') AS full_address,
      badge_1, badge_2, badge_3, badge_4,
      estimated_value, estimated_value_subtext,
      price_per_sqft_text, final_match_level,
      building_type, unit_count
    FROM with_display
    WHERE REGEXP_CONTAINS(full_address_canonical, r'\bCENTRAL PARK (WEST|EAST)\b')
      AND REGEXP_REPLACE(
            REGEXP_REPLACE(full_address_canonical,
              r'\bCENTRAL PARK WEST\b', 'CENTRAL PARK W'),
            r'\bCENTRAL PARK EAST\b', 'CENTRAL PARK E')
          != full_address_canonical
  ),

  -- ── 6. Ordinal stripped: 86TH → 86 ───────────────────────────────────────
  ordinal_stripped_rows AS (
    SELECT
      REGEXP_REPLACE(full_address_canonical, r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1')
        AS full_address,
      badge_1, badge_2, badge_3, badge_4,
      estimated_value, estimated_value_subtext,
      price_per_sqft_text, final_match_level,
      building_type, unit_count
    FROM with_display
    WHERE REGEXP_CONTAINS(full_address_canonical, r'\b\d{1,3}(ST|ND|RD|TH)\b')
      AND REGEXP_REPLACE(full_address_canonical,
            r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1') != full_address_canonical
  ),

  -- ── 7. No-zip alias ───────────────────────────────────────────────────────
  no_zip_rows AS (
    SELECT
      REGEXP_REPLACE(full_address_canonical, r'\s+\d{5}\s*$', '') AS full_address,
      badge_1, badge_2, badge_3, badge_4,
      estimated_value, estimated_value_subtext,
      price_per_sqft_text, final_match_level,
      building_type, unit_count
    FROM with_display
    WHERE REGEXP_CONTAINS(full_address_canonical, r'\s+\d{5}\s*$')
  ),

  -- ── 8. CPW + no ordinal + no zip ─────────────────────────────────────────
  cpw_alias_no_ordinal AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(full_address_canonical,
              r'\bCENTRAL PARK WEST\b', 'CENTRAL PARK W'),
            r'\bCENTRAL PARK EAST\b', 'CENTRAL PARK E'),
          r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1'),
        r'\s+\d{5}\s*$', '') AS full_address,
      badge_1, badge_2, badge_3, badge_4,
      estimated_value, estimated_value_subtext,
      price_per_sqft_text, final_match_level,
      building_type, unit_count
    FROM with_display
    WHERE REGEXP_CONTAINS(full_address_canonical, r'\bCENTRAL PARK (WEST|EAST)\b')
  ),

  -- ── 9. No-zip + ordinal stripped ──────────────────────────────────────────
  no_zip_no_ordinal_rows AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(full_address_canonical,
          r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1'),
        r'\s+\d{5}\s*$', '') AS full_address,
      badge_1, badge_2, badge_3, badge_4,
      estimated_value, estimated_value_subtext,
      price_per_sqft_text, final_match_level,
      building_type, unit_count
    FROM with_display
    WHERE REGEXP_CONTAINS(full_address_canonical, r'\b\d{1,3}(ST|ND|RD|TH)\b')
      AND REGEXP_CONTAINS(full_address_canonical, r'\s+\d{5}\s*$')
  ),

  -- ── 10. Directional short form: WEST → W before street number ─────────────
  directional_short_rows AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(full_address_canonical,
              r'\bWEST\s+(\d)',  'W \\1'),
            r'\bEAST\s+(\d)',   'E \\1'),
          r'\bNORTH\s+(\d)',  'N \\1'),
        r'\bSOUTH\s+(\d)',   'S \\1') AS full_address,
      badge_1, badge_2, badge_3, badge_4,
      estimated_value, estimated_value_subtext,
      price_per_sqft_text, final_match_level,
      building_type, unit_count
    FROM with_display
    WHERE REGEXP_CONTAINS(full_address_canonical, r'^\d+\s+(WEST|EAST|NORTH|SOUTH)\s+\d')
      AND NOT REGEXP_CONTAINS(full_address_canonical, r'\bCENTRAL PARK\b')
  ),

  -- ── 11. Directional short + ordinal stripped ──────────────────────────────
  directional_short_no_ordinal AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(full_address_canonical,
                r'\bWEST\s+(\d)',  'W \\1'),
              r'\bEAST\s+(\d)',   'E \\1'),
            r'\bNORTH\s+(\d)',  'N \\1'),
          r'\bSOUTH\s+(\d)',   'S \\1'),
        r'\b(\d{1,3})(ST|ND|RD|TH)\b', '\\1') AS full_address,
      badge_1, badge_2, badge_3, badge_4,
      estimated_value, estimated_value_subtext,
      price_per_sqft_text, final_match_level,
      building_type, unit_count
    FROM with_display
    WHERE REGEXP_CONTAINS(full_address_canonical, r'^\d+\s+(WEST|EAST|NORTH|SOUTH)\s+\d')
      AND NOT REGEXP_CONTAINS(full_address_canonical, r'\bCENTRAL PARK\b')
  ),

  -- ── 12. UNION all alias forms ─────────────────────────────────────────────
  all_rows AS (
    SELECT * FROM canonical_rows
    UNION ALL SELECT * FROM cpw_alias_rows
    UNION ALL SELECT * FROM ordinal_stripped_rows
    UNION ALL SELECT * FROM no_zip_rows
    UNION ALL SELECT * FROM cpw_alias_no_ordinal
    UNION ALL SELECT * FROM no_zip_no_ordinal_rows
    UNION ALL SELECT * FROM directional_short_rows
    UNION ALL SELECT * FROM directional_short_no_ordinal
  )

-- Final: deduplicate by full_address; prefer rows with non-null estimated_value
SELECT
  full_address,
  ANY_VALUE(badge_1)                    AS badge_1,
  ANY_VALUE(badge_2)                    AS badge_2,
  ANY_VALUE(badge_3)                    AS badge_3,
  ANY_VALUE(badge_4)                    AS badge_4,
  MAX(estimated_value)                  AS estimated_value,
  ANY_VALUE(estimated_value_subtext)    AS estimated_value_subtext,
  ANY_VALUE(price_per_sqft_text)        AS price_per_sqft_text,
  ANY_VALUE(final_match_level)          AS final_match_level,
  ANY_VALUE(building_type)              AS building_type,
  MAX(unit_count)                       AS unit_count
FROM all_rows
WHERE full_address IS NOT NULL
  AND TRIM(full_address) != ''
GROUP BY full_address;
