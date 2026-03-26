-- =============================================================================
-- Rebuild: streetiq_gold.us_nyc_street_pricing
-- US / NYC only — not used by France.
--
-- Source: streetiq_gold.us_nyc_raw_sales
-- Schema (verified): ADDRESS, SALE_PRICE, GROSS_SQUARE_FEET
-- Street line: first segment before comma (drops unit/suffix after comma).
--
-- Normalization (inline SQL only — not us_nyc_api_truth):
-- uppercase, strip leading house #, suffix abbreviations, ordinal strip (42ND -> 42).
--
-- Run (example):
--   bq query --use_legacy_sql=false --project_id=streetiq-bigquery --location=EU < scripts/us/nyc/sql/rebuild-us-nyc-street-pricing.sql
-- =============================================================================

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.us_nyc_street_pricing` AS
WITH
  -- ---------------------------------------------------------------------------
  -- One row per raw sale: street from ADDRESS (before first comma).
  -- ---------------------------------------------------------------------------
  base AS (
    SELECT
      TRIM(SPLIT(NULLIF(TRIM(CAST(ADDRESS AS STRING)), ''), ',')[SAFE_OFFSET(0)]) AS raw_street,
      SAFE_CAST(SALE_PRICE AS FLOAT64) AS sale_price,
      SAFE_CAST(GROSS_SQUARE_FEET AS FLOAT64) AS gross_square_feet
    FROM `streetiq-bigquery.streetiq_gold.us_nyc_raw_sales`
    WHERE SALE_PRICE IS NOT NULL
      AND SAFE_CAST(SALE_PRICE AS FLOAT64) > 0
      AND ADDRESS IS NOT NULL
      AND TRIM(CAST(ADDRESS AS STRING)) != ''
      AND TRIM(SPLIT(NULLIF(TRIM(CAST(ADDRESS AS STRING)), ''), ',')[SAFE_OFFSET(0)]) != ''
  ),

  -- Strip leading house number token (e.g. "123 W 42 ST" -> "W 42 ST").
  stripped_house AS (
    SELECT
      REGEXP_REPLACE(TRIM(UPPER(raw_street)), r'^\d+[A-Z]?\s+', '') AS upper_no_house,
      sale_price,
      SAFE_DIVIDE(sale_price, NULLIF(gross_square_feet, 0)) AS price_per_sqft
    FROM base
  ),

  collapsed AS (
    SELECT
      TRIM(REGEXP_REPLACE(upper_no_house, r'\s+', ' ')) AS s0,
      sale_price,
      price_per_sqft
    FROM stripped_house
    WHERE TRIM(upper_no_house) != ''
  ),

  -- Long multi-word tokens first (BOULEVARD, PARKWAY, TERRACE, then STREET, etc.).
  -- Ten nested REGEXP_REPLACE calls: BLVD … LANE, then ROAD as the outermost.
  suffixed AS (
    SELECT
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          s0,
                          r'(?i)\bBOULEVARD\b',
                          'BLVD'
                        ),
                        r'(?i)\bPARKWAY\b',
                        'PKWY'
                      ),
                      r'(?i)\bTERRACE\b',
                      'TER'
                    ),
                    r'(?i)\bSTREET\b',
                    'ST'
                  ),
                  r'(?i)\bAVENUE\b',
                  'AVE'
                ),
                r'(?i)\bPLACE\b',
                'PL'
              ),
              r'(?i)\bDRIVE\b',
              'DR'
            ),
            r'(?i)\bCOURT\b',
            'CT'
          ),
          r'(?i)\bLANE\b',
          'LN'
        ),
        r'(?i)\bROAD\b',
        'RD'
      ) AS s1,
      sale_price,
      price_per_sqft
    FROM collapsed
  ),

  normalized AS (
    SELECT
      REGEXP_REPLACE(s1, r'(?i)\b(\d+)(ST|ND|RD|TH)\b', '\\1') AS normalized_street_name,
      sale_price,
      price_per_sqft
    FROM suffixed
  )

SELECT
  normalized_street_name AS street_name,
  AVG(sale_price) AS avg_street_price,
  AVG(IF(price_per_sqft IS NOT NULL AND price_per_sqft > 0, price_per_sqft, NULL)) AS avg_street_price_per_sqft,
  COUNT(*) AS transaction_count
FROM normalized
WHERE normalized_street_name IS NOT NULL
  AND TRIM(normalized_street_name) != ''
GROUP BY normalized_street_name
HAVING COUNT(*) >= 1;
