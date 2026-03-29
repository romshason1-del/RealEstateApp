-- =============================================================================
-- Build: streetiq_gold.us_nyc_building_truth_v3
-- US / NYC only — not used by France.
--
-- Architecture: PLUTO-DRIVEN (v3 redesign)
--   PLUTO is the authoritative source of building enumeration.
--   Every lot in us_nyc_pluto_normalized becomes a row in this table.
--   Sales (us_nyc_raw_sales, all years from 2000-01-01 onward) are LEFT JOIN
--   enrichment only — a building is never dropped because sales are absent.
--
-- Why this matters:
--   The old sales-driven ETL silently dropped buildings with no recent
--   transactions (e.g. 234 Central Park W co-op buildings that rarely sell).
--   PLUTO-driven enumeration guarantees complete coverage.
--
-- Address normalization (mirrors us-nyc-address-normalize.ts):
--   1. Strip unit suffix (everything from first comma onward in ADDRESS).
--   2. UPPER + collapse spaces.
--   3. Expand street type abbreviations (ST→STREET, AVE→AVENUE, etc.)
--      using RE2-safe capture-group patterns (no lookaheads).
--   4. Normalize CENTRAL PARK W → CENTRAL PARK WEST.
--   5. Expand W/E/N/S directional prefix before street numbers.
--   6. Append ", NEW YORK, NY <zip>" suffix.
--
-- Run:
--   bq query --use_legacy_sql=false --project_id=streetiq-bigquery --location=EU \
--     < scripts/us/nyc/sql/build-us-nyc-building-truth-v3.sql
-- =============================================================================

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.us_nyc_building_truth_v3` AS

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1 – PLUTO: enumerate every building (primary driver)
-- ─────────────────────────────────────────────────────────────────────────────
WITH

  -- ── 1a. Raw PLUTO: typed columns + clean address upper ───────────────────
  pluto_base AS (
    SELECT
      TRIM(REGEXP_REPLACE(UPPER(COALESCE(address, '')), r'\s+', ' '))
        AS raw_addr_upper,
      TRIM(CAST(postcode AS STRING))                     AS zip_code,
      SAFE_CAST(borocode AS INT64)                       AS borough_code,
      SAFE_CAST(`Tax block` AS INT64)                    AS block,
      SAFE_CAST(`Tax lot` AS INT64)                      AS lot,
      TRIM(CAST(bldgclass AS STRING))                    AS bldg_class,
      SAFE_CAST(unitsres AS INT64)                       AS units_res,
      SAFE_CAST(unitstotal AS INT64)                     AS units_total
    FROM `streetiq-bigquery.streetiq_gold.us_nyc_pluto_normalized`
    WHERE address IS NOT NULL
      AND TRIM(CAST(address AS STRING)) != ''
  ),

  -- ── 1b. PLUTO address normalization ──────────────────────────────────────
  pluto_norm AS (
    SELECT
      *,
      -- Step 1: street-type expansion (RE2-safe: capture group, no lookahead)
      TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    raw_addr_upper,
                    r'(?i)\bBLVD\.?\b',       'BOULEVARD'),
                  r'(?i)\bAVE\.?\b',          'AVENUE'),
                r'(?i)\bSTE\.?\b',            'SUITE'),
              r'(?i)\bPL\.?(\s|$)',           'PLACE\\1'),
            r'(?i)\bCT\.?(\s|$)',            'COURT\\1'),
          r'(?i)\bST\.?(\s|$)',             'STREET\\1')
      ) AS addr_abbrev_expanded
    FROM pluto_base
    WHERE raw_addr_upper != ''
  ),

  pluto_canonical AS (
    SELECT
      *,
      -- Step 2: CPW normalization + directional expansion
      TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    addr_abbrev_expanded,
                    r'(?i)\bCENTRAL\s+PARK\s+W\b', 'CENTRAL PARK WEST'),
                  r'(?i)\bCENTRAL\s+PARK\s+E\b',   'CENTRAL PARK EAST'),
                r'(?i)(^|\s)W\s+(\d)',              'WEST \\2'),
              r'(?i)(^|\s)E\s+(\d)',                'EAST \\2'),
            r'(?i)(^|\s)N\s+(\d)',                  'NORTH \\2'),
          r'(?i)(^|\s)S\s+(\d)',                    'SOUTH \\2')
      ) AS addr_canonical,
      -- BBL: borough-block-lot key
      CONCAT(
        CAST(borough_code AS STRING), '-',
        LPAD(CAST(block AS STRING), 5, '0'), '-',
        LPAD(CAST(lot   AS STRING), 4, '0')
      ) AS bbl
    FROM pluto_norm
  ),

  pluto_enriched AS (
    SELECT
      *,
      CASE
        WHEN zip_code IS NOT NULL AND TRIM(zip_code) NOT IN ('', '0')
          THEN CONCAT(addr_canonical, ', NEW YORK, NY ', zip_code)
        ELSE CONCAT(addr_canonical, ', NEW YORK, NY')
      END AS full_address_canonical,
      -- Building type from PLUTO class code
      CASE
        WHEN REGEXP_CONTAINS(bldg_class, r'^A')      THEN 'single_family'
        WHEN REGEXP_CONTAINS(bldg_class, r'^B')      THEN 'two_family'
        WHEN bldg_class IN ('C4','C6')               THEN 'co_op'
        WHEN REGEXP_CONTAINS(bldg_class, r'^C')      THEN 'small_multifamily'
        WHEN bldg_class IN ('D0','D4')               THEN 'co_op'
        WHEN REGEXP_CONTAINS(bldg_class, r'^D')      THEN 'large_multifamily'
        WHEN REGEXP_CONTAINS(bldg_class, r'^R')      THEN 'condo'
        WHEN REGEXP_CONTAINS(bldg_class, r'^S')      THEN 'mixed_use'
        WHEN REGEXP_CONTAINS(bldg_class, r'^K')      THEN 'mixed_use'
        WHEN REGEXP_CONTAINS(bldg_class, r'^V')      THEN 'vacant'
        ELSE 'unknown'
      END AS building_type_pluto
    FROM pluto_canonical
  ),

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2 – Sales: aggregate from 2000-01-01 onward (LEFT JOIN enrichment)
-- ─────────────────────────────────────────────────────────────────────────────

  -- ── 2a. Raw sales: typed columns, all records from 2000+ ─────────────────
  sales_raw AS (
    SELECT
      TRIM(SPLIT(NULLIF(TRIM(CAST(ADDRESS AS STRING)), ''), ',')[SAFE_OFFSET(0)])
        AS raw_building_addr,
      SAFE_CAST(ZIP_CODE         AS STRING)  AS zip_code,
      SAFE_CAST(BOROUGH          AS INT64)   AS borough_code,
      SAFE_CAST(BLOCK            AS INT64)   AS block,
      SAFE_CAST(LOT              AS INT64)   AS lot,
      SAFE_CAST(TOTAL_UNITS      AS INT64)   AS total_units,
      TRIM(CAST(BUILDING_CLASS_AT_TIME_OF_SALE AS STRING)) AS bldg_class,
      SAFE_CAST(SALE_PRICE       AS FLOAT64) AS sale_price,
      SAFE_CAST(GROSS_SQUARE_FEET AS FLOAT64) AS gross_sqft,
      DATE(SAFE_CAST(SALE_DATE   AS TIMESTAMP)) AS sale_date
    FROM `streetiq-bigquery.streetiq_gold.us_nyc_raw_sales`
    WHERE ADDRESS IS NOT NULL
      AND TRIM(CAST(ADDRESS AS STRING)) != ''
      AND DATE(SAFE_CAST(SALE_DATE AS TIMESTAMP)) >= DATE('2000-01-01')
  ),

  -- ── 2b. Sales address normalization (same pipeline as PLUTO) ─────────────
  sales_norm AS (
    SELECT
      *,
      TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    TRIM(REGEXP_REPLACE(UPPER(raw_building_addr), r'\s+', ' ')),
                    r'(?i)\bBLVD\.?\b',   'BOULEVARD'),
                  r'(?i)\bAVE\.?\b',      'AVENUE'),
                r'(?i)\bSTE\.?\b',        'SUITE'),
              r'(?i)\bPL\.?(\s|$)',       'PLACE\\1'),
            r'(?i)\bCT\.?(\s|$)',        'COURT\\1'),
          r'(?i)\bST\.?(\s|$)',         'STREET\\1')
      ) AS addr_abbrev_expanded
    FROM sales_raw
    WHERE raw_building_addr IS NOT NULL
      AND TRIM(raw_building_addr) != ''
  ),

  sales_canonical AS (
    SELECT
      *,
      TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    addr_abbrev_expanded,
                    r'(?i)\bCENTRAL\s+PARK\s+W\b', 'CENTRAL PARK WEST'),
                  r'(?i)\bCENTRAL\s+PARK\s+E\b',   'CENTRAL PARK EAST'),
                r'(?i)(^|\s)W\s+(\d)',              'WEST \\2'),
              r'(?i)(^|\s)E\s+(\d)',                'EAST \\2'),
            r'(?i)(^|\s)N\s+(\d)',                  'NORTH \\2'),
          r'(?i)(^|\s)S\s+(\d)',                    'SOUTH \\2')
      ) AS addr_canonical,
      CONCAT(
        CAST(borough_code AS STRING), '-',
        LPAD(CAST(block AS STRING), 5, '0'), '-',
        LPAD(CAST(lot   AS STRING), 4, '0')
      ) AS bbl,
      CASE
        WHEN zip_code IS NOT NULL AND TRIM(zip_code) NOT IN ('', '0')
          THEN CONCAT(
            TRIM(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          addr_abbrev_expanded,
                          r'(?i)\bCENTRAL\s+PARK\s+W\b', 'CENTRAL PARK WEST'),
                        r'(?i)\bCENTRAL\s+PARK\s+E\b',   'CENTRAL PARK EAST'),
                      r'(?i)(^|\s)W\s+(\d)',              'WEST \\2'),
                    r'(?i)(^|\s)E\s+(\d)',                'EAST \\2'),
                  r'(?i)(^|\s)N\s+(\d)',                  'NORTH \\2'),
                r'(?i)(^|\s)S\s+(\d)',                    'SOUTH \\2')
            ),
            ', NEW YORK, NY ', TRIM(zip_code))
        ELSE
          CONCAT(
            TRIM(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          addr_abbrev_expanded,
                          r'(?i)\bCENTRAL\s+PARK\s+W\b', 'CENTRAL PARK WEST'),
                        r'(?i)\bCENTRAL\s+PARK\s+E\b',   'CENTRAL PARK EAST'),
                      r'(?i)(^|\s)W\s+(\d)',              'WEST \\2'),
                    r'(?i)(^|\s)E\s+(\d)',                'EAST \\2'),
                  r'(?i)(^|\s)N\s+(\d)',                  'NORTH \\2'),
                r'(?i)(^|\s)S\s+(\d)',                    'SOUTH \\2')
            ),
            ', NEW YORK, NY')
      END AS full_address_canonical
    FROM sales_norm
  ),

  -- ── 2c. Sales: building-type mode per BBL (no correlated subquery) ────────
  sales_btype_counts AS (
    SELECT
      bbl,
      CASE
        WHEN REGEXP_CONTAINS(bldg_class, r'^A')  THEN 'single_family'
        WHEN REGEXP_CONTAINS(bldg_class, r'^B')  THEN 'two_family'
        WHEN bldg_class IN ('C4','C6')           THEN 'co_op'
        WHEN REGEXP_CONTAINS(bldg_class, r'^C')  THEN 'small_multifamily'
        WHEN bldg_class IN ('D0','D4')           THEN 'co_op'
        WHEN REGEXP_CONTAINS(bldg_class, r'^D')  THEN 'large_multifamily'
        WHEN REGEXP_CONTAINS(bldg_class, r'^R')  THEN 'condo'
        WHEN REGEXP_CONTAINS(bldg_class, r'^S')  THEN 'mixed_use'
        WHEN REGEXP_CONTAINS(bldg_class, r'^K')  THEN 'mixed_use'
        WHEN REGEXP_CONTAINS(bldg_class, r'^V')  THEN 'vacant'
        ELSE NULL
      END AS btype,
      COUNT(*) AS cnt
    FROM sales_canonical
    WHERE bldg_class IS NOT NULL
    GROUP BY bbl, btype
    HAVING btype IS NOT NULL
  ),

  sales_btype_best AS (
    SELECT bbl, btype AS building_type_sales
    FROM (
      SELECT bbl, btype,
        ROW_NUMBER() OVER (PARTITION BY bbl ORDER BY cnt DESC, btype) AS rn
      FROM sales_btype_counts
    )
    WHERE rn = 1
  ),

  -- ── 2d. Sales: aggregate per BBL ─────────────────────────────────────────
  sales_agg AS (
    SELECT
      sc.bbl,
      MAX(sc.total_units)                                    AS unit_count_sales,
      AVG(IF(sc.sale_price > 10000, sc.sale_price, NULL))    AS avg_sale_price,
      MAX(IF(sc.sale_price > 10000, sc.sale_price, NULL))    AS max_sale_price,
      MAX(IF(sc.sale_price > 10000, sc.sale_date,  NULL))    AS latest_sale_date,
      COUNT(IF(sc.sale_price > 10000, 1, NULL))              AS sale_count,
      AVG(IF(sc.sale_price > 10000 AND sc.gross_sqft > 0,
             SAFE_DIVIDE(sc.sale_price, sc.gross_sqft), NULL)) AS avg_ppsf
    FROM sales_canonical sc
    GROUP BY sc.bbl
  ),

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3 – Merge: PLUTO LEFT JOIN sales (PLUTO row always survives)
-- ─────────────────────────────────────────────────────────────────────────────

  merged AS (
    SELECT
      p.full_address_canonical,
      p.bbl,
      p.zip_code,
      p.borough_code,
      -- Building type: prefer sales mode (more up-to-date class code), fall back to PLUTO
      COALESCE(bt.building_type_sales, p.building_type_pluto, 'unknown') AS building_type,
      -- Unit count: PLUTO unitstotal is authoritative; fall back to sales max
      COALESCE(p.units_total, sa.unit_count_sales, 1)                   AS unit_count,
      sa.avg_sale_price,
      sa.max_sale_price,
      sa.latest_sale_date,
      COALESCE(sa.sale_count, 0)                                        AS sale_count,
      sa.avg_ppsf
    FROM pluto_enriched p
    LEFT JOIN sales_agg  sa ON p.bbl = sa.bbl
    LEFT JOIN sales_btype_best bt ON p.bbl = bt.bbl
  )

SELECT
  full_address_canonical  AS full_address,
  bbl,
  zip_code,
  borough_code,
  building_type,
  COALESCE(unit_count, 1) AS unit_count,
  avg_sale_price,
  max_sale_price,
  latest_sale_date,
  sale_count,
  avg_ppsf
FROM merged
WHERE full_address_canonical IS NOT NULL
  AND TRIM(full_address_canonical) != '';
