-- =============================================================================
-- Build: streetiq_gold.us_nyc_address_master_v1 + nyc_normalize_address_master_v1()
-- US / NYC only — not used by France.
--
-- Unifies PLUTO + Rolling Sales building lines with one suffix-normalization
-- pipeline (abbreviated street types + directionals). Sales rows are appended
-- only when no PLUTO row shares the same normalized_address.
--
-- Run:
--   bq query --use_legacy_sql=false --project_id=streetiq-bigquery --location=EU \
--     < scripts/us/nyc/sql/build-us-nyc-address-master-v1.sql
-- =============================================================================

CREATE OR REPLACE FUNCTION `streetiq-bigquery.streetiq_gold.nyc_normalize_address_master_v1`(addr STRING)
RETURNS STRING
LANGUAGE sql
AS (
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
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                  TRIM(REGEXP_REPLACE(UPPER(TRIM(IFNULL(addr, ''))), r'\s+', ' ')),
                                  r'\bSTREET\b', 'ST'),
                                r'\bAVENUE\b', 'AVE'),
                              r'\bBOULEVARD\b', 'BLVD'),
                            r'\bDRIVE\b', 'DR'),
                          r'\bPLACE\b', 'PL'),
                        r'\bCOURT\b', 'CT'),
                      r'\bLANE\b', 'LN'),
                    r'\bROAD\b', 'RD'),
                  r'\bTERRACE\b', 'TER'),
                r'\bPARKWAY\b', 'PKY'),
              r'\bHIGHWAY\b', 'HWY'),
            r'\bEXPRESSWAY\b', 'EXPY'),
          r'\bNORTH\b', 'N'),
        r'\bSOUTH\b', 'S'),
      r'\bEAST\b', 'E'),
    r'\bWEST\b', 'W')
);

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.us_nyc_address_master_v1`
OPTIONS (description = 'Unified NYC address master - PLUTO + Sales normalized')
PARTITION BY source_date
CLUSTER BY normalized_address, borough, zipcode
AS

WITH

  pluto_src AS (
    SELECT
      address,
      TRIM(REGEXP_REPLACE(CAST(postcode AS STRING), r'\.0\s*$', '')) AS zipcode,
      SAFE_CAST(borocode AS INT64) AS borocode,
      SAFE_CAST(`Tax block` AS INT64) AS tax_block,
      SAFE_CAST(`Tax lot` AS INT64) AS tax_lot,
      SAFE_CAST(unitsres AS INT64) AS unitsres,
      SAFE_CAST(unitstotal AS INT64) AS unitstotal,
      TRIM(CAST(bldgclass AS STRING)) AS bldgclass,
      SAFE_CAST(landuse AS FLOAT64) AS landuse,
      SAFE_CAST(assesstot AS FLOAT64) AS assesstot,
      SAFE_CAST(yearbuilt AS INT64) AS yearbuilt,
      SAFE_CAST(numbldgs AS FLOAT64) AS numbldgs,
      SAFE_CAST(numfloors AS FLOAT64) AS numfloors,
      SAFE_CAST(lotarea AS FLOAT64) AS lotarea,
      SAFE_CAST(bldgarea AS FLOAT64) AS bldgarea
    FROM `streetiq-bigquery.streetiq_gold.us_nyc_pluto_normalized`
    WHERE address IS NOT NULL
      AND TRIM(CAST(address AS STRING)) != ''
  ),

  pluto_normalized AS (
    SELECT
      CONCAT(
        CAST(borocode AS STRING), '-',
        LPAD(CAST(tax_block AS STRING), 5, '0'), '-',
        LPAD(CAST(tax_lot AS STRING), 4, '0')
      ) AS bbl,
      TRIM(REGEXP_REPLACE(UPPER(COALESCE(address, '')), r'\s+', ' ')) AS raw_address,
      `streetiq-bigquery.streetiq_gold.nyc_normalize_address_master_v1`(
        TRIM(REGEXP_REPLACE(UPPER(COALESCE(address, '')), r'\s+', ' '))
      ) AS normalized_address,
      zipcode,
      CAST(borocode AS STRING) AS borough,
      unitsres,
      unitstotal,
      bldgclass,
      landuse,
      assesstot,
      yearbuilt,
      numbldgs,
      numfloors,
      lotarea,
      bldgarea,
      'PLUTO' AS source,
      CURRENT_DATE() AS source_date
    FROM pluto_src
  ),

  sales_src AS (
    SELECT
      TRIM(SPLIT(NULLIF(TRIM(CAST(ADDRESS AS STRING)), ''), ',')[SAFE_OFFSET(0)]) AS raw_building_addr,
      SAFE_CAST(ZIP_CODE AS STRING) AS zip_code,
      SAFE_CAST(BOROUGH AS INT64) AS borough_code,
      SAFE_CAST(BLOCK AS INT64) AS block,
      SAFE_CAST(LOT AS INT64) AS lot,
      SAFE_CAST(SALE_PRICE AS FLOAT64) AS sale_price
    FROM `streetiq-bigquery.streetiq_gold.us_nyc_raw_sales`
    WHERE ADDRESS IS NOT NULL
      AND TRIM(CAST(ADDRESS AS STRING)) != ''
      AND SAFE_CAST(SALE_PRICE AS FLOAT64) > 10000
  ),

  sales_normalized AS (
    SELECT
      CASE
        WHEN borough_code IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
          THEN CONCAT(
            CAST(borough_code AS STRING), '-',
            LPAD(CAST(block AS STRING), 5, '0'), '-',
            LPAD(CAST(lot AS STRING), 4, '0')
          )
        ELSE NULL
      END AS bbl,
      TRIM(REGEXP_REPLACE(UPPER(COALESCE(raw_building_addr, '')), r'\s+', ' ')) AS raw_address,
      `streetiq-bigquery.streetiq_gold.nyc_normalize_address_master_v1`(
        TRIM(REGEXP_REPLACE(UPPER(COALESCE(raw_building_addr, '')), r'\s+', ' '))
      ) AS normalized_address,
      TRIM(zip_code) AS zipcode,
      CAST(borough_code AS STRING) AS borough,
      CAST(NULL AS INT64) AS unitsres,
      CAST(NULL AS INT64) AS unitstotal,
      CAST(NULL AS STRING) AS bldgclass,
      CAST(NULL AS FLOAT64) AS landuse,
      sale_price AS assesstot,
      CAST(NULL AS INT64) AS yearbuilt,
      CAST(NULL AS FLOAT64) AS numbldgs,
      CAST(NULL AS FLOAT64) AS numfloors,
      CAST(NULL AS FLOAT64) AS lotarea,
      CAST(NULL AS FLOAT64) AS bldgarea,
      'SALES' AS source,
      CURRENT_DATE() AS source_date
    FROM sales_src
    WHERE raw_building_addr IS NOT NULL
      AND TRIM(raw_building_addr) != ''
  ),

  pluto_norm_keys AS (
    SELECT DISTINCT normalized_address FROM pluto_normalized
  ),

  merged AS (
    SELECT * FROM pluto_normalized
    UNION ALL
    SELECT s.*
    FROM sales_normalized s
    LEFT JOIN pluto_norm_keys p ON s.normalized_address = p.normalized_address
    WHERE p.normalized_address IS NULL
  )

SELECT
  ROW_NUMBER() OVER (ORDER BY normalized_address, raw_address, source, bbl) AS id,
  normalized_address,
  raw_address,
  bbl,
  zipcode,
  borough,
  unitsres,
  unitstotal,
  bldgclass,
  landuse,
  assesstot,
  yearbuilt,
  numbldgs,
  numfloors,
  lotarea,
  bldgarea,
  source,
  source_date
FROM merged;
