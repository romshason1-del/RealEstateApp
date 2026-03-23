-- France Building Unit Flags: precompute which addresses have unit-level differentiation
-- Builds: streetiq_gold.france_building_unit_flags
--
-- Normalized join columns match app/backend normalization exactly.
-- Join on: postcode_norm, city_norm, street_norm_clean, house_number_norm
--
-- Run: bq query --use_legacy_sql=false --project_id=streetiq-bigquery < scripts/create-france-building-unit-flags.sql
-- Or: npx tsx scripts/build-france-building-unit-flags.ts

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.france_building_unit_flags` AS
WITH base AS (
  SELECT
    postcode,
    city,
    street,
    house_number,
    unit_number
  FROM `streetiq-bigquery.streetiq_gold.france_dvf_rich_source`
  WHERE property_type = 'Appartement'
    AND unit_number IS NOT NULL
    AND TRIM(CAST(unit_number AS STRING)) != ''
),
-- Normalization matching app: normalizePostcodeForFranceSource, normalizeCityForFranceSource,
-- frBqStreetNormalizedEarly, normHn (house_number)
street_prep AS (
  SELECT *,
    REGEXP_REPLACE(REGEXP_REPLACE(UPPER(NORMALIZE(TRIM(CAST(street AS STRING)), NFD)), r'\p{M}', ''), r'[^A-Z0-9 ]+', ' ') AS street_base
  FROM base
),
street_no_prefix AS (
  SELECT *,
    REGEXP_REPLACE(street_base, r'^(RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\.?\s+', '') AS street_sp
  FROM street_prep
),
with_norm AS (
  SELECT
    postcode,
    city,
    street,
    house_number,
    unit_number,
    LPAD(TRIM(CAST(postcode AS STRING)), 5, '0') AS postcode_norm,
    CASE WHEN TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(NORMALIZE(TRIM(CAST(city AS STRING)), NFD)), r'\p{M}', ''), r'\s*\d{1,2}(?:ER|E|EME)?(?:\s*ARRONDISSEMENT)?\s*$', '')) = 'LYONS' THEN 'LYON' ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(NORMALIZE(TRIM(CAST(city AS STRING)), NFD)), r'\p{M}', ''), r'\s*\d{1,2}(?:ER|E|EME)?(?:\s*ARRONDISSEMENT)?\s*$', '')) END AS city_norm,
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
      street_sp,
      r'\s+DU\s+', ' '), r'\s+DE\s+', ' '), r'\s+DES\s+', ' '), r'\s+LA\s+', ' '), r'\s+LE\s+', ' '),
      r'^DU\s+', ''), r'^DE\s+', ''), r'^DES\s+', ''), r'^LA\s+', ''), r'^LE\s+', ''),
      r'\s+', ' '
    )) AS street_norm_clean,
    COALESCE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(TRIM(CAST(house_number AS STRING))), r'\s+BIS\b', 'B'), r'\s+TER\b', 'T'), r'\s+QUATER\b', 'Q'),
          r'[\s\-]+', ''
        ),
        r'[^0-9A-Z]', ''
      ),
      ''
    ) AS house_number_norm
  FROM street_no_prefix
)
SELECT
  ANY_VALUE(postcode) AS postcode,
  ANY_VALUE(city) AS city,
  ANY_VALUE(street) AS street,
  ANY_VALUE(house_number) AS house_number,
  postcode_norm,
  city_norm,
  street_norm_clean,
  house_number_norm,
  COUNT(DISTINCT unit_number) AS distinct_unit_count,
  COUNT(*) AS rows_count,
  COUNT(DISTINCT unit_number) > 1 AS has_unit_level_differentiation
FROM with_norm
GROUP BY
  postcode_norm,
  city_norm,
  street_norm_clean,
  house_number_norm;
