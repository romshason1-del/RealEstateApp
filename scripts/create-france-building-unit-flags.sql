-- France Building Unit Flags: precompute which addresses have unit-level differentiation
-- Builds: streetiq_gold.france_building_unit_flags
--
-- Used at runtime to decide when to prompt for apartment/unit number.
-- Only buildings with has_unit_level_differentiation = true should trigger the prompt.
--
-- Run: bq query --use_legacy_sql=false --project_id=streetiq-bigquery < scripts/create-france-building-unit-flags.sql
-- Or: npx tsx scripts/build-france-building-unit-flags.ts

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.france_building_unit_flags` AS
SELECT
  postcode,
  city,
  street,
  house_number,
  COUNT(DISTINCT unit_number) AS distinct_unit_count,
  COUNT(*) AS rows_count,
  COUNT(DISTINCT unit_number) > 1 AS has_unit_level_differentiation
FROM `streetiq-bigquery.streetiq_gold.france_dvf_rich_source`
WHERE
  property_type = 'Appartement'
  AND unit_number IS NOT NULL
  AND TRIM(CAST(unit_number AS STRING)) != ''
GROUP BY
  postcode,
  city,
  street,
  house_number;
