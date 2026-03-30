-- us_nyc_building_truth_v3 spot checks. US / NYC only.
-- Run after build-us-nyc-building-truth-v3.sql
-- Run: bq query --use_legacy_sql=false --project_id=streetiq-bigquery --location=EU < this file

SELECT
  'truth_234_cpw' AS probe,
  full_address,
  bbl,
  CAST(zip_code AS STRING) AS zip_code,
  building_type,
  unit_count
FROM `streetiq-bigquery.streetiq_gold.us_nyc_building_truth_v3`
WHERE REGEXP_CONTAINS(full_address, r'234.*CENTRAL PARK')

UNION ALL

SELECT
  'truth_245_e63',
  full_address,
  bbl,
  CAST(zip_code AS STRING),
  building_type,
  unit_count
FROM `streetiq-bigquery.streetiq_gold.us_nyc_building_truth_v3`
WHERE REGEXP_CONTAINS(full_address, r'245.*(EAST|E)\s+63')
   OR full_address LIKE '%245%E%63%'

UNION ALL

SELECT
  'truth_40_w86',
  full_address,
  bbl,
  CAST(zip_code AS STRING),
  building_type,
  unit_count
FROM `streetiq-bigquery.streetiq_gold.us_nyc_building_truth_v3`
WHERE REGEXP_CONTAINS(full_address, r'40.*(WEST|W)\s+86')
ORDER BY probe, full_address;
