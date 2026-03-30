-- PLUTO spot checks (Manhattan). US / NYC only.
-- Run: bq query --use_legacy_sql=false --project_id=streetiq-bigquery --location=EU < this file

SELECT
  'pluto_234_cpw' AS probe,
  address,
  CAST(postcode AS STRING) AS postcode,
  CAST(borocode AS INT64) AS borocode,
  `Tax block` AS tax_block,
  `Tax lot` AS tax_lot,
  bldgclass,
  unitsres,
  unitstotal
FROM `streetiq-bigquery.streetiq_gold.us_nyc_pluto_normalized`
WHERE borocode = 1
  AND REGEXP_CONTAINS(UPPER(COALESCE(address, '')), r'(^|\s)234(\s|-).*CENTRAL\s+PARK')

UNION ALL

SELECT
  'pluto_245_e63',
  address,
  CAST(postcode AS STRING),
  CAST(borocode AS INT64),
  `Tax block`,
  `Tax lot`,
  bldgclass,
  unitsres,
  unitstotal
FROM `streetiq-bigquery.streetiq_gold.us_nyc_pluto_normalized`
WHERE borocode = 1
  AND REGEXP_CONTAINS(UPPER(COALESCE(address, '')), r'(^|\s)245(\s|-).*63')
  AND REGEXP_CONTAINS(UPPER(COALESCE(address, '')), r'EAST|(^|\s)E\s+')

UNION ALL

SELECT
  'pluto_40_w86',
  address,
  CAST(postcode AS STRING),
  CAST(borocode AS INT64),
  `Tax block`,
  `Tax lot`,
  bldgclass,
  unitsres,
  unitstotal
FROM `streetiq-bigquery.streetiq_gold.us_nyc_pluto_normalized`
WHERE borocode = 1
  AND REGEXP_CONTAINS(UPPER(COALESCE(address, '')), r'(^|\s)40(\s|-).*86')
ORDER BY probe, address;
