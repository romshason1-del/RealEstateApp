-- France Building Profile: aggregated building-level intelligence from france_dvf_rich_source
-- Builds: streetiq_gold.france_building_profile
--
-- building_key = postcode | normalized_street | house_number
-- Classification:
--   distinct_unit_count >= 3 → apartment_building
--   total_transactions <= 2 → likely_house
--   else → unclear
--
-- Run: bq query --use_legacy_sql=false < scripts/create-france-building-profile.sql
-- Or: npx tsx scripts/build-france-building-profile.ts

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.france_building_profile` AS
WITH base AS (
  SELECT
    LPAD(TRIM(CAST(postcode AS STRING)), 5, '0') AS postcode,
    TRIM(CAST(street AS STRING)) AS street_raw,
    TRIM(CAST(house_number AS STRING)) AS house_number,
    COALESCE(NULLIF(TRIM(CAST(unit_number AS STRING)), ''), '(single)') AS unit_key,
    price_per_m2
  FROM `streetiq-bigquery.streetiq_gold.france_dvf_rich_source`
  WHERE LOWER(TRIM(CAST(country AS STRING))) = 'fr'
    AND postcode IS NOT NULL
    AND TRIM(CAST(postcode AS STRING)) != ''
    AND street IS NOT NULL
    AND TRIM(CAST(street AS STRING)) != ''
    AND house_number IS NOT NULL
    AND TRIM(CAST(house_number AS STRING)) != ''
    AND price_per_m2 IS NOT NULL
    AND price_per_m2 > 0
),
-- Normalize street: uppercase, strip accents (NFD), remove punctuation, strip prefixes, collapse stopwords
normalized AS (
  SELECT
    postcode,
    house_number,
    unit_key,
    price_per_m2,
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
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                REGEXP_REPLACE(UPPER(NORMALIZE(TRIM(street_raw), NFD)), r'\p{M}', ''),
                                r'[^A-Z0-9 ]+', ' '
                              ),
                              r'^(RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\.?\s+', ''
                            ),
                            r'\s+DU\s+', ' '
                          ),
                          r'\s+DE\s+', ' '
                        ),
                        r'\s+DES\s+', ' '
                      ),
                      r'\s+LA\s+', ' '
                    ),
                    r'\s+LE\s+', ' '
                  ),
                  r'^DU\s+', ''
                ),
                r'^DE\s+', ''
              ),
              r'^DES\s+', ''
            ),
            r'^LA\s+', ''
          ),
          r'^LE\s+', ''
        ),
        r'\s+', ' '
      )
    ) AS street_norm
  FROM base
),
with_key AS (
  SELECT
    postcode,
    street_norm,
    house_number,
    CONCAT(postcode, '|', street_norm, '|', house_number) AS building_key,
    unit_key,
    price_per_m2
  FROM normalized
  WHERE street_norm != ''
)
SELECT
  building_key,
  postcode,
  street_norm AS normalized_street,
  house_number,
  COUNT(*) AS total_transactions,
  COUNT(DISTINCT unit_key) AS distinct_unit_count,
  AVG(price_per_m2) AS avg_price_m2,
  APPROX_QUANTILES(price_per_m2, 100)[OFFSET(50)] AS median_price_m2,
  CASE
    WHEN COUNT(DISTINCT unit_key) >= 3 THEN 'apartment_building'
    WHEN COUNT(*) <= 2 THEN 'likely_house'
    ELSE 'unclear'
  END AS building_class
FROM with_key
GROUP BY building_key, postcode, street_norm, house_number;
