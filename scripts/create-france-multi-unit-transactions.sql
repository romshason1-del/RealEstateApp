-- France multi-unit disclosure helper: streetiq_gold.france_multi_unit_transactions
--
-- Grain: one row per (normalized address + sale date + valeur), matching API lookup keys.
-- multi_unit_transaction = TRUE only when exactly ONE DVF mutation at that grain is a multi-*apartment* sale:
--   COUNT(DISTINCT unit_number) >= 2 counting ONLY property_type = Appartement (ignore Dépendance, Parking,
--   Local, etc.), AND SUM(surface_m2) of those Appartement rows > 40 m² (prefer false).
--
-- Why older logic failed: (1) grouping only by address + date + price merged unrelated mutations; (2) row
-- counts / distinct lots included annexes (dépendance, parking, cave) that are not additional dwellings.
--
-- Requires france_dvf_rich_source columns document_id and mutation_group_key (see build-france-rich-source.ts).
-- Reload rich source, then run:
--   npx tsx scripts/build-france-multi-unit-transactions.ts
-- Or: bq query --use_legacy_sql=false --project_id=streetiq-bigquery < scripts/create-france-multi-unit-transactions.sql

CREATE OR REPLACE TABLE `streetiq-bigquery.streetiq_gold.france_multi_unit_transactions` AS
WITH base AS (
  SELECT
    country,
    city,
    postcode,
    street,
    house_number,
    unit_number,
    surface_m2,
    last_sale_date,
    last_sale_price,
    document_id,
    mutation_group_key,
    property_type
  FROM `streetiq-bigquery.streetiq_gold.france_dvf_rich_source`
),
street_prep AS (
  SELECT
    *,
    REGEXP_REPLACE(REGEXP_REPLACE(UPPER(NORMALIZE(TRIM(CAST(street AS STRING)), NFD)), r'\p{M}', ''), r'[^A-Z0-9 ]+', ' ') AS street_base
  FROM base
),
street_no_prefix AS (
  SELECT
    *,
    REGEXP_REPLACE(street_base, r'^(RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\.?\s+', '') AS street_sp
  FROM street_prep
),
with_norm AS (
  SELECT
    country,
    city,
    postcode,
    street,
    house_number,
    unit_number,
    surface_m2,
    last_sale_date,
    last_sale_price,
    document_id,
    mutation_group_key,
    property_type,
    LPAD(TRIM(CAST(postcode AS STRING)), 5, '0') AS postcode_norm,
    CASE
      WHEN TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(UPPER(NORMALIZE(TRIM(CAST(city AS STRING)), NFD)), r'\p{M}', ''),
          r'\s*\d{1,2}(?:ER|E|EME)?(?:\s*ARRONDISSEMENT)?\s*$',
          ''
        )
      ) = 'LYONS' THEN 'LYON'
      ELSE TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(UPPER(NORMALIZE(TRIM(CAST(city AS STRING)), NFD)), r'\p{M}', ''),
          r'\s*\d{1,2}(?:ER|E|EME)?(?:\s*ARRONDISSEMENT)?\s*$',
          ''
        )
      )
    END AS city_norm,
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
                            REGEXP_REPLACE(street_sp, r'\s+DU\s+', ' '),
                            r'\s+DE\s+',
                            ' '
                          ),
                          r'\s+DES\s+',
                          ' '
                        ),
                        r'\s+LA\s+',
                        ' '
                      ),
                      r'\s+LE\s+',
                      ' '
                    ),
                    r'^DU\s+',
                    ''
                  ),
                  r'^DE\s+',
                  ''
                ),
                r'^DES\s+',
                ''
              ),
              r'^LA\s+',
              ''
            ),
            r'^LE\s+',
            ''
          ),
          r'\s+',
          ' '
        )
      )
    ) AS street_norm_clean,
    COALESCE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(REGEXP_REPLACE(UPPER(TRIM(CAST(house_number AS STRING))), r'\s+BIS\b', 'B'), r'\s+TER\b', 'T'),
            r'\s+QUATER\b',
            'Q'
          ),
          r'[\s\-]+',
          ''
        ),
        r'[^0-9A-Z]',
        ''
      ),
      ''
    ) AS house_number_norm,
    COALESCE(
      NULLIF(TRIM(CAST(document_id AS STRING)), ''),
      NULLIF(TRIM(CAST(mutation_group_key AS STRING)), '')
    ) AS effective_mut_key
  FROM street_no_prefix
),
all_grains AS (
  SELECT
    postcode_norm,
    city_norm,
    street_norm_clean,
    house_number_norm,
    last_sale_date,
    last_sale_price,
    ANY_VALUE(country) AS country,
    ANY_VALUE(postcode) AS postcode,
    ANY_VALUE(city) AS city,
    ANY_VALUE(street) AS street,
    ANY_VALUE(house_number) AS house_number
  FROM with_norm
  WHERE LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'
  GROUP BY
    postcode_norm,
    city_norm,
    street_norm_clean,
    house_number_norm,
    last_sale_date,
    last_sale_price
),
per_mutation AS (
  SELECT
    effective_mut_key,
    postcode_norm,
    city_norm,
    street_norm_clean,
    house_number_norm,
    last_sale_date,
    last_sale_price,
    COUNTIF(LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement') AS n_apt,
    COUNT(DISTINCT CASE
      WHEN LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'
      THEN NULLIF(TRIM(CAST(unit_number AS STRING)), '')
    END) AS duc_apt,
    SUM(
      CASE
        WHEN LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'
          AND surface_m2 IS NOT NULL
          AND surface_m2 > 0
        THEN surface_m2
        ELSE 0
      END
    ) AS sum_surf_apt,
    MIN(
      CASE
        WHEN LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'
          AND surface_m2 IS NOT NULL
          AND surface_m2 > 0
        THEN surface_m2
      END
    ) AS min_surf_apt,
    MAX(
      CASE
        WHEN LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'
          AND surface_m2 IS NOT NULL
          AND surface_m2 > 0
        THEN surface_m2
      END
    ) AS max_surf_apt,
    COUNTIF(
      LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'
      AND surface_m2 IS NOT NULL
      AND surface_m2 > 0
    ) AS n_surf_pos_apt,
    MAX(IF(NULLIF(TRIM(CAST(document_id AS STRING)), '') IS NOT NULL, 1, 0)) = 1 AS has_document_id
  FROM with_norm
  WHERE effective_mut_key IS NOT NULL AND TRIM(CAST(effective_mut_key AS STRING)) != ''
  GROUP BY
    effective_mut_key,
    postcode_norm,
    city_norm,
    street_norm_clean,
    house_number_norm,
    last_sale_date,
    last_sale_price
),
mut_flagged AS (
  SELECT
    *,
    CASE
      WHEN duc_apt < 2 OR n_apt < 2 THEN FALSE
      WHEN sum_surf_apt <= 40 THEN FALSE
      WHEN has_document_id AND duc_apt >= 2 THEN TRUE
      WHEN
        n_surf_pos_apt >= 2
        AND min_surf_apt IS NOT NULL
        AND min_surf_apt > 0
        AND max_surf_apt <= min_surf_apt * 1.05
        AND duc_apt >= 2
      THEN
        TRUE
      ELSE FALSE
    END AS is_multi_unit_mutation
  FROM per_mutation
),
grain_mutation_stats AS (
  SELECT
    postcode_norm,
    city_norm,
    street_norm_clean,
    house_number_norm,
    last_sale_date,
    last_sale_price,
    COUNT(DISTINCT effective_mut_key) AS distinct_mutation_count,
    LOGICAL_OR(is_multi_unit_mutation) AS any_mutation_multi,
    MAX(IF(is_multi_unit_mutation, duc_apt, NULL)) AS distinct_unit_when_multi
  FROM mut_flagged
  GROUP BY
    postcode_norm,
    city_norm,
    street_norm_clean,
    house_number_norm,
    last_sale_date,
    last_sale_price
)
SELECT
  ag.country,
  ag.postcode,
  ag.city,
  ag.street,
  ag.house_number,
  ag.postcode_norm,
  ag.city_norm,
  ag.street_norm_clean,
  ag.house_number_norm,
  TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(TRIM(CAST(ag.last_sale_date AS STRING)), 1, 10)), 'Europe/Paris') AS last_sale_date,
  SAFE_CAST(ag.last_sale_price AS INT64) AS last_sale_price_raw,
  CAST(
    COALESCE(g.distinct_mutation_count, 0) = 1 AND COALESCE(g.any_mutation_multi, FALSE) AS BOOL
  ) AS multi_unit_transaction,
  CASE
    WHEN COALESCE(g.distinct_mutation_count, 0) = 1 AND COALESCE(g.any_mutation_multi, FALSE) THEN g.distinct_unit_when_multi
    ELSE 1
  END AS distinct_unit_count
FROM all_grains ag
LEFT JOIN grain_mutation_stats g
ON
  ag.postcode_norm = g.postcode_norm
  AND ag.city_norm = g.city_norm
  AND ag.street_norm_clean = g.street_norm_clean
  AND ag.house_number_norm = g.house_number_norm
  AND TRIM(CAST(ag.last_sale_date AS STRING)) = TRIM(CAST(g.last_sale_date AS STRING))
  AND ag.last_sale_price = g.last_sale_price;
