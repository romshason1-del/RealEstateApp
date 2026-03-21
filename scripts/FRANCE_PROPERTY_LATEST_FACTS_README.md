# France property_latest_facts Pipeline

Builds the `property_latest_facts` gold table from DVF (Demandes de Valeurs Foncières) to increase coverage for:

- **exact_house** — houses resolving with high confidence
- **exact_address** — apartment address-level matches
- **building_similar_unit** — similar apartments in the same building

## Quick Start

```bash
# 1. Download DVF files (2022, 2023, 2024)
npm run download:dvf

# 2. Build property_latest_facts (single year or all years)
npm run build:france-facts ValeursFoncieres-2024.txt
npm run build:france-facts .   # all ValeursFoncieres-*.txt in cwd (needs ~8GB heap)
```

For all years, the script uses `--max-old-space-size=8192`. If OOM occurs, process one year at a time and merge outputs manually, or increase heap: `NODE_OPTIONS=--max-old-space-size=12288 npm run build:france-facts .`

Output: `output/property_latest_facts_france.ndjson`

## Schema

| Column | Type | Description |
|--------|------|-------------|
| country | STRING | "FR" |
| city | STRING | Commune |
| postcode | STRING | Code postal |
| street | STRING | Voie (raw) |
| house_number | STRING | No voie |
| unit_number | STRING \| null | 1er lot (apartment) |
| property_type | STRING | Appartement, Maison, etc. |
| surface_m2 | FLOAT \| null | Surface réelle bati |
| last_sale_date | STRING | YYYY-MM-DD |
| last_sale_price | INT | Thousandths of euro (÷1000) |
| price_per_m2 | INT \| null | Thousandths of euro |
| data_source | STRING | "dvf" |

## Deduplication

One row per (country, city, postcode, normalized_street, house_number, unit_number). Most recent sale wins.

## Loading to BigQuery

```bash
bq load --source_format=NEWLINE_DELIMITED_JSON \
  --replace \
  streetiq-bigquery:streetiq_gold.property_latest_facts \
  output/property_latest_facts_france.ndjson
```

## Summary Logs

The pipeline prints:

```
[FR_FACTS] total_france_fact_rows_before=...
[FR_FACTS] total_france_fact_rows_after=...
[FR_FACTS] distinct_city_count=...
[FR_FACTS] distinct_street_house_number_count=...
[FR_FACTS] distinct_apartment_address_candidate_count=...
```

## Coverage Notes

- **Houses** (`property_type` = Maison): feed exact_house
- **Apartments** (`property_type` = Appartement): feed exact_address and building_similar_unit
- Rows without surface still provide address coverage; price_per_m2 is null for those
