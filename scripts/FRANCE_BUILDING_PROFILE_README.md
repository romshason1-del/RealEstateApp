# France Building Profile

Aggregated building-level intelligence from `france_dvf_rich_source` (official DVF data only).

## Table: `streetiq_gold.france_building_profile`

| Column | Type | Description |
|--------|------|-------------|
| building_key | STRING | `postcode\|normalized_street\|house_number` |
| postcode | STRING | LPAD 5 digits |
| normalized_street | STRING | Uppercase, accent-stripped, prefix/stopword normalized |
| house_number | STRING | Trimmed |
| total_transactions | INT64 | Count of DVF rows at this address |
| distinct_unit_count | INT64 | Count of distinct unit_number (or 1 when null) |
| avg_price_m2 | FLOAT64 | Average price/m² (thousandths of €, same as source) |
| median_price_m2 | FLOAT64 | Median price/m² |
| building_class | STRING | `apartment_building` \| `likely_house` \| `unclear` |

## Classification rules

- **apartment_building**: `distinct_unit_count >= 3`
- **likely_house**: `total_transactions <= 2`
- **unclear**: otherwise

## Build

```bash
npx tsx scripts/build-france-building-profile.ts
```

Or run the SQL directly:

```bash
bq query --use_legacy_sql=false --project_id=streetiq-bigquery < scripts/create-france-building-profile.sql
```

## Usage in property-value flow

- Queried in parallel with `france_building_intelligence_v2`
- When `building_class = 'apartment_building'` → strengthens apartment detection
- When `building_class = 'likely_house'` → strengthens house detection
- Fallback when intelligence_v2 returns no row
- Avoids false apartment detection when DVF shows ≤2 transactions

## Prerequisites

- `france_dvf_rich_source` must exist and be populated
- BigQuery credentials with access to `streetiq-bigquery.streetiq_gold`
