# France Building Unit Flags

Precomputed table indicating which addresses/buildings have more than one distinct unit number in official DVF data. Used at runtime to decide when to prompt for apartment/unit number (has_unit_level_differentiation).

## Table: `streetiq_gold.france_building_unit_flags`

| Column | Type | Description |
|--------|------|-------------|
| postcode | STRING | Postal code |
| city | STRING | City/commune |
| street | STRING | Street name |
| house_number | STRING | Building number |
| distinct_unit_count | INT64 | Count of distinct unit_number values |
| rows_count | INT64 | Total DVF rows at this address |
| has_unit_level_differentiation | BOOL | true when distinct_unit_count > 1 |

## Source

- **Source table**: `france_dvf_rich_source` (read-only; no changes)
- **Filter**: `property_type = 'Appartement'` and non-empty unit_number
- **Group by**: postcode, city, street, house_number

## Build

Run once or schedule daily:

```bash
npx tsx scripts/build-france-building-unit-flags.ts
# or
npm run build:france-unit-flags
```

Or run SQL directly:

```bash
bq query --use_legacy_sql=false --project_id=streetiq-bigquery < scripts/create-france-building-unit-flags.sql
```

## Usage

- Runtime lookup by (postcode, city, street, house_number) to get `has_unit_level_differentiation`
- When true → safe to prompt for apartment/unit number
- When false or no row → do NOT prompt; return building-level result

## Prerequisites

- `france_dvf_rich_source` must exist and be populated
- BigQuery credentials with access to `streetiq-bigquery.streetiq_gold`
