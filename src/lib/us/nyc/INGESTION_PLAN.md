# NYC BigQuery ingestion plan (Phase 1 foundation)

**Scope:** United States / New York City only. **No France code or pipelines.**

## Objectives

1. Land **official/public** NYC datasets in BigQuery as **raw** tables (verbatim columns where practical).
2. Produce **normalized** tables with stable types, naming, and join keys (borough–block–lot, dates, money).
3. Reserve **helper / truth** table names for later stages (facts, flags, transaction truth, display context). **No population in this phase.**

## Target datasets (first wave)

| Dataset | Publisher | Use |
|--------|-----------|-----|
| **NYC DOF Rolling Sales** | NYC Department of Finance | Transaction-level sale price, sale date, address, tax lot identifiers |
| **MapPLUTO** (or PLUTO) | NYC Planning / Bytes of the Big Apple | Property facts: land/building area, units, year built, building class, BBL identity |

**Out of scope for this plan:** DOB filings, zoning layers, ACRIS document-level joins (may follow later).

## High-level pipeline

1. **Download** published files (CSV/Shapefile/GeoPackage per NYC’s current distribution — confirm URLs on NYC Open Data before automation).
2. **Load raw** into `us_nyc_raw_sales` and `us_nyc_raw_pluto` (append or partition by **ingestion batch** / **tax year** / **file vintage** — see schema plan).
3. **Transform** into `us_nyc_sales_normalized` and `us_nyc_pluto_normalized` (typed columns, consistent BBL, ISO dates, numeric cents or decimal dollars — **decide once** in normalization plan).
4. **Validate** row counts, null rates on keys, duplicate BBL+date+price spot checks.
5. **Later:** populate helper tables from normalized layers only.

## Environment prerequisites

- Google Cloud project with BigQuery enabled.
- Service account (or user) with `bigquery.datasets.create`, `bigquery.tables.create`, `bigquery.tables.updateData`, `bigquery.jobs.create`.
- Dataset name suggestion: `us_nyc` (or project convention — **single dataset for all tables below**).

## Next physical step

See `scripts/us/nyc/README.md` for commands to create datasets/tables and load files once you have local paths or GCS URIs.

## References (verify before production loads)

- NYC Open Data: Rolling Sales (DOF) — search “Rolling sales data”.
- NYC Open Data / BYTES: MapPLUTO — current vintage and file format.
