# NYC BigQuery ingestion — next steps

**Isolated from France.** Implement loads using official NYC files only.

## Prerequisites

1. [Google Cloud SDK](https://cloud.google.com/sdk) installed (`gcloud`, `bq`).
2. BigQuery dataset created (example name `us_nyc`):

```bash
bq mk --dataset --location=US YOUR_PROJECT_ID:us_nyc
```

3. Authenticated credentials with BigQuery data editor permissions:

```bash
gcloud auth application-default login
```

## 1) Obtain source files (manual until automated)

- **Rolling Sales:** Download the DOF Rolling Sales CSV for the borough(s) / period you need from NYC Open Data (search “Rolling sales data”).
- **MapPLUTO:** Download the current MapPLUTO release (shapefile / geodatabase / CSV if offered) from NYC Planning / BYTES.

Record file paths locally, e.g. `C:\data\nyc\sales\...csv` or upload to GCS.

## 2) Create tables (DDL)

Schema details: `src/lib/us/nyc/BIGQUERY_SCHEMA_PLAN.md`.

Example: create raw sales table with JSON payload (adjust project/dataset):

```bash
bq query --use_legacy_sql=false --project_id=YOUR_PROJECT_ID "
CREATE TABLE IF NOT EXISTS \`YOUR_PROJECT_ID.us_nyc.us_nyc_raw_sales\` (
  ingestion_batch_id STRING NOT NULL,
  ingestion_ts TIMESTAMP NOT NULL,
  source_file_name STRING,
  source_row_number INT64,
  raw_record JSON
);
"
```

Repeat for `us_nyc_raw_pluto` and normalized tables per **BIGQUERY_SCHEMA_PLAN.md**.

## 3) Load CSV into raw (autodetect vs manual schema)

If loading CSV **directly** to a flat raw table (alternative to JSON row):

```bash
bq load --source_format=CSV --autodetect --skip_leading_rows=1 \
  YOUR_PROJECT_ID:us_nyc.us_nyc_raw_sales_flat \
  ./path/to/rollingsales.csv
```

For the **JSON** raw pattern, use a small loader script (future) or `bq load` with newline-delimited JSON produced from CSV.

## 4) Run placeholder scripts (scaffold only)

These print guidance and exit; they do **not** load data yet:

```bash
npx tsx scripts/us/nyc/ingest-rolling-sales.ts --help
npx tsx scripts/us/nyc/ingest-pluto.ts --help
```

## 5) After tables exist

- Add SQL under `scripts/us/nyc/sql/` (future) to populate `us_nyc_sales_normalized` and `us_nyc_pluto_normalized` per `NORMALIZATION_PLAN.md`.
- Populate helper tables only when business rules exist.
