-- List columns for real_estate_us.us_nyc_app_output_final_v4 (dataset location: US).
-- Align src/lib/us/us-nyc-app-output-schema.ts NYC_APP_OUTPUT_V4_COL with this output.
--
-- bq query --use_legacy_sql=false --project_id=streetiq-bigquery --location=US \
--   < scripts/us/nyc/sql/describe-us-nyc-app-output-final-v4-columns.sql

SELECT column_name, data_type
FROM `streetiq-bigquery.real_estate_us.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'us_nyc_app_output_final_v4'
ORDER BY ordinal_position;
