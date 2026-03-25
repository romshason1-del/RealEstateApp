/**
 * NYC DOF Rolling Sales → BigQuery (scaffold).
 * Does not load data. Does not import France modules.
 *
 * Next: implement batch ID, CSV/NDJSON read, and bq insert/load jobs.
 */

function printHelp(): void {
  console.log(`
NYC Rolling Sales ingestion (placeholder)

Planned target table: us_nyc_raw_sales → us_nyc_sales_normalized
Docs: src/lib/us/nyc/INGESTION_PLAN.md
      src/lib/us/nyc/FIELD_MAPPING.md

Future usage (example):
  npx tsx scripts/us/nyc/ingest-rolling-sales.ts --file <path-to-csv> --dataset YOUR_PROJECT:us_nyc

No files are read in this scaffold.
`);
}

const argv = process.argv.slice(2);
if (argv.includes("--help") || argv.includes("-h")) {
  printHelp();
  process.exit(0);
}

printHelp();
process.exit(0);
