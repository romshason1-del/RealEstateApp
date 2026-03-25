/**
 * NYC MapPLUTO / PLUTO → BigQuery (scaffold).
 * Does not load data. Does not import France modules.
 *
 * Next: implement vintage label, file read (CSV/SHP pipeline), and bq load.
 */

function printHelp(): void {
  console.log(`
NYC MapPLUTO ingestion (placeholder)

Planned target table: us_nyc_raw_pluto → us_nyc_pluto_normalized
Docs: src/lib/us/nyc/INGESTION_PLAN.md
      src/lib/us/nyc/FIELD_MAPPING.md

Future usage (example):
  npx tsx scripts/us/nyc/ingest-pluto.ts --file <path-to-file> --vintage 24v2 --dataset YOUR_PROJECT:us_nyc

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
