/**
 * NYC app output table — US only. Dataset `real_estate_us` is in BigQuery location **US** (not EU).
 */

import { NYC_APP_OUTPUT_V4_COL } from "@/lib/us/us-nyc-app-output-schema";

const DEFAULT_DATASET_TABLE = "real_estate_us.us_nyc_app_output_final_v4";

/** Row lookup uses {@link NYC_APP_OUTPUT_V4_COL.lookup_address}. */
export const US_NYC_APP_OUTPUT_ADDRESS_COL = NYC_APP_OUTPUT_V4_COL.lookup_address;

export function getNycAppOutputTableReference(): string {
  const override = (process.env.US_NYC_APP_OUTPUT_TABLE ?? "").trim();
  const datasetTable = override || DEFAULT_DATASET_TABLE;
  const project = (process.env.BIGQUERY_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT_ID || "").trim();
  if (!project) return datasetTable;
  if (datasetTable.includes(".")) {
    const parts = datasetTable.split(".");
    if (parts.length === 3) return datasetTable;
    return `${project}.${datasetTable}`;
  }
  return `${project}.${datasetTable}`;
}
