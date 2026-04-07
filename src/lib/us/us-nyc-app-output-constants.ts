/**
 * NYC app output table — US only. Dataset `real_estate_us` lives in BigQuery project **streetiq-bigquery**,
 * location **US** (not EU).
 *
 * Production bug fix: do NOT prefix this path with `BIGQUERY_PROJECT_ID` / `GOOGLE_CLOUD_PROJECT_ID`.
 * Vercel sets those to the deployment project (e.g. `project-29fdf5d2-…`), which does not contain
 * `real_estate_us` — queries must target **streetiq-bigquery** explicitly.
 */

import { NYC_APP_OUTPUT_V4_COL } from "@/lib/us/us-nyc-app-output-schema";

/** BigQuery project that owns `real_estate_us` and `us_nyc_app_output_final_v4` in production. */
export const US_NYC_APP_OUTPUT_DATA_PROJECT = "streetiq-bigquery";

const DEFAULT_DATASET_AND_TABLE = "real_estate_us.us_nyc_app_output_final_v4";

/**
 * Fully qualified default: `streetiq-bigquery.real_estate_us.us_nyc_app_output_final_v4`.
 * Production NYC card reads only this table (location US) — do not point the app at EU-hosted datasets.
 */
export const US_NYC_APP_OUTPUT_FULL_TABLE_DEFAULT = `${US_NYC_APP_OUTPUT_DATA_PROJECT}.${DEFAULT_DATASET_AND_TABLE}`;

/** Row lookup uses {@link NYC_APP_OUTPUT_V4_COL.lookup_address}. */
export const US_NYC_APP_OUTPUT_ADDRESS_COL = NYC_APP_OUTPUT_V4_COL.lookup_address;

/**
 * Resolved table id for SQL `FROM \`project.dataset.table\``.
 *
 * - Default: always {@link US_NYC_APP_OUTPUT_FULL_TABLE_DEFAULT} (streetiq-bigquery).
 * - Override `US_NYC_APP_OUTPUT_TABLE`:
 *   - If 3 segments (`project.dataset.table`), use as-is (staging / alternate project).
 *   - If 2 segments (`dataset.table`), prefix with {@link US_NYC_APP_OUTPUT_DATA_PROJECT}.
 *   - If 1 segment (table id only), use `streetiq-bigquery.real_estate_us.<table>`.
 */
export function getNycAppOutputTableReference(): string {
  const override = (process.env.US_NYC_APP_OUTPUT_TABLE ?? "").trim();
  if (!override) {
    return US_NYC_APP_OUTPUT_FULL_TABLE_DEFAULT;
  }

  const parts = override.split(".").map((p) => p.trim()).filter(Boolean);
  if (parts.length === 3) {
    return override;
  }
  if (parts.length === 2) {
    return `${US_NYC_APP_OUTPUT_DATA_PROJECT}.${override}`;
  }
  if (parts.length === 1) {
    return `${US_NYC_APP_OUTPUT_DATA_PROJECT}.real_estate_us.${parts[0]}`;
  }
  return US_NYC_APP_OUTPUT_FULL_TABLE_DEFAULT;
}

export type NycAppOutputTableResolutionLog = {
  /** Project in the fully qualified table id (data project, not necessarily the BigQuery client default). */
  resolved_data_project_id: string;
  resolved_dataset_id: string;
  resolved_table_id: string;
  full_table_reference: string;
  /** Job client: `BIGQUERY_PROJECT_ID` || `GOOGLE_CLOUD_PROJECT_ID` (may differ from data project). */
  bigquery_client_project_id_from_env: string;
  query_location: "US";
};

/** NYC-only diagnostics for production (temporary). */
export function getNycAppOutputTableResolutionForLog(): NycAppOutputTableResolutionLog {
  const full = getNycAppOutputTableReference();
  const parts = full.split(".").map((p) => p.trim()).filter(Boolean);
  const [resolved_data_project_id, resolved_dataset_id, resolved_table_id] =
    parts.length === 3 ? [parts[0]!, parts[1]!, parts[2]!] : ["(parse_error)", "(parse_error)", "(parse_error)"];
  const bigquery_client_project_id_from_env = (
    process.env.BIGQUERY_PROJECT_ID ||
    process.env.GOOGLE_CLOUD_PROJECT_ID ||
    ""
  ).trim();
  return {
    resolved_data_project_id,
    resolved_dataset_id,
    resolved_table_id,
    full_table_reference: full,
    bigquery_client_project_id_from_env: bigquery_client_project_id_from_env || "(unset — client may use credentials default)",
    query_location: "US",
  };
}
