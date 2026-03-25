/**
 * US BigQuery access — NYC uses the same app BigQuery client/env as other routes.
 * Do not import France BigQuery query services (`bigquery-france-service`).
 */

import { isBigQueryConfigured } from "@/lib/bigquery-client";

export type USQueryParams = Record<string, string | number | boolean | null | undefined>;

/** True when the shared data project env is set (required to run NYC truth-table queries). */
export function isUSBigQueryConfigured(): boolean {
  return isBigQueryConfigured();
}

/**
 * Placeholder for a dedicated US query runner.
 * Implement with @google-cloud/bigquery or project standard when datasets are ready.
 */
export type USBigQueryRunner = {
  runQuery: (_sql: string, _params?: USQueryParams) => Promise<readonly unknown[]>;
};

export function createUSBigQueryRunner(): USBigQueryRunner | null {
  return null;
}
