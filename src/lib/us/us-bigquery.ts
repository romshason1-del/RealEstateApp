/**
 * US BigQuery access — scaffold only.
 * Future: wire official/public US datasets here. Do not import France BigQuery services.
 */

export type USQueryParams = Record<string, string | number | boolean | null | undefined>;

/** Returns whether US BigQuery env/config is present (stub: always false until implemented). */
export function isUSBigQueryConfigured(): boolean {
  return false;
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
