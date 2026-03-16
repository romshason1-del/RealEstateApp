/**
 * Centralized BigQuery client for the application.
 * Reuses a single client instance.
 *
 * ROOT CAUSE of "Cannot parse as CloudRegion": When location is undefined,
 * the BigQuery API receives an invalid value and fails. For EU datasets,
 * location must be explicitly set to "EU".
 *
 * FAST_QUERY_PATH=DISABLED: Avoid jobs.query endpoint which adds formatOptions.timestampOutputFormat
 * by default, causing "timestamp_output_format is not supported yet" on EU. Use createQueryJob instead.
 */
if (typeof process !== "undefined") {
  process.env.FAST_QUERY_PATH = "DISABLED";
}

import { BigQuery } from "@google-cloud/bigquery";

let clientInstance: BigQuery | null = null;

function logEnvVars(): void {
  const envVars = [
    "GOOGLE_CLOUD_PROJECT_ID",
    "GOOGLE_CLOUD_PROJECT",
    "GCLOUD_PROJECT",
    "GOOGLE_CLOUD_QUOTA_PROJECT",
    "BIGQUERY_LOCATION",
    "BIGQUERY_EMULATOR_HOST",
    "GOOGLE_CLOUD_REGION",
    "CLOUDSDK_COMPUTE_REGION",
    "GOOGLE_APPLICATION_CREDENTIALS",
  ];
  const values: Record<string, string> = {};
  for (const k of envVars) {
    const v = process.env[k];
    if (v !== undefined) values[k] = v ? `${v.slice(0, 20)}${v.length > 20 ? "..." : ""}` : "(empty)";
  }
  console.log("[BigQuery] Env vars that may affect client:", values);
}

function logCredentialSource(): void {
  const keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS?.trim();
  const emulator = process.env.BIGQUERY_EMULATOR_HOST;
  let source: string;
  if (emulator) {
    source = "emulator";
  } else if (keyFile) {
    source = "keyFilename";
  } else {
    source = "ADC (Application Default Credentials)";
  }
  console.log("[BigQuery] Credential source:", source);
  if (keyFile) console.log("[BigQuery] keyFilename path:", keyFile);
  if (emulator) console.log("[BigQuery] Emulator host:", emulator);
}

/**
 * Log auth metadata: projectId, principal, credential type.
 * Call after client is created to verify auth resolution.
 */
export async function logAuthMetadata(client: BigQuery): Promise<void> {
  try {
    const projectId = (client as { projectId?: string }).projectId;
    console.log("[BigQuery] Client projectId at runtime:", projectId);

    const authClient = (client as { authClient?: { getCredentials?: () => Promise<{ client_email?: string }>; credential?: { client_email?: string } } }).authClient;
    if (authClient) {
      try {
        const creds = await authClient.getCredentials?.();
        const email = creds?.client_email ?? authClient.credential?.client_email ?? "(unknown)";
        console.log("[BigQuery] Authenticated principal:", email);
      } catch (e) {
        console.log("[BigQuery] Could not get credentials:", (e as Error)?.message);
      }
    }
  } catch (e) {
    console.log("[BigQuery] Auth metadata error:", (e as Error)?.message);
  }
}

export function getBigQueryConfig(): { projectId: string; dataset: string; table: string; location: string } {
  const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID;
  const dataset = process.env.BIGQUERY_DATASET ?? "real_estate_data";
  const table = process.env.BIGQUERY_TABLE ?? "france_transactions";
  const location = process.env.BIGQUERY_LOCATION?.trim() || "EU";
  if (!projectId) {
    throw new Error("GOOGLE_CLOUD_PROJECT_ID is required");
  }
  return { projectId, dataset, table, location };
}

export function getBigQueryClient(): BigQuery {
  if (!clientInstance) {
    logEnvVars();
    logCredentialSource();

    const { projectId, location } = getBigQueryConfig();
    const keyFilename = process.env.GOOGLE_APPLICATION_CREDENTIALS?.trim() || undefined;

    // Explicit projectId required: "Not found: Project X" when ADC project differs from target.
    const clientOptions: { projectId: string; keyFilename?: string; location: string } = {
      projectId,
      ...(keyFilename ? { keyFilename } : {}),
      location,
    };

    console.log("[BigQuery] Constructor options (before new BigQuery):", JSON.stringify(clientOptions, null, 2));
    clientInstance = new BigQuery(clientOptions);
    console.log("[BigQuery] Client created. this.projectId =", (clientInstance as { projectId?: string }).projectId);
    console.log("[BigQuery] Client created. this.location =", (clientInstance as { location?: string }).location);
  }
  return clientInstance;
}

export function isBigQueryConfigured(): boolean {
  return !!process.env.GOOGLE_CLOUD_PROJECT_ID;
}
