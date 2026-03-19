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
 *
 * Credentials (in order of precedence):
 * 1. GOOGLE_ACCESS_TOKEN — OAuth2 access token (e.g. from Vercel / serverless); no key file required.
 * 2. GOOGLE_APPLICATION_CREDENTIALS — path to service account JSON (optional, local/dev).
 * 3. Application Default Credentials (ADC) — when neither of the above is set.
 */
if (typeof process !== "undefined") {
  process.env.FAST_QUERY_PATH = "DISABLED";
}

import { BigQuery } from "@google-cloud/bigquery";
import { OAuth2Client } from "google-auth-library";

let clientInstance: BigQuery | null = null;

function logEnvVars(): void {
  const envVars = [
    "GOOGLE_CLOUD_PROJECT_ID",
    "GOOGLE_CLOUD_PROJECT",
    "GCLOUD_PROJECT",
    "GOOGLE_CLOUD_QUOTA_PROJECT",
    "BIGQUERY_PROJECT_ID",
    "BIGQUERY_LOCATION",
    "BIGQUERY_EMULATOR_HOST",
    "GOOGLE_CLOUD_REGION",
    "CLOUDSDK_COMPUTE_REGION",
    "GOOGLE_ACCESS_TOKEN",
    "GOOGLE_APPLICATION_CREDENTIALS_JSON",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "VERCEL",
    "NODE_ENV",
  ];
  const values: Record<string, string> = {};
  for (const k of envVars) {
    const v = process.env[k];
    if (v !== undefined) values[k] = v ? `${v.slice(0, 20)}${v.length > 20 ? "..." : ""}` : "(empty)";
  }
  console.log("[BigQuery] Env vars that may affect client:", values);
}

function logCredentialSource(): void {
  const accessToken = process.env.GOOGLE_ACCESS_TOKEN?.trim();
  const credsJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON?.trim();
  const keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS?.trim();
  const emulator = process.env.BIGQUERY_EMULATOR_HOST;
  let source: string;
  if (emulator) {
    source = "emulator";
  } else if (credsJson) {
    source = "GOOGLE_APPLICATION_CREDENTIALS_JSON";
  } else if (accessToken) {
    source = "GOOGLE_ACCESS_TOKEN";
  } else if (keyFile) {
    source = "keyFilename";
  } else {
    source = "ADC (Application Default Credentials)";
  }
  console.log("[BigQuery] Credential source:", source);
  if (accessToken) console.log("[BigQuery] Using GOOGLE_ACCESS_TOKEN");
  if (credsJson) console.log("[BigQuery] Using GOOGLE_APPLICATION_CREDENTIALS_JSON");
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
  // Data project (where the dataset/table live).
  // This may be different from the auth project embedded in the service account JSON.
  const projectId = (process.env.BIGQUERY_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT_ID || "").trim();
  const dataset = process.env.BIGQUERY_DATASET ?? "real_estate_data";
  const table = process.env.BIGQUERY_TABLE ?? "france_transactions";
  const location = process.env.BIGQUERY_LOCATION?.trim() || "EU";
  if (!projectId) {
    throw new Error("BIGQUERY_PROJECT_ID (or GOOGLE_CLOUD_PROJECT_ID fallback) is required");
  }
  return { projectId, dataset, table, location };
}

export function getBigQueryClient(): BigQuery {
  if (!clientInstance) {
    logEnvVars();
    logCredentialSource();

    const { projectId: dataProjectId, dataset, table, location } = getBigQueryConfig();

    const accessToken = process.env.GOOGLE_ACCESS_TOKEN?.trim();
    const raw = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
    const credentials = raw
      ? (JSON.parse(raw) as { client_email?: string; private_key?: string; project_id?: string })
      : undefined;
    if (credentials?.private_key) {
      credentials.private_key = credentials.private_key.replace(/\\n/g, "\n");
    }
    const isProd = process.env.NODE_ENV === "production" || !!process.env.VERCEL;
    const authProjectId = credentials?.project_id?.trim() || undefined;

    type ClientOptions = {
      projectId: string;
      location?: string;
      authClient?: OAuth2Client;
      credentials?: { client_email?: string; private_key?: string };
    };

    // Production-safe auth preference:
    // 1) GOOGLE_APPLICATION_CREDENTIALS_JSON (service account JSON) if present
    // 2) GOOGLE_ACCESS_TOKEN if present
    // 3) ADC (useful locally)
    //
    // NOTE: In production, we intentionally do NOT use keyFilename / GOOGLE_APPLICATION_CREDENTIALS path.
    const clientOptions: ClientOptions = {
      projectId: dataProjectId,
      // location is passed per-query in this codebase; keep it optional here.
      ...(location ? { location } : {}),
      ...(credentials ? { credentials } : {}),
    };

    if (!credentials && accessToken) {
      const authClient = new OAuth2Client({
        credentials: { access_token: accessToken },
      });
      clientOptions.authClient = authClient;
    }

    // Production logs (no secrets)
    const emailExists = !!credentials?.client_email;
    const pkExists = !!credentials?.private_key;
    const pkHasHeader = !!credentials?.private_key?.startsWith("-----BEGIN PRIVATE KEY-----");
    console.log("[BigQuery] Production auth check:", {
      authProjectId: authProjectId ?? "(missing)",
      dataProjectId,
      dataset,
      table,
      isProd,
      credentialsLoaded: !!credentials,
      clientEmailExists: emailExists,
      privateKeyExists: pkExists,
      privateKeyHasHeader: pkHasHeader,
      usingAccessToken: !credentials && !!accessToken,
      usingKeyFilename: false,
      usingGOOGLE_APPLICATION_CREDENTIALS_path: false,
    });

    const safeLogOptions = { ...clientOptions } as Record<string, unknown>;
    if (safeLogOptions.authClient) safeLogOptions.authClient = "[OAuth2Client]";
    if (safeLogOptions.credentials) {
      safeLogOptions.credentials = {
        client_email: emailExists ? "[set]" : "[missing]",
        private_key: pkExists ? "[set]" : "[missing]",
      };
    }
    console.log("[BigQuery] Constructor options (before new BigQuery):", JSON.stringify(safeLogOptions, null, 2));

    // Per requirements:
    // - GOOGLE_APPLICATION_CREDENTIALS_JSON is used only for authentication credentials
    // - BIGQUERY_PROJECT_ID selects the data project for queries/table references
    // - We do NOT force projectId = credentials.project_id
    clientInstance = new BigQuery({
      projectId: dataProjectId,
      credentials,
    });

    console.log("[BigQuery] Client created. this.projectId =", (clientInstance as { projectId?: string }).projectId);
    console.log("[BigQuery] Client created. this.location =", (clientInstance as { location?: string }).location);
  }
  return clientInstance;
}

export function isBigQueryConfigured(): boolean {
  // Data project is selected via BIGQUERY_PROJECT_ID in this codebase.
  // Keep GOOGLE_CLOUD_PROJECT_ID as a fallback for older env setups.
  return !!((process.env.BIGQUERY_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT_ID || "").trim());
}
