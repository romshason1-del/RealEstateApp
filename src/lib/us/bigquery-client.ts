/**
 * US-only BigQuery client. streetiq_gold (NYC truth table) is hosted in EU — client location must be EU.
 * Do not use for France; keep isolated from @/lib/bigquery-client.
 */
if (typeof process !== "undefined") {
  process.env.FAST_QUERY_PATH = "DISABLED";
}

import { BigQuery } from "@google-cloud/bigquery";
import { OAuth2Client } from "google-auth-library";

let usClientInstance: BigQuery | null = null;

function usDataProjectId(): string {
  return (process.env.BIGQUERY_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT_ID || "").trim();
}

export function getUSBigQueryClient(): BigQuery {
  if (!usClientInstance) {
    const projectId = usDataProjectId();
    if (!projectId) {
      throw new Error("BIGQUERY_PROJECT_ID or GOOGLE_CLOUD_PROJECT_ID is required for US BigQuery");
    }

    const accessToken = process.env.GOOGLE_ACCESS_TOKEN?.trim();
    const raw = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
    const credentials = raw
      ? (JSON.parse(raw) as { client_email?: string; private_key?: string; project_id?: string })
      : undefined;
    if (credentials?.private_key) {
      credentials.private_key = credentials.private_key.replace(/\\n/g, "\n");
    }

    type ClientOptions = {
      projectId: string;
      location: string;
      authClient?: OAuth2Client;
      credentials?: { client_email?: string; private_key?: string };
    };

    const clientOptions: ClientOptions = {
      projectId,
      location: "EU",
      ...(credentials ? { credentials } : {}),
    };

    if (!credentials && accessToken) {
      clientOptions.authClient = new OAuth2Client({
        credentials: { access_token: accessToken },
      });
    }

    usClientInstance = new BigQuery(clientOptions);
  }
  return usClientInstance;
}

/** Same project env as shared app; used by /api/us/property-value gate. */
export function isUSNycBigQueryProjectConfigured(): boolean {
  return !!usDataProjectId();
}
