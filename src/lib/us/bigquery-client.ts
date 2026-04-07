/**
 * US-only BigQuery client. Default `projectId` is `BIGQUERY_PROJECT_ID` || `GOOGLE_CLOUD_PROJECT_ID`
 * (often the Vercel deployment project). That is **not** where NYC v4 data lives: table IDs for
 * `us_nyc_app_output_final_v4` must be fully qualified via `getNycAppOutputTableReference()`
 * in `us-nyc-app-output-constants.ts` (pinned to `streetiq-bigquery`).
 *
 * Used by NYC v4 route readers and legacy NYC SQL helpers — not for France; keep isolated from @/lib/bigquery-client.
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
