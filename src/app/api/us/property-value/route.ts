/**
 * US property value API (isolated route).
 * NYC: reads only from BigQuery `streetiq-bigquery.streetiq_gold.us_nyc_api_truth`.
 */

import { NextRequest, NextResponse } from "next/server";
import { isUSBigQueryConfigured } from "@/lib/us/us-bigquery";
import { normalizeUSAddressLine } from "@/lib/us/us-address-normalize";
import { buildNycTruthLookupCandidates } from "@/lib/us/us-nyc-address-normalize";
import { queryUSNYCApiTruthWithCandidates } from "@/lib/us/us-nyc-api-truth";
import { createEmptyUSNYCApiTruthResponse } from "@/lib/us/us-response-shape";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

async function handle(addressRaw: string) {
  const { line } = normalizeUSAddressLine(addressRaw);
  if (!line) {
    return NextResponse.json(
      createEmptyUSNYCApiTruthResponse({
        success: false,
        message: "address is required",
      }),
      { status: 400 }
    );
  }

  if (!isUSBigQueryConfigured()) {
    return NextResponse.json(
      createEmptyUSNYCApiTruthResponse({
        success: false,
        message: "BigQuery is not configured (set BIGQUERY_PROJECT_ID or GOOGLE_CLOUD_PROJECT_ID)",
      }),
      { status: 503 }
    );
  }

  try {
    const candidates = buildNycTruthLookupCandidates(line);
    if (candidates.length === 0) {
      return NextResponse.json(
        createEmptyUSNYCApiTruthResponse({
          success: false,
          message: "address could not be normalized for lookup",
        }),
        { status: 400 }
      );
    }
    const body = await queryUSNYCApiTruthWithCandidates(candidates);
    return NextResponse.json(body);
  } catch (e) {
    const msg = e instanceof Error ? e.message : "BigQuery query failed";
    return NextResponse.json(
      createEmptyUSNYCApiTruthResponse({
        success: false,
        message: msg,
      }),
      { status: 500 }
    );
  }
}

export async function GET(req: NextRequest) {
  const address = req.nextUrl.searchParams.get("address") ?? "";
  return handle(address);
}

export async function POST(req: NextRequest) {
  let address = "";
  try {
    const body = (await req.json()) as { address?: string };
    address = typeof body.address === "string" ? body.address : "";
  } catch {
    address = "";
  }
  return handle(address);
}
