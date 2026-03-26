/**
 * US property value API (isolated route).
 * NYC: reads only from BigQuery `streetiq-bigquery.streetiq_gold.us_nyc_api_truth`.
 */

import { NextRequest, NextResponse } from "next/server";
import { isUSBigQueryConfigured } from "@/lib/us/us-bigquery";
import { normalizeUSAddressLine } from "@/lib/us/us-address-normalize";
import { buildNycTruthLookupNormalizationDebug } from "@/lib/us/us-nyc-address-normalize";
import {
  queryUSNYCApiTruthWithCandidatesDebug,
  US_NYC_API_TRUTH_SQL_WHERE,
  US_NYC_API_TRUTH_TABLE_REFERENCE,
} from "@/lib/us/us-nyc-api-truth";
import { omitUsNycDebugFromPayload } from "@/lib/us/us-nyc-api-response-debug";
import { createEmptyUSNYCApiTruthResponse } from "@/lib/us/us-response-shape";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

function logUsNycDebug(label: string, payload: object): void {
  try {
    console.log(`[US /api/us/property-value] ${label}`, JSON.stringify(payload));
  } catch {
    console.log(`[US /api/us/property-value] ${label}`, payload);
  }
}

async function handle(addressRaw: string) {
  const { line } = normalizeUSAddressLine(addressRaw);
  if (!line) {
    const empty = createEmptyUSNYCApiTruthResponse({
      success: false,
      message: "address is required",
    });
    const us_nyc_debug = {
      original_input: addressRaw,
      normalized_full_address: null,
      normalized_building_address: null,
      table_name_used: US_NYC_API_TRUTH_TABLE_REFERENCE,
      sql_where_used: US_NYC_API_TRUTH_SQL_WHERE,
      rows_found_count: 0,
      first_row_if_any: null,
      note: "empty address after trim",
    };
    logUsNycDebug("TEMP_DEBUG", us_nyc_debug);
    return NextResponse.json(omitUsNycDebugFromPayload({ ...empty, us_nyc_debug } as Record<string, unknown>), { status: 400 });
  }

  if (!isUSBigQueryConfigured()) {
    const empty = createEmptyUSNYCApiTruthResponse({
      success: false,
      message: "Property records are temporarily unavailable.",
    });
    const us_nyc_debug = {
      original_input: addressRaw,
      normalized_full_address: null,
      normalized_building_address: null,
      table_name_used: US_NYC_API_TRUTH_TABLE_REFERENCE,
      sql_where_used: US_NYC_API_TRUTH_SQL_WHERE,
      rows_found_count: 0,
      first_row_if_any: null,
      note: "BigQuery project env missing",
    };
    logUsNycDebug("TEMP_DEBUG", us_nyc_debug);
    return NextResponse.json(omitUsNycDebugFromPayload({ ...empty, us_nyc_debug } as Record<string, unknown>), { status: 503 });
  }

  try {
    const norm = buildNycTruthLookupNormalizationDebug(line);
    if (!norm) {
      const empty = createEmptyUSNYCApiTruthResponse({
        success: false,
        message: "We couldn't read this address. Check the format and try again.",
      });
      const us_nyc_debug = {
        original_input: addressRaw,
        normalized_full_address: null,
        normalized_building_address: null,
        table_name_used: US_NYC_API_TRUTH_TABLE_REFERENCE,
        sql_where_used: US_NYC_API_TRUTH_SQL_WHERE,
        rows_found_count: 0,
        first_row_if_any: null,
        candidates_tried: [] as string[],
        attempts: [] as { candidate: string; rows_returned: number }[],
        note: "normalization produced empty core",
      };
      logUsNycDebug("TEMP_DEBUG", us_nyc_debug);
      return NextResponse.json(omitUsNycDebugFromPayload({ ...empty, us_nyc_debug } as Record<string, unknown>), { status: 400 });
    }

    const { response, debug } = await queryUSNYCApiTruthWithCandidatesDebug(addressRaw, norm);
    logUsNycDebug("TEMP_DEBUG", { ...debug });
    return NextResponse.json(omitUsNycDebugFromPayload({ ...response, us_nyc_debug: debug } as Record<string, unknown>), {
      status: 200,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : "BigQuery query failed";
    const empty = createEmptyUSNYCApiTruthResponse({
      success: false,
      message: process.env.NODE_ENV === "production" ? "Something went wrong loading property records." : msg,
    });
    const us_nyc_debug = {
      original_input: addressRaw,
      normalized_full_address: null,
      normalized_building_address: null,
      table_name_used: US_NYC_API_TRUTH_TABLE_REFERENCE,
      sql_where_used: US_NYC_API_TRUTH_SQL_WHERE,
      rows_found_count: 0,
      first_row_if_any: null,
      error: msg,
    };
    logUsNycDebug("TEMP_DEBUG", us_nyc_debug);
    return NextResponse.json(omitUsNycDebugFromPayload({ ...empty, us_nyc_debug } as Record<string, unknown>), { status: 500 });
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
