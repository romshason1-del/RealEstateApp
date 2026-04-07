/**
 * NYC-only: lookup `real_estate_us.us_nyc_app_output_final_v4` by address.
 * Source of truth for NYC app card data — see `src/lib/property-value-api.ts` (US → this route).
 * Does not affect France routes or UI.
 */

import { NextRequest, NextResponse } from "next/server";
import { parseUSAddressFromFullString } from "@/lib/address-parse";
import { getUSBigQueryClient } from "@/lib/us/bigquery-client";
import { isUSBigQueryConfigured } from "@/lib/us/us-bigquery";
import { adaptNycAppOutputRowToPropertyPayload } from "@/lib/us/us-nyc-app-output-adapter";
import { queryNycAppOutputFinalV4Row } from "@/lib/us/us-nyc-app-output-query";
import { omitUsNycDebugFromPayload, shouldIncludeUsNycDebugInApiResponse } from "@/lib/us/us-nyc-api-response-debug";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

function logNycAppOutputV4(payload: Record<string, unknown>): void {
  try {
    console.log(
      "[NYC_APP_OUTPUT_V4]",
      JSON.stringify({
        matched_candidate: payload.matched_candidate ?? null,
        row_found: payload.row_found ?? null,
        hierarchy: payload.nyc_display_hierarchy ?? null,
        confidence: payload.nyc_match_confidence ?? null,
        has_exact_transaction: payload.nyc_has_exact_transaction ?? null,
        should_prompt_for_unit: payload.should_prompt_for_unit ?? null,
      })
    );
  } catch {
    /* ignore */
  }
}

export async function GET(req: NextRequest) {
  const address = req.nextUrl.searchParams.get("address")?.trim() ?? "";
  const unitOrLot = req.nextUrl.searchParams.get("unit_or_lot")?.trim() || null;

  if (!address) {
    return NextResponse.json({ success: false, message: "address is required", row: null }, { status: 400 });
  }

  if (!isUSBigQueryConfigured()) {
    return NextResponse.json(
      { success: false, message: "Property records are temporarily unavailable.", row: null },
      { status: 503 }
    );
  }

  try {
    const parsed = parseUSAddressFromFullString(address);
    const client = getUSBigQueryClient();
    const { row, debug } = await queryNycAppOutputFinalV4Row(client, address, unitOrLot);

    const payload = adaptNycAppOutputRowToPropertyPayload(
      row,
      { city: parsed.city, street: parsed.street, houseNumber: parsed.houseNumber },
      { unitOrLotSubmitted: unitOrLot }
    );

    logNycAppOutputV4({
      matched_candidate: debug.matched_candidate,
      row_found: debug.row_found,
      nyc_display_hierarchy: payload.nyc_display_hierarchy,
      nyc_match_confidence: payload.nyc_match_confidence,
      nyc_has_exact_transaction: payload.nyc_has_exact_transaction,
      should_prompt_for_unit: payload.should_prompt_for_unit,
    });

    const body: Record<string, unknown> = {
      ...payload,
      row: row ?? null,
    };
    if (shouldIncludeUsNycDebugInApiResponse()) {
      body.us_nyc_app_output_debug = debug;
    }

    return NextResponse.json(omitUsNycDebugFromPayload(body));
  } catch (e) {
    const msg = e instanceof Error ? e.message : "Query failed";
    return NextResponse.json({ success: false, message: msg, row: null }, { status: 500 });
  }
}
