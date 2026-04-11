/**
 * NYC-only: lookup `streetiq-bigquery.real_estate_us.us_nyc_property_ui_production_v10` (location US) by address.
 * Source of truth for NYC app card data — see `src/lib/property-value-api.ts` (US → this route).
 * Does not affect France routes or UI.
 */

import { NextRequest, NextResponse } from "next/server";
import { parseUSAddressFromFullString } from "@/lib/address-parse";
import { getUSBigQueryClient } from "@/lib/us/bigquery-client";
import { isUSBigQueryConfigured } from "@/lib/us/us-bigquery";
import { adaptNycPropertyUiProductionRowToPropertyPayload } from "@/lib/us/us-nyc-property-ui-production-adapter";
import { queryNycPropertyUiProductionV10Row } from "@/lib/us/us-nyc-property-ui-production-query";
import { omitUsNycDebugFromPayload, shouldIncludeUsNycDebugInApiResponse } from "@/lib/us/us-nyc-api-response-debug";
import type { NycAppOutputQueryDebug } from "@/lib/us/us-nyc-property-ui-production-query";
import { NYC_PROPERTY_UI_PRODUCTION_V10_COL as NYC_COL } from "@/lib/us/us-nyc-property-ui-production-schema";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

function normalizeUiCardType(raw: unknown): string {
  return String(raw ?? "")
    .toUpperCase()
    .trim()
    .replace(/\s+/g, "_");
}

function logNycPropertyUiV10(
  address: string,
  row: Record<string, unknown> | null,
  payload: Record<string, unknown>,
  debug: NycAppOutputQueryDebug,
  responseStatus: "row_matched" | "no_row"
): void {
  try {
    const r = row;
    const rowSnapshot = r ? { ...r } : null;
    console.log(
      "[NYC_PROPERTY_UI_V10] BQ_ROW_RAW",
      JSON.stringify({
        address_submitted: address,
        table: debug.table,
        row_found: debug.row_found,
        match_column: debug.match_column,
        match_tier: debug.match_tier,
        matched_candidate: debug.matched_candidate,
        row: rowSnapshot,
      })
    );
    console.log(
      "[NYC_PROPERTY_UI_V10] BQ_ROW_KEY_FIELDS",
      JSON.stringify({
        address_submitted: address,
        row_found: debug.row_found,
        ui_card_type: r ? normalizeUiCardType(r[NYC_COL.ui_card_type]) : null,
        [NYC_COL.lookup_address]: r?.[NYC_COL.lookup_address] ?? null,
        [NYC_COL.confidence_label]: r?.[NYC_COL.confidence_label] ?? null,
        [NYC_COL.deal_score_numeric]: r?.[NYC_COL.deal_score_numeric] ?? null,
        [NYC_COL.display_estimated_value]: r?.[NYC_COL.display_estimated_value] ?? null,
        [NYC_COL.last_transaction_amount]: r?.[NYC_COL.last_transaction_amount] ?? null,
      })
    );
    console.log(
      "[NYC_PROPERTY_UI_V10] API_PAYLOAD_FINAL",
      JSON.stringify({ address_submitted: address, response_status: responseStatus, ...payload })
    );
    console.log(
      "[NYC_PROPERTY_UI_V10] MATCH_SUMMARY",
      JSON.stringify({
        table: debug.table,
        response_status: responseStatus,
        raw_input: debug.raw_input,
        normalized_pipeline_input: debug.normalized_pipeline_input,
        candidates_count: debug.candidates_tried.length,
        norm_keys_tried: debug.norm_keys_tried,
        unit_norm_keys_tried: debug.unit_norm_keys_tried,
        match_column: debug.match_column,
        match_tier: debug.match_tier,
        matched_norm_key: debug.matched_norm_key,
        matched_stored_lookup_address: debug.matched_stored_lookup_address,
        matched_stored_property_address: debug.matched_stored_property_address,
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
    const { row, debug } = await queryNycPropertyUiProductionV10Row(client, address, unitOrLot);

    const payload = adaptNycPropertyUiProductionRowToPropertyPayload(
      row,
      { city: parsed.city, street: parsed.street, houseNumber: parsed.houseNumber },
      { unitOrLotSubmitted: unitOrLot }
    );

    logNycPropertyUiV10(
      address,
      row,
      payload,
      debug,
      debug.row_found ? "row_matched" : "no_row"
    );

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
