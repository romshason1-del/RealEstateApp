/**
 * US property value API (isolated route).
 * NYC: BigQuery precomputed `us_nyc_card_output_v5` + `us_nyc_last_transaction_engine_v3` (exact `full_address`).
 */

import { NextRequest, NextResponse } from "next/server";
import { parseUSAddressFromFullString } from "@/lib/address-parse";
import { isUSBigQueryConfigured } from "@/lib/us/us-bigquery";
import { normalizeUSAddressLine } from "@/lib/us/us-address-normalize";
import { buildNycTruthLookupNormalizationDebug } from "@/lib/us/us-nyc-address-normalize";
import { adaptUsNycTruthJsonForMainPropertyValueRoute } from "@/lib/us/us-nyc-main-payload";
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

async function handle(addressRaw: string, unitOrLotRaw?: string | null) {
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

    const unitOrLot =
      typeof unitOrLotRaw === "string" && unitOrLotRaw.trim() !== "" ? unitOrLotRaw.trim() : null;
    const { response, debug } = await queryUSNYCApiTruthWithCandidatesDebug(addressRaw, norm, {
      unitOrLot,
    });
    logUsNycDebug("TEMP_DEBUG", { ...debug });

    if (process.env.NYC_LOG_PROPERTY_VALUE_FIELDS === "1") {
      const row = debug.first_row_if_any as Record<string, unknown> | null | undefined;
      const ups = parseUSAddressFromFullString(addressRaw);
      const adapted = await adaptUsNycTruthJsonForMainPropertyValueRoute(
        { ...response, us_nyc_debug: debug } as Record<string, unknown>,
        { city: ups.city, street: ups.street, houseNumber: ups.houseNumber }
      );
      const pr = adapted.property_result as Record<string, unknown> | undefined;
      try {
        console.log(
          "[NYC_PROPERTY_VALUE_FIELDS]",
          JSON.stringify({
            route: "us_property_value",
            address_searched: addressRaw,
            candidate_generator_version: norm.candidate_generator_version ?? null,
            zip_from_input: norm.zip_from_input ?? null,
            normalized_candidates: norm.candidates,
            final_selected_candidate: debug.final_selected_candidate ?? null,
            precomputed_row_matched: debug.precomputed_row_matched ?? null,
            matched_full_address: (row?.full_address as string | undefined) ?? response.nyc_card_full_address ?? null,
            building_type: row?.building_type ?? null,
            unit_count: row?.unit_count ?? null,
            nyc_pending_unit_prompt: response.nyc_pending_unit_prompt ?? null,
            should_prompt_for_unit: adapted.should_prompt_for_unit ?? null,
            unit_prompt_reason: adapted.unit_prompt_reason ?? null,
            unit_lookup_status: response.unit_lookup_status ?? null,
            nyc_final_match_level: response.nyc_final_match_level ?? null,
            nyc_fallback_used: response.nyc_fallback_used ?? null,
            nyc_fallback_type: response.nyc_fallback_type ?? null,
            nyc_fallback_score_reason: response.nyc_fallback_score_reason ?? null,
            nyc_final_transaction_match_level: response.nyc_final_transaction_match_level ?? null,
            estimated_value: response.estimated_value ?? null,
            latest_sale_price: response.latest_sale_price ?? null,
            latest_sale_date: response.latest_sale_date ?? null,
            property_result_value_level: pr?.value_level ?? null,
            property_result_exact_value_message: pr?.exact_value_message ?? null,
          })
        );
      } catch {
        /* ignore */
      }
    }

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
  const unitOrLot = req.nextUrl.searchParams.get("unit_or_lot");
  return handle(address, unitOrLot);
}

export async function POST(req: NextRequest) {
  let address = "";
  let unitOrLot: string | null = null;
  try {
    const body = (await req.json()) as { address?: string; unit_or_lot?: string };
    address = typeof body.address === "string" ? body.address : "";
    unitOrLot = typeof body.unit_or_lot === "string" ? body.unit_or_lot : null;
  } catch {
    address = "";
  }
  return handle(address, unitOrLot);
}
