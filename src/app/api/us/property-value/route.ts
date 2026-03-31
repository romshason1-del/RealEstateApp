/**
 * US property value API (isolated route).
 * NYC: BigQuery precomputed `us_nyc_card_output_v5` + `us_nyc_last_transaction_engine_v3` (exact `full_address`).
 * Candidate hints may prepend rows from `us_nyc_address_master_v1` (see `scripts/us/nyc/sql/build-us-nyc-address-master-v1.sql`).
 */

import { NextRequest, NextResponse } from "next/server";
import { parseUSAddressFromFullString } from "@/lib/address-parse";
import { isUSBigQueryConfigured } from "@/lib/us/us-bigquery";
import { normalizeUSAddressLine } from "@/lib/us/us-address-normalize";
import { buildNycTruthLookupNormalizationDebug } from "@/lib/us/us-nyc-address-normalize";
import { adaptUsNycTruthJsonForMainPropertyValueRoute } from "@/lib/us/us-nyc-main-payload";
import { getUSBigQueryClient } from "@/lib/us/bigquery-client";
import {
  normalizeNycAddressMasterV1Line,
  queryBuildingTruthFullAddressesFromAddressMaster,
  queryUnitFromAddressMaster,
} from "@/lib/us/us-nyc-address-master";
import { queryPlutoResidentialMultiUnitGate } from "@/lib/us/us-nyc-pluto-gate";
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

async function handle(addressRaw: string, unitOrLotRaw?: string | null, unitParamRaw?: string | null) {
  console.log("[STREETIQ_FORCE_CHECK] handle() called, address:", addressRaw);
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
    const unitFromParam =
      typeof unitParamRaw === "string" && unitParamRaw.trim() !== "" ? unitParamRaw.trim() : null;
    const combinedUnit = unitFromParam ?? unitOrLot;
    const hasSubmittedUnit = typeof combinedUnit === "string" && combinedUnit.trim() !== "";

    const client = getUSBigQueryClient();

    const plutoGate = await queryPlutoResidentialMultiUnitGate(
      client,
      norm.normalized_building_address,
      norm.zip_from_input ?? null
    );
    const gateBblFromPluto = plutoGate.hit ? plutoGate.bbl : null;
    if (plutoGate.hit && plutoGate.strictMultiUnitResidential && !hasSubmittedUnit) {
      return NextResponse.json(
        {
          status: "requires_unit",
          message: "Please enter a unit number to see specific valuation and sales history",
          property: null,
          valuation: null,
          lastTransaction: null,
          nyc_pluto_strict_unit_gate: true,
        },
        { status: 200 }
      );
    }

    const masterLine =
      norm.normalized_building_address.trim() ||
      norm.normalized_full_address.split(",")[0]?.replace(/\s+/g, " ").trim() ||
      "";
    const masterNorm = normalizeNycAddressMasterV1Line(masterLine);

    // Single master gate (includes combined unit): commercial must run on every request, including after unit submit.
    const masterGate = await queryBuildingTruthFullAddressesFromAddressMaster(
      client,
      masterNorm,
      combinedUnit
    );
    const gateBbl =
      gateBblFromPluto ??
      (masterGate.requiresUnit && "buildingData" in masterGate
        ? masterGate.buildingData.bbl
        : !masterGate.requiresUnit && "bbl" in masterGate
          ? masterGate.bbl ?? null
          : null);
    const gateBldgClass = "bldgclass" in masterGate ? masterGate.bldgclass : undefined;
    console.log("[GATE_BLDGCLASS]", gateBldgClass, "isCommercial:", masterGate.isCommercial);
    if (masterGate.isCommercial) {
      return NextResponse.json(
        {
          status: "commercial_property",
          message: "Commercial property — limited residential data available",
          property: null,
          valuation: null,
          lastTransaction: null,
        },
        { status: 200 }
      );
    }
    if (masterGate.requiresUnit && !hasSubmittedUnit) {
      return NextResponse.json(
        {
          status: "requires_unit",
          message: "Please enter a unit number to see specific valuation and sales history",
          property: null,
          valuation: null,
          lastTransaction: null,
        },
        { status: 200 }
      );
    }

    let addressMasterUnitSearch: Awaited<ReturnType<typeof queryUnitFromAddressMaster>> | null = null;
    if (combinedUnit && masterNorm) {
      const zmFromInput =
        typeof norm.zip_from_input === "string" && norm.zip_from_input.trim() !== ""
          ? norm.zip_from_input.trim()
          : null;
      addressMasterUnitSearch = await queryUnitFromAddressMaster(
        client,
        masterNorm,
        combinedUnit,
        zmFromInput,
        gateBbl
      );
    }

    const { response, debug } = await queryUSNYCApiTruthWithCandidatesDebug(addressRaw, norm, {
      unitOrLot: combinedUnit,
      bblHint: gateBbl,
    });
    const debugOut =
      addressMasterUnitSearch != null
        ? { ...debug, address_master_unit_search: addressMasterUnitSearch }
        : debug;
    logUsNycDebug("TEMP_DEBUG", { ...debugOut });

    if (process.env.NYC_LOG_PROPERTY_VALUE_FIELDS === "1") {
      const row = debugOut.first_row_if_any as Record<string, unknown> | null | undefined;
      const ups = parseUSAddressFromFullString(addressRaw);
      const adapted = await adaptUsNycTruthJsonForMainPropertyValueRoute(
        { ...response, us_nyc_debug: debugOut } as Record<string, unknown>,
        { city: ups.city, street: ups.street, houseNumber: ups.houseNumber }
      );
      const pr = adapted.property_result as Record<string, unknown> | undefined;
      try {
        console.log(
          "[NYC_PROPERTY_VALUE_FIELDS]",
          JSON.stringify({
            route: "us_property_value",
            address_searched: addressRaw,
            address_master_normalized: debugOut.address_master_normalized ?? null,
            address_master_hint_full_addresses: debugOut.address_master_hint_full_addresses ?? null,
            candidate_generator_version: norm.candidate_generator_version ?? null,
            zip_from_input: norm.zip_from_input ?? null,
            normalized_candidates: norm.candidates,
            candidates_after_address_master: debugOut.candidates_tried ?? null,
            final_selected_candidate: debugOut.final_selected_candidate ?? null,
            precomputed_row_matched: debugOut.precomputed_row_matched ?? null,
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

    const responseOut = { ...response, us_nyc_debug: debugOut } as Record<string, unknown>;
    if (!responseOut.status && responseOut.estimated_value != null) {
      responseOut.status = "success";
    }
    return NextResponse.json(omitUsNycDebugFromPayload(responseOut), {
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
  const unit = req.nextUrl.searchParams.get("unit");
  return handle(address, unitOrLot, unit);
}

export async function POST(req: NextRequest) {
  let address = "";
  let unitOrLot: string | null = null;
  let unit: string | null = null;
  try {
    const body = (await req.json()) as { address?: string; unit_or_lot?: string; unit?: string };
    address = typeof body.address === "string" ? body.address : "";
    unitOrLot = typeof body.unit_or_lot === "string" ? body.unit_or_lot : null;
    unit = typeof body.unit === "string" ? body.unit : null;
  } catch {
    address = "";
  }
  return handle(address, unitOrLot, unit);
}
