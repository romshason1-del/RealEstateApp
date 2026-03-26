/**
 * Maps `/api/us/property-value` NYC truth JSON into the shape the main property-value UI expects.
 * US-only — do not use for France.
 */

import { fetchAcrisNycTruthDeedHistory } from "@/lib/us/acris/acris-truth";
import { fetchDobNycBuildingInsights } from "@/lib/us/dob/dob-truth";
import { shouldIncludeUsNycDebugInApiResponse } from "@/lib/us/us-nyc-api-response-debug";
import { coerceBigQueryDateToYyyyMmDd } from "./us-bq-date";

function num(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  const x = Number(String(v));
  return Number.isFinite(x) ? x : null;
}

function str(v: unknown): string | null {
  if (v == null || v === "") return null;
  return String(v);
}

/** Last-token expansion for ACRIS Real Property Legals `street_name` matching only (not UI / not BigQuery). */
const ACRIS_STREET_SUFFIX_MAP: Record<string, string> = {
  ST: "STREET",
  AVE: "AVENUE",
  RD: "ROAD",
  BLVD: "BOULEVARD",
  DR: "DRIVE",
  CT: "COURT",
  LN: "LANE",
  PL: "PLACE",
  TER: "TERRACE",
  PKWY: "PARKWAY",
};

function normalizeStreetNameForAcrisLegals(raw: string): string {
  const s = raw.trim().replace(/\s+/g, " ").toUpperCase();
  if (!s) return "";
  const parts = s.split(" ");
  const lastRaw = parts[parts.length - 1] ?? "";
  const last = lastRaw.replace(/\.$/, "").toUpperCase();
  const expanded = ACRIS_STREET_SUFFIX_MAP[last];
  if (expanded) {
    parts[parts.length - 1] = expanded;
  }
  return parts.join(" ");
}

/**
 * When `success` is true, merges truth fields + `address` + minimal `property_result` for legacy checks.
 * Attaches `us_nyc_debug` only in non-production API responses.
 * NYC UI uses `data_source === "us_nyc_truth"` and reads top-level metrics (no street-average presentation).
 *
 * Enrichment: optional ACRIS deed history (secondary validation vs truth `latest_sale_*`; does not replace them).
 * Enrichment: optional DOB job filings summary (`dob_*`; does not replace truth or ACRIS).
 *
 * Fallback: when BigQuery truth returns no property row (`has_truth_property_row === false`), enrich from
 * street pricing + ACRIS so the card is not fully empty when either source has data.
 *
 * Street-only query (no `ctx.houseNumber`): street pricing only, no ACRIS/DOB/property-level truth.
 */
export async function adaptUsNycTruthJsonForMainPropertyValueRoute(
  us: Record<string, unknown>,
  ctx: { city: string; street: string; houseNumber: string }
): Promise<Record<string, unknown>> {
  const city = ctx.city.trim() || "—";
  const street = ctx.street.trim() || "—";
  const houseNumber = ctx.houseNumber.trim() || "—";
  const rawHouse = ctx.houseNumber.trim();
  const isStreetOnlyQuery = rawHouse === "" || rawHouse === "—";

  const estimated_value = num(us.estimated_value);
  const latest_sale_price = num(us.latest_sale_price);
  const latest_sale_date = coerceBigQueryDateToYyyyMmDd(us.latest_sale_date);
  const latest_sale_total_units = num(us.latest_sale_total_units);
  const avg_street_price = num(us.avg_street_price);
  const avg_street_price_per_sqft = num(us.avg_street_price_per_sqft);
  const transaction_count = num(us.transaction_count);
  const price_per_sqft = num(us.price_per_sqft);
  const sales_address = str(us.sales_address);
  const pluto_address = str(us.pluto_address);
  const street_name = str(us.street_name);
  const truthHouseNumber = str(us.house_number);

  if (isStreetOnlyQuery) {
    const ev = avg_street_price != null && avg_street_price > 0 ? avg_street_price : null;
    const skippedAcrisDebug = { success: false, deed_count: 0, matched: null };
    const skippedDobDebug = {
      success: false,
      has_filings: null,
      filing_count: null,
      building_type: null,
      existing_units: null,
      proposed_units: null,
    };
    const outStreet: Record<string, unknown> = {
      success: us.success === true,
      data_source: "us_nyc_truth",
      message: null,
      address: { city, street, house_number: houseNumber },
      ...(typeof us.has_truth_property_row === "boolean" ? { has_truth_property_row: us.has_truth_property_row } : {}),
      estimated_value: ev,
      latest_sale_price: null,
      latest_sale_date: null,
      latest_sale_total_units: null,
      avg_street_price,
      avg_street_price_per_sqft,
      transaction_count,
      price_per_sqft,
      sales_address,
      pluto_address,
      street_name,
      property_result: {
        exact_value: ev,
        exact_value_message: ev != null && ev > 0 ? null : "Unavailable",
        value_level: "street-level" as const,
        last_transaction: {
          amount: 0,
          date: null,
          message: "No specific property selected",
        },
        street_average: ev,
        street_average_message: ev != null && ev > 0 ? null : "No street average",
        livability_rating: "FAIR" as const,
      },
      acris_last_sale_price: null,
      acris_last_sale_date: null,
      acris_has_multiple_deeds: false,
      dob_has_filings: false,
      dob_filing_count: 0,
      dob_building_type: null,
      dob_existing_units: null,
      dob_proposed_units: null,
    };
    if (shouldIncludeUsNycDebugInApiResponse()) {
      const priorDebug =
        us.us_nyc_debug != null && typeof us.us_nyc_debug === "object" && !Array.isArray(us.us_nyc_debug)
          ? { ...(us.us_nyc_debug as Record<string, unknown>) }
          : {};
      outStreet.us_nyc_debug = { ...priorDebug, acris_debug: skippedAcrisDebug, dob_debug: skippedDobDebug };
    }
    return outStreet;
  }

  const lastAmt = latest_sale_price != null && latest_sale_price > 0 ? latest_sale_price : 0;

  const property_result = {
    exact_value: estimated_value,
    exact_value_message:
      estimated_value != null && estimated_value > 0
        ? null
        : lastAmt > 0
          ? null
          : "Unavailable",
    value_level: "property-level" as const,
    last_transaction: {
      amount: lastAmt,
      date: latest_sale_date,
      message: lastAmt > 0 ? undefined : "No official sale recorded",
    },
    street_average: null,
    street_average_message: null,
    livability_rating: "FAIR" as const,
  };

  const out: Record<string, unknown> = {
    success: us.success === true,
    data_source: "us_nyc_truth",
    message: null,
    address: { city, street, house_number: houseNumber },
    ...(typeof us.has_truth_property_row === "boolean" ? { has_truth_property_row: us.has_truth_property_row } : {}),
    estimated_value,
    latest_sale_price,
    latest_sale_date,
    latest_sale_total_units,
    avg_street_price,
    avg_street_price_per_sqft,
    transaction_count,
    price_per_sqft,
    sales_address,
    pluto_address,
    street_name,
    property_result,
  };

  if (lastAmt > 0 && latest_sale_date) {
    out.last_sale = { price: lastAmt, date: latest_sale_date };
  }

  const acrisStreetNumber = (truthHouseNumber ?? "").trim() || houseNumber;
  const acrisStreetNameRaw = (street_name ?? "").trim() || street;
  const acrisStreetName = normalizeStreetNameForAcrisLegals(acrisStreetNameRaw);

  let acris_last_sale_price: number | null = null;
  let acris_last_sale_date: string | null = null;
  let acris_has_multiple_deeds = false;
  let acris_debug: { success: boolean; deed_count: number; matched: unknown } = {
    success: false,
    deed_count: 0,
    matched: null,
  };

  if (acrisStreetNumber && acrisStreetName) {
    const acris = await fetchAcrisNycTruthDeedHistory({
      streetNumber: acrisStreetNumber,
      streetName: acrisStreetName,
    });

    if (acris.success) {
      acris_last_sale_price = acris.latest_deed?.document_amt ?? null;
      acris_last_sale_date = acris.latest_deed?.document_date
        ? coerceBigQueryDateToYyyyMmDd(acris.latest_deed.document_date)
        : null;
      acris_has_multiple_deeds = acris.has_multiple_deeds;
      acris_debug = {
        success: true,
        deed_count: acris.deeds.length,
        matched: acris.latest_deed ?? null,
      };
    } else {
      acris_debug = {
        success: false,
        deed_count: 0,
        matched: null,
      };
    }
  }

  let dob_has_filings = false;
  let dob_filing_count = 0;
  let dob_building_type: string | null = null;
  let dob_existing_units: number | null = null;
  let dob_proposed_units: number | null = null;

  let dob_debug: {
    success: boolean;
    has_filings: boolean | null;
    filing_count: number | null;
    building_type: string | null;
    existing_units: number | null;
    proposed_units: number | null;
  } = {
    success: false,
    has_filings: null,
    filing_count: null,
    building_type: null,
    existing_units: null,
    proposed_units: null,
  };

  if (acrisStreetNumber && acrisStreetNameRaw) {
    const dob = await fetchDobNycBuildingInsights({
      houseNumber: acrisStreetNumber,
      streetName: acrisStreetNameRaw,
    });

    if (dob.success) {
      dob_has_filings = dob.has_filings;
      dob_filing_count = dob.filing_count;
      dob_building_type = dob.building_type;
      dob_existing_units = dob.existing_units;
      dob_proposed_units = dob.proposed_units;
      dob_debug = {
        success: true,
        has_filings: dob.has_filings,
        filing_count: dob.filing_count,
        building_type: dob.building_type,
        existing_units: dob.existing_units,
        proposed_units: dob.proposed_units,
      };
    } else {
      dob_debug = {
        success: false,
        has_filings: null,
        filing_count: null,
        building_type: null,
        existing_units: null,
        proposed_units: null,
      };
    }
  }

  out.acris_last_sale_price = acris_last_sale_price;
  out.acris_last_sale_date = acris_last_sale_date;
  out.acris_has_multiple_deeds = acris_has_multiple_deeds;

  out.dob_has_filings = dob_has_filings;
  out.dob_filing_count = dob_filing_count;
  out.dob_building_type = dob_building_type;
  out.dob_existing_units = dob_existing_units;
  out.dob_proposed_units = dob_proposed_units;

  /** Only explicit `false` from `/api/us/property-value` enables fallback; missing flag does not. */
  const noTruthPropertyRow = us.has_truth_property_row === false;

  const hasStreetPricing = avg_street_price != null && avg_street_price > 0;
  const hasAcrisSale = acris_last_sale_price != null && acris_last_sale_price > 0;

  if (noTruthPropertyRow && (hasStreetPricing || hasAcrisSale)) {
    const estimatedValueFallback = hasStreetPricing ? avg_street_price : acris_last_sale_price;
    const lastTxAmt = hasAcrisSale ? acris_last_sale_price! : 0;
    const lastTxDate = hasAcrisSale ? acris_last_sale_date : null;

    out.estimated_value = estimatedValueFallback;
    out.latest_sale_price = hasAcrisSale ? acris_last_sale_price : null;
    out.latest_sale_date = hasAcrisSale ? acris_last_sale_date : null;

    if (hasAcrisSale && lastTxDate) {
      out.last_sale = { price: lastTxAmt, date: lastTxDate };
    } else {
      delete out.last_sale;
    }

    out.property_result = {
      exact_value: estimatedValueFallback,
      exact_value_message:
        estimatedValueFallback != null && estimatedValueFallback > 0 ? null : "Unavailable",
      value_level: "street-level" as const,
      last_transaction: {
        amount: lastTxAmt,
        date: lastTxDate,
        message: hasAcrisSale ? undefined : "No official sale recorded",
      },
      street_average: hasStreetPricing ? avg_street_price : null,
      street_average_message: hasStreetPricing ? null : "No street average",
      livability_rating: "FAIR" as const,
    };
  }

  if (shouldIncludeUsNycDebugInApiResponse()) {
    const priorDebug =
      us.us_nyc_debug != null && typeof us.us_nyc_debug === "object" && !Array.isArray(us.us_nyc_debug)
        ? { ...(us.us_nyc_debug as Record<string, unknown>) }
        : {};
    out.us_nyc_debug = { ...priorDebug, acris_debug, dob_debug };
  }

  return out;
}
