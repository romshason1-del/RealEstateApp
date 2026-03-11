#!/usr/bin/env tsx
/**
 * US Valuation Trust Validation
 * Tests 10 residential addresses and reports valuation path.
 * Run with dev server: npm run dev (in another terminal)
 * Then: npx tsx scripts/validate-us-valuation.ts
 */
const BASE = process.env.VALIDATE_API_BASE ?? "http://localhost:3000";

const TEST_ADDRESSES = [
  "123 Oak St, Springfield, IL 62701",
  "456 Maple Ave, Columbus, OH 43215",
  "789 Elm St, Kansas City, MO 64108",
  "321 Pine Rd, Denver, CO 80202",
  "555 Cedar Ln, Austin, TX 78701",
  "100 Birch Dr, Nashville, TN 37201",
  "200 Walnut St, Charlotte, NC 28202",
  "300 Spruce Ave, Minneapolis, MN 55401",
  "400 Ash Blvd, Portland, OR 97201",
  "500 Hickory Way, Seattle, WA 98101",
  "600 Willow Ct, Phoenix, AZ 85001",
  "700 Chestnut St, Philadelphia, PA 19101",
  "800 Cherry Ln, Indianapolis, IN 46201",
  "900 Poplar Rd, Atlanta, GA 30301",
  "1100 Magnolia Dr, Miami, FL 33101",
  "1200 Dogwood St, San Antonio, TX 78201",
  "1300 Sycamore Ave, Detroit, MI 48201",
  "1400 Oakwood Dr, San Diego, CA 92101",
  "1500 Riverside Ln, Sacramento, CA 95814",
  "1600 Parkview Dr, Albuquerque, NM 87102",
];

type ValueSource =
  | "rentcast_avm"
  | "last_sale"
  | "sales_history"
  | "latest_transaction"
  | "nearby_comps"
  | "zillow_area"
  | "redfin_area"
  | "census_median"
  | "none";

interface AuditRow {
  address: string;
  normalized?: string;
  geocoded?: string;
  rentcastMatch: boolean;
  avmFound: boolean;
  lastSaleFound: boolean;
  salesHistoryFound: boolean;
  nearbyCompsFound: boolean;
  valueSource: ValueSource;
  finalEstimate: number | null;
  propertyLevel: boolean;
  confidence: string;
  plausible: string;
}

async function fetchInsights(address: string): Promise<Record<string, unknown>> {
  const params = new URLSearchParams({ address, countryCode: "US" });
  const res = await fetch(`${BASE}/api/property-value?${params}`, {
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return (await res.json()) as Record<string, unknown>;
}

function auditResponse(address: string, data: Record<string, unknown>): AuditRow {
  const addr = data.address as { city?: string; street?: string; house_number?: string } | undefined;
  const normalized = addr ? [addr.house_number, addr.street, addr.city].filter(Boolean).join(", ") : undefined;

  const pr = data.property_result as {
    exact_value?: number | null;
    value_level?: string;
    value_range?: { low_estimate?: number; estimated_value?: number; high_estimate?: number };
  } | undefined;
  const valueSource = (data.value_source as ValueSource) ?? "none";
  const valueLevel = pr?.value_level ?? (data.is_area_level_estimate ? "area-level" : "unknown");
  const propertyLevel = valueLevel === "property-level" || valueLevel === "street-level";

  const avm = typeof data.avm_value === "number" && data.avm_value > 0 ? data.avm_value : null;
  const lastSale = data.last_sale as { price?: number } | undefined;
  const salesHistory = data.sales_history as Array<{ price: number }> | undefined;
  const nearbyComps = data.nearby_comps as { avg_price?: number } | undefined;

  const rentcastMatch = !!(
    (avm != null && avm > 0) ||
    (lastSale?.price != null && lastSale.price > 0) ||
    (Array.isArray(salesHistory) && salesHistory.length > 0) ||
    (nearbyComps?.avg_price != null && nearbyComps.avg_price > 0)
  );
  const avmFound = avm != null && avm > 0;
  const lastSaleFound = lastSale?.price != null && lastSale.price > 0;
  const salesHistoryFound = Array.isArray(salesHistory) && salesHistory.length > 0;
  const nearbyCompsFound = nearbyComps?.avg_price != null && nearbyComps.avg_price > 0;

  const valueRange = data.value_range as { low_estimate?: number; estimated_value?: number; high_estimate?: number } | undefined;
  const finalEstimate = pr?.exact_value ?? valueRange?.estimated_value ?? avm ?? null;
  const rangeStr = valueRange?.low_estimate != null && valueRange?.high_estimate != null
    ? `$${(valueRange.low_estimate / 1000).toFixed(0)}k–$${(valueRange.high_estimate! / 1000).toFixed(0)}k`
    : finalEstimate != null ? `$${(finalEstimate / 1000).toFixed(0)}k` : "—";
  const confidence = (data.us_match_confidence as string) ?? "unknown";

  let plausible = "—";
  if (finalEstimate != null && finalEstimate > 0) {
    if (valueSource === "rentcast_avm" || valueSource === "last_sale" || valueSource === "sales_history" || valueSource === "latest_transaction") {
      plausible = "Property-level; likely accurate";
    } else if (valueSource === "nearby_comps") {
      plausible = "Comps-based; reasonable";
    } else {
      plausible = "Area-level; may be inaccurate for this property";
    }
  }

  return {
    address,
    normalized,
    rentcastMatch,
    avmFound,
    lastSaleFound,
    salesHistoryFound,
    nearbyCompsFound,
    valueSource,
    finalEstimate,
    propertyLevel,
    confidence,
    plausible: `${rangeStr} · ${plausible}`,
  };
}

async function main() {
  console.log("=== US Valuation Trust Validation ===\n");
  console.log("API base:", BASE);
  console.log("");

  const results: AuditRow[] = [];

  for (const addr of TEST_ADDRESSES) {
    try {
      const data = await fetchInsights(addr);
      results.push(auditResponse(addr, data));
    } catch (e) {
      results.push({
        address: addr,
        rentcastMatch: false,
        avmFound: false,
        lastSaleFound: false,
        salesHistoryFound: false,
        nearbyCompsFound: false,
        valueSource: "none",
        finalEstimate: null,
        propertyLevel: false,
        confidence: "error",
        plausible: String(e),
      });
    }
  }

  console.log("--- Results ---\n");
  for (const r of results) {
    console.log(`Address: ${r.address}`);
    if (r.normalized) console.log(`  Normalized: ${r.normalized}`);
    console.log(`  RentCast match: ${r.rentcastMatch} | AVM: ${r.avmFound} | Last sale: ${r.lastSaleFound} | Sales history: ${r.salesHistoryFound} | Nearby comps: ${r.nearbyCompsFound}`);
    console.log(`  Final value: ${r.finalEstimate != null ? `$${r.finalEstimate.toLocaleString()}` : "—"}`);
    console.log(`  Source: ${r.valueSource} | Property-level: ${r.propertyLevel} | Confidence: ${r.confidence}`);
    console.log(`  Plausible: ${r.plausible}`);
    console.log("");
  }

  const propertyLevelCount = results.filter((r) => r.propertyLevel).length;
  const areaLevelCount = results.filter((r) => !r.propertyLevel && r.finalEstimate != null).length;
  console.log("--- Summary ---");
  console.log(`Property-level estimates: ${propertyLevelCount}`);
  console.log(`Area-level estimates: ${areaLevelCount}`);
  console.log(`Errors: ${results.filter((r) => r.confidence === "error").length}`);
}

main().catch(console.error);
