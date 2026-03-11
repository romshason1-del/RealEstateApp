#!/usr/bin/env tsx
/**
 * US Valuation Trust Validation
 * Tests 10 residential addresses and reports valuation path.
 * Run with dev server: npm run dev (in another terminal)
 * Then: npx tsx scripts/validate-us-valuation.ts
 */
const BASE = process.env.VALIDATE_API_BASE ?? "http://localhost:3000";

const TEST_ADDRESSES = [
  "568 N Tigertail Rd, Los Angeles, CA",
  "1600 Amphitheatre Parkway, Mountain View, CA",
  "350 5th Ave, New York, NY 10118",
  "123 Main St, Austin, TX 78701",
  "742 Evergreen Terrace, Springfield, IL",
  "1 Apple Park Way, Cupertino, CA",
  "4059 Mt Lee Dr, Hollywood, CA",
  "100 Universal City Plaza, Universal City, CA",
  "221B Baker St, London, CA 90210",
  "456 Oak Ave, Chicago, IL 60601",
  "10 Downing St, San Francisco, CA 94102",
  "555 W 5th St, Los Angeles, CA 90013",
  "2000 Market St, San Francisco, CA 94114",
  "1001 Pennsylvania Ave NW, Washington, DC 20004",
  "1 World Trade Center, New York, NY 10007",
  "400 Broad St, Seattle, WA 98109",
  "233 S Wacker Dr, Chicago, IL 60606",
  "888 Brannan St, San Francisco, CA 94103",
  "1 Main St, Cambridge, MA 02142",
  "500 S Grand Ave, Los Angeles, CA 90071",
];

type ValueSource =
  | "rentcast_avm"
  | "last_sale"
  | "sales_history"
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
