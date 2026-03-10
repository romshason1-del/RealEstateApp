/**
 * UK Land Registry Price Paid Data Provider
 * Uses HM Land Registry SPARQL endpoint: https://landregistry.data.gov.uk/
 * Building-level filtering with area fallback.
 */

import type { PropertyDataProvider } from "./provider-interface";
import type {
  PropertyValueInput,
  PropertyValueInsightsResult,
  PropertyValueInsightsSuccess,
  PropertyValueInsightsNoMatch,
  PropertyValueInsightsError,
} from "./types";

const SPARQL_ENDPOINT = "http://landregistry.data.gov.uk/landregistry/query";
const FIVE_YEARS_MS = 5 * 365.25 * 24 * 60 * 60 * 1000;

/** Exclude: Additional (transfers, buy-to-let, repossessions), lease extensions, non-standard */
const INVALID_CATEGORY_PATTERNS = [
  /additional/i,
  /transfer/i,
  /lease\s*extension/i,
  /repossession/i,
  /power\s*of\s*sale/i,
];

type SparqlBinding = { value?: string };
type SparqlResult = { results?: { bindings?: Record<string, SparqlBinding>[] } };

function parseAmount(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val)) return val;
  const s = String(val ?? "").replace(/[^\d.]/g, "");
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : 0;
}

function parseDate(val: unknown): string {
  if (val == null || val === "") return "";
  const d = new Date(String(val));
  return Number.isFinite(d.getTime()) ? d.toISOString().slice(0, 10) : "";
}

function isValidTransaction(category: string): boolean {
  const cat = (category ?? "").trim();
  if (!cat) return true;
  return !INVALID_CATEGORY_PATTERNS.some((p) => p.test(cat));
}

function buildSparqlQuery(postcode: string): string {
  const normalized = postcode.trim().replace(/\s+/g, " ").toUpperCase();
  return `
PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>
PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?paon ?saon ?street ?town ?county ?postcode ?amount ?date ?category
WHERE {
  VALUES ?postcode {"${normalized}"^^xsd:string}
  ?addr lrcommon:postcode ?postcode.
  ?transx lrppi:propertyAddress ?addr ;
          lrppi:pricePaid ?amount ;
          lrppi:transactionDate ?date ;
          lrppi:transactionCategory/skos:prefLabel ?category.
  OPTIONAL { ?addr lrcommon:county ?county }
  OPTIONAL { ?addr lrcommon:paon ?paon }
  OPTIONAL { ?addr lrcommon:saon ?saon }
  OPTIONAL { ?addr lrcommon:street ?street }
  OPTIONAL { ?addr lrcommon:town ?town }
}
ORDER BY DESC(?date)
LIMIT 500
`.trim();
}

function getBinding(binding: Record<string, SparqlBinding>, key: string): string {
  const b = binding[key];
  return b?.value ?? "";
}

function normalizeForMatch(s: string): string {
  return (s ?? "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s]/g, "")
    .trim();
}

/** Check if a Land Registry record matches the requested building (paon + street [+ town]). */
function matchesBuilding(
  paon: string,
  addrStreet: string,
  addrTown: string,
  houseNumber: string,
  street: string,
  city: string
): boolean {
  const paonNorm = normalizeForMatch(paon);
  const streetNorm = normalizeForMatch(addrStreet);
  const townNorm = normalizeForMatch(addrTown);
  const hnNorm = normalizeForMatch(houseNumber);
  const reqStreetNorm = normalizeForMatch(street);
  const reqCityNorm = normalizeForMatch(city);

  const paonMatches =
    !hnNorm ||
    paonNorm === hnNorm ||
    paonNorm.startsWith(hnNorm) ||
    hnNorm.startsWith(paonNorm) ||
    (paonNorm && paonNorm.includes(hnNorm));

  const streetMatches =
    !reqStreetNorm ||
    streetNorm.includes(reqStreetNorm) ||
    reqStreetNorm.includes(streetNorm) ||
    streetNorm === reqStreetNorm;

  const townMatches = !reqCityNorm || townNorm.includes(reqCityNorm) || reqCityNorm.includes(townNorm);

  return Boolean(paonMatches && streetMatches && townMatches);
}

export class UKLandRegistryProvider implements PropertyDataProvider {
  readonly id = "uk-land-registry";
  readonly name = "UK Land Registry (Price Paid Data)";

  async getInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult> {
    const postcode = (input.postcode ?? input.zip ?? "").trim().replace(/\s+/g, " ");
    const street = (input.street ?? "").trim();
    const city = (input.city ?? "").trim();
    const houseNumber = (input.houseNumber ?? "").trim();

    if (!postcode) {
      return {
        message: "UK Land Registry requires a postcode to look up transactions.",
        error: "INVALID_INPUT",
      } as PropertyValueInsightsError;
    }

    const query = buildSparqlQuery(postcode);

    let res: Response;
    try {
      res = await fetch(SPARQL_ENDPOINT, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Accept: "application/sparql-results+json",
        },
        body: new URLSearchParams({ query }),
        signal: AbortSignal.timeout(15000),
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown error";
      return {
        message: `Failed to fetch UK Land Registry data: ${msg}`,
        error: "DATA_SOURCE_UNAVAILABLE",
        debug: { request_error: msg },
      } as PropertyValueInsightsError;
    }

    if (!res.ok) {
      return {
        message: `UK Land Registry returned ${res.status}`,
        error: "DATA_SOURCE_UNAVAILABLE",
        debug: { http_status: res.status },
      } as PropertyValueInsightsError;
    }

    let json: SparqlResult;
    try {
      json = await res.json();
    } catch {
      return {
        message: "Invalid response from UK Land Registry",
        error: "DATA_SOURCE_UNAVAILABLE",
      } as PropertyValueInsightsError;
    }

    const bindings = json?.results?.bindings ?? [];
    if (bindings.length === 0) {
      return {
        message: "no transaction found",
        debug: { postcode, records_fetched: 0 },
      } as PropertyValueInsightsNoMatch;
    }

    const now = Date.now();
    const fiveYearsAgo = now - FIVE_YEARS_MS;

    type Tx = {
      date: string;
      dateStr: string;
      amount: number;
      category: string;
      dateMs: number;
      paon: string;
      addrStreet: string;
      addrTown: string;
    };

    const seen = new Set<string>();
    const items: Tx[] = [];

    for (const b of bindings) {
      const dateStr = getBinding(b, "date");
      const date = parseDate(dateStr);
      const amount = parseAmount(getBinding(b, "amount"));
      const category = getBinding(b, "category");
      const paon = getBinding(b, "paon");
      const addrStreet = getBinding(b, "street");
      const addrTown = getBinding(b, "town");

      if (amount <= 0 || !isValidTransaction(category)) continue;

      const dedupeKey = `${paon}|${addrStreet}|${dateStr}|${amount}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);

      const dateMs = date ? new Date(date).getTime() : 0;
      items.push({
        date,
        dateStr,
        amount,
        category,
        dateMs,
        paon,
        addrStreet: addrStreet.toLowerCase(),
        addrTown: addrTown.toLowerCase(),
      });
    }

    if (items.length === 0) {
      return {
        message: "no transaction found",
        debug: { postcode, records_fetched: bindings.length },
      } as PropertyValueInsightsNoMatch;
    }

    const cityLower = city.toLowerCase();
    const buildingTxs = items.filter((t) =>
      matchesBuilding(t.paon, t.addrStreet, t.addrTown, houseNumber, street, cityLower)
    );
    const building5y = buildingTxs.filter((t) => t.dateMs >= fiveYearsAgo);
    const postcode5y = items.filter((t) => t.dateMs >= fiveYearsAgo);

    const sortedBuilding = [...building5y].sort((a, b) => b.dateMs - a.dateMs);
    const latestBuilding = sortedBuilding[0] ?? null;

    let buildingAveragePrice: number | null = null;
    if (building5y.length > 0) {
      const sum = building5y.reduce((s, t) => s + t.amount, 0);
      buildingAveragePrice = Math.round(sum / building5y.length);
    }

    let averageAreaPrice: number | null = null;
    if (postcode5y.length > 0) {
      const sum = postcode5y.reduce((s, t) => s + t.amount, 0);
      averageAreaPrice = Math.round(sum / postcode5y.length);
    }

    const ukData = {
      building_average_price: buildingAveragePrice,
      transactions_in_building: building5y.length,
      latest_building_transaction: latestBuilding
        ? {
            price: latestBuilding.amount,
            date: latestBuilding.date || latestBuilding.dateStr,
            property_type: latestBuilding.category || undefined,
          }
        : null,
      average_area_price: averageAreaPrice,
    };

    const success: PropertyValueInsightsSuccess = {
      address: {
        city: city || postcode,
        street: street || postcode,
        house_number: houseNumber,
      },
      match_quality: building5y.length > 0 ? "exact_building" : "no_reliable_match",
      latest_transaction: latestBuilding
        ? {
            transaction_date: latestBuilding.date || latestBuilding.dateStr,
            transaction_price: latestBuilding.amount,
            property_size: 0,
            price_per_m2: 0,
          }
        : {
            transaction_date: "",
            transaction_price: 0,
            property_size: 0,
            price_per_m2: 0,
          },
      current_estimated_value: null,
      building_summary_last_3_years: {
        transactions_count_last_3_years: building5y.length,
        transactions_count_last_5_years: building5y.length,
        latest_building_transaction_price: latestBuilding?.amount ?? 0,
        average_apartment_value_today: buildingAveragePrice ?? averageAreaPrice ?? 0,
      },
      market_value_source: "exact_provider",
      source: "uk-land-registry",
      uk_land_registry: ukData,
    };

    return success;
  }
}
