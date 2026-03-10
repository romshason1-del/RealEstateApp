/**
 * UK Land Registry Price Paid Data Provider
 * Uses HM Land Registry SPARQL endpoint: https://landregistry.data.gov.uk/
 * Postcode-first search with exact and fuzzy building matching.
 */

import type { PropertyDataProvider } from "./provider-interface";
import type {
  PropertyValueInput,
  PropertyValueInsightsResult,
  PropertyValueInsightsSuccess,
  PropertyValueInsightsNoMatch,
  PropertyValueInsightsError,
  PropertyValueInsightsDebug,
} from "./types";

const SPARQL_ENDPOINT = "http://landregistry.data.gov.uk/landregistry/query";
const FIVE_YEARS_MS = 5 * 365.25 * 24 * 60 * 60 * 1000;

/** UK street abbreviations to full form for normalization */
const STREET_ABBREVS: [RegExp, string][] = [
  [/\bst\b/gi, "street"],
  [/\bstr\b/gi, "street"],
  [/\brd\b/gi, "road"],
  [/\bave\b/gi, "avenue"],
  [/\bav\b/gi, "avenue"],
  [/\bln\b/gi, "lane"],
  [/\bdr\b/gi, "drive"],
  [/\bpl\b/gi, "place"],
  [/\bplz\b/gi, "place"],
  [/\bplza\b/gi, "plaza"],
  [/\bct\b/gi, "court"],
  [/\bcr\b/gi, "crescent"],
  [/\bcl\b/gi, "close"],
  [/\bter\b/gi, "terrace"],
  [/\bterr\b/gi, "terrace"],
  [/\bgr\b/gi, "grove"],
  [/\bgrn\b/gi, "green"],
  [/\bway\b/gi, "way"],
  [/\bwalk\b/gi, "walk"],
  [/\bmt\b/gi, "mount"],
  [/\bmnt\b/gi, "mount"],
  [/\bsq\b/gi, "square"],
  [/\bblvd\b/gi, "boulevard"],
  [/\bhwy\b/gi, "highway"],
  [/\bn\b/gi, "north"],
  [/\bs\b/gi, "south"],
  [/\be\b/gi, "east"],
  [/\bw\b/gi, "west"],
];

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

type AddressMatchMode = "exact" | "fuzzy" | "postcode_only" | "none";

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

/** Normalize for matching: lowercase, expand abbreviations, remove punctuation, dedupe words */
function normalizeStreet(s: string): string {
  let t = (s ?? "").toLowerCase().replace(/\s+/g, " ").trim();
  for (const [re, full] of STREET_ABBREVS) {
    t = t.replace(re, full);
  }
  t = t.replace(/[^\w\s]/g, " ").replace(/\s+/g, " ").trim();
  const words = t.split(/\s+/).filter(Boolean);
  const seen = new Set<string>();
  const deduped = words.filter((w) => {
    if (seen.has(w)) return false;
    seen.add(w);
    return true;
  });
  return deduped.join(" ");
}

function normalizeForMatch(s: string): string {
  return (s ?? "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s]/g, "")
    .trim();
}

/** Extract numeric part from PAON for comparison (e.g. "25A" -> "25", "34" -> "34") */
function extractNumberPart(s: string): string {
  const m = (s ?? "").match(/\d+/);
  return m ? m[0] : "";
}

/** Check if PAON matches house number (supports "25", "25A", "34", "FLAT 3" etc.) */
function paonMatchesHouseNumber(paon: string, saon: string, houseNumber: string): boolean {
  const hn = normalizeForMatch(houseNumber);
  const paonNorm = normalizeForMatch(paon);
  const saonNorm = normalizeForMatch(saon);
  if (!hn) return true;
  if (paonNorm === hn || paonNorm.startsWith(hn) || hn.startsWith(paonNorm)) return true;
  if (paonNorm.includes(hn) || hn.includes(paonNorm)) return true;
  const paonNum = extractNumberPart(paon);
  const hnNum = extractNumberPart(houseNumber);
  if (paonNum && hnNum && paonNum === hnNum) return true;
  if (saonNorm && (saonNorm.includes(hn) || hn.includes(saonNorm))) return true;
  return false;
}

/** Check if street strings match (partial, normalized) */
function streetMatches(reqStreet: string, addrStreet: string, exact: boolean): boolean {
  const req = normalizeStreet(reqStreet);
  const addr = normalizeStreet(addrStreet);
  if (!req) return true;
  if (req === addr) return true;
  if (addr.includes(req) || req.includes(addr)) return true;
  const reqWords = req.split(/\s+/);
  const addrWords = addr.split(/\s+/);
  const matchCount = reqWords.filter((w) => addr.includes(w)).length;
  if (exact) {
    return matchCount >= Math.min(reqWords.length, 2) && addrWords.length >= 1;
  }
  return matchCount >= 1 || addrWords.some((w) => req.includes(w));
}

/** Exact building match: strict paon + street + town */
function matchesBuildingExact(
  paon: string,
  saon: string,
  addrStreet: string,
  addrTown: string,
  houseNumber: string,
  street: string,
  city: string
): boolean {
  const townNorm = normalizeForMatch(addrTown);
  const reqCityNorm = normalizeForMatch(city);
  const townOk = !reqCityNorm || townNorm.includes(reqCityNorm) || reqCityNorm.includes(townNorm);
  if (!townOk) return false;
  if (!paonMatchesHouseNumber(paon, saon, houseNumber)) return false;
  return streetMatches(street, addrStreet, true);
}

/** Fuzzy building match: tolerant paon + street + town */
function matchesBuildingFuzzy(
  paon: string,
  saon: string,
  addrStreet: string,
  addrTown: string,
  houseNumber: string,
  street: string,
  city: string
): boolean {
  const townNorm = normalizeForMatch(addrTown);
  const reqCityNorm = normalizeForMatch(city);
  const townOk = !reqCityNorm || townNorm.includes(reqCityNorm) || reqCityNorm.includes(townNorm);
  if (!townOk) return false;
  if (!paonMatchesHouseNumber(paon, saon, houseNumber)) return false;
  return streetMatches(street, addrStreet, false);
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
        debug: { postcode, records_fetched: 0, postcode_results_count: 0 },
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
      saon: string;
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
      const saon = getBinding(b, "saon");
      const addrStreet = getBinding(b, "street");
      const addrTown = getBinding(b, "town");

      if (amount <= 0 || !isValidTransaction(category)) continue;

      const dedupeKey = `${paon}|${saon}|${addrStreet}|${dateStr}|${amount}`;
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
        saon,
        addrStreet: addrStreet.toLowerCase(),
        addrTown: addrTown.toLowerCase(),
      });
    }

    const postcodeResultsCount = items.length;
    if (postcodeResultsCount === 0) {
      return {
        message: "no transaction found",
        debug: { postcode, records_fetched: bindings.length, postcode_results_count: 0 },
      } as PropertyValueInsightsNoMatch;
    }

    const cityLower = city.toLowerCase();

    const exactMatches = items.filter((t) =>
      matchesBuildingExact(t.paon, t.saon, t.addrStreet, t.addrTown, houseNumber, street, cityLower)
    );
    const fuzzyMatches = items.filter(
      (t) =>
        !matchesBuildingExact(t.paon, t.saon, t.addrStreet, t.addrTown, houseNumber, street, cityLower) &&
        matchesBuildingFuzzy(t.paon, t.saon, t.addrStreet, t.addrTown, houseNumber, street, cityLower)
    );

    const buildingTxs = exactMatches.length > 0 ? exactMatches : fuzzyMatches;
    const building5y = buildingTxs.filter((t) => t.dateMs >= fiveYearsAgo);
    const postcode5y = items.filter((t) => t.dateMs >= fiveYearsAgo);

    const sortedBuildingAll = [...buildingTxs].sort((a, b) => b.dateMs - a.dateMs);
    const latestBuilding = sortedBuildingAll[0] ?? null;

    let buildingAveragePrice: number | null = null;
    if (building5y.length >= 2) {
      const sum = building5y.reduce((s, t) => s + t.amount, 0);
      buildingAveragePrice = Math.round(sum / building5y.length);
    }

    let averageAreaPrice: number | null = null;
    if (postcode5y.length > 0) {
      const sum = postcode5y.reduce((s, t) => s + t.amount, 0);
      averageAreaPrice = Math.round(sum / postcode5y.length);
    }

    let addressMatchMode: AddressMatchMode = "none";
    if (exactMatches.length > 0) addressMatchMode = "exact";
    else if (fuzzyMatches.length > 0) addressMatchMode = "fuzzy";
    else if (postcodeResultsCount > 0) addressMatchMode = "postcode_only";

    const ukDebug: PropertyValueInsightsDebug = {
      postcode_results_count: postcodeResultsCount,
      exact_building_matches_count: exactMatches.length,
      fuzzy_building_matches_count: fuzzyMatches.length,
      address_match_mode: addressMatchMode,
    };

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
      match_quality: buildingTxs.length > 0 ? "exact_building" : "no_reliable_match",
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
      debug: ukDebug,
    };

    return success;
  }
}
