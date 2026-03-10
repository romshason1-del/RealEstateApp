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

const NOMINATIM_URL = "https://nominatim.openstreetmap.org/search";
const GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json";

/** UK postcode pattern (outward + inward, e.g. SW1A 2AA, W11 3QY) */
const UK_POSTCODE_REGEX = /[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}/i;

/**
 * Resolve postcode from address using geocoding (Nominatim or Google).
 * Returns normalized UK postcode or null if not found.
 */
async function resolvePostcodeFromAddress(address: string): Promise<string | null> {
  const q = address.trim();
  if (!q || q.length < 5) return null;

  // Try Google first if API key available
  const googleKey = typeof process !== "undefined" ? process.env?.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY : undefined;
  if (googleKey) {
    try {
      const url = `${GOOGLE_GEOCODE_URL}?address=${encodeURIComponent(q)}&region=gb&key=${googleKey}`;
      const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
      if (res.ok) {
        const data = (await res.json()) as { results?: Array<{ address_components?: Array<{ long_name: string; types: string[] }> }> };
        const comps = data?.results?.[0]?.address_components ?? [];
        const postal = comps.find((c) => c.types.includes("postal_code"));
        const pc = postal?.long_name?.trim();
        if (pc && UK_POSTCODE_REGEX.test(pc.replace(/\s/g, ""))) return normalizeUKPostcode(pc);
      }
    } catch {
      // Fall through to Nominatim
    }
  }

  // Nominatim (free, no API key)
  try {
    const url = `${NOMINATIM_URL}?q=${encodeURIComponent(q)}&format=json&addressdetails=1&limit=1`;
    const res = await fetch(url, {
      headers: { "User-Agent": "StreetIQ-PropertyValue/1.0 (UK Land Registry lookup)" },
      signal: AbortSignal.timeout(8000),
    });
    if (res.ok) {
      const data = (await res.json()) as Array<{ address?: { postcode?: string } }>;
      const pc = data?.[0]?.address?.postcode?.trim();
      if (pc && UK_POSTCODE_REGEX.test(pc.replace(/\s/g, ""))) return normalizeUKPostcode(pc);
    }
  } catch {
    // Geocoding failed
  }
  return null;
}

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

/** Filter outliers using IQR (interquartile range). Returns values within Q1 - 1.5*IQR and Q3 + 1.5*IQR. */
function filterOutliersIQR(sortedAmounts: number[]): number[] {
  if (sortedAmounts.length < 4) return sortedAmounts;
  const q1Idx = Math.floor(sortedAmounts.length * 0.25);
  const q3Idx = Math.floor(sortedAmounts.length * 0.75);
  const q1 = sortedAmounts[q1Idx] ?? 0;
  const q3 = sortedAmounts[q3Idx] ?? 0;
  const iqr = q3 - q1;
  const lower = q1 - 1.5 * iqr;
  const upper = q3 + 1.5 * iqr;
  return sortedAmounts.filter((a) => a >= lower && a <= upper);
}

/**
 * Normalize UK postcode for Land Registry query.
 * Format: outward (2-4 chars) + space + inward (3 chars). E.g. BA1 1BN, SL7 1AW.
 */
function normalizeUKPostcode(postcode: string): string {
  let s = (postcode ?? "").trim().replace(/\s+/g, " ").toUpperCase();
  s = s.replace(/[^\w\s]/g, "").replace(/\s+/g, "");
  if (s.length >= 5) {
    s = s.slice(0, -3) + " " + s.slice(-3);
  }
  return s.trim();
}

/** Build SPARQL query using exact postcode match (VALUES). Tries both spaced and compact formats for Land Registry compatibility. */
function buildSparqlQueryExact(postcode: string): string {
  const normalized = normalizeUKPostcode(postcode);
  const compact = normalized.replace(/\s/g, "");
  const valuesClause =
    normalized.includes(" ") && compact !== normalized
      ? `VALUES ?postcode {"${normalized}"^^xsd:string "${compact}"^^xsd:string}`
      : `VALUES ?postcode {"${normalized}"^^xsd:string}`;
  return `
PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>
PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?paon ?saon ?street ?town ?county ?postcode ?amount ?date ?category
WHERE {
  ${valuesClause}
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

/** Fallback: use outward code (e.g. BA1) if exact postcode returns no results */
function buildSparqlQueryOutward(postcode: string): string {
  const normalized = normalizeUKPostcode(postcode);
  const outward = normalized.split(/\s/)[0] || normalized.replace(/\s/g, "").slice(0, -3) || normalized;
  return buildSparqlQueryByPostcodePrefix(outward);
}

/** Fallback: use postcode area (e.g. BA) - first 1-2 letters */
function buildSparqlQueryArea(postcode: string): string {
  const normalized = normalizeUKPostcode(postcode);
  const outward = normalized.split(/\s/)[0] || normalized.replace(/\s/g, "").slice(0, -3) || normalized;
  const area = outward.match(/^[A-Z]{1,2}/i)?.[0]?.toUpperCase() || outward.slice(0, 2);
  return buildSparqlQueryByPostcodePrefix(area);
}

function buildSparqlQueryByPostcodePrefix(prefix: string): string {
  return `
PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>
PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?paon ?saon ?street ?town ?county ?postcode ?amount ?date ?category
WHERE {
  ?addr lrcommon:postcode ?postcode.
  FILTER(STRSTARTS(STR(?postcode), "${prefix}"))
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

/** Fallback: query by street and town when postcode returns nothing */
function buildSparqlQueryStreetTown(street: string, town: string): string {
  const streetNorm = (street ?? "").trim().replace(/[^\w\s]/g, "").replace(/\s+/g, " ").toUpperCase();
  const townNorm = (town ?? "").trim().replace(/[^\w\s]/g, "").replace(/\s+/g, " ").toUpperCase();
  if (!streetNorm || !townNorm) return "";
  const streetPart = streetNorm.split(/\s+/)[0] || streetNorm;
  return `
PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>
PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?paon ?saon ?street ?town ?county ?postcode ?amount ?date ?category
WHERE {
  ?addr lrcommon:street ?street.
  ?addr lrcommon:town ?town.
  FILTER(CONTAINS(LCASE(STR(?street)), "${streetPart.toLowerCase()}") && CONTAINS(LCASE(STR(?town)), "${townNorm.toLowerCase()}"))
  ?transx lrppi:propertyAddress ?addr ;
          lrppi:pricePaid ?amount ;
          lrppi:transactionDate ?date ;
          lrppi:transactionCategory/skos:prefLabel ?category.
  OPTIONAL { ?addr lrcommon:county ?county }
  OPTIONAL { ?addr lrcommon:paon ?paon }
  OPTIONAL { ?addr lrcommon:saon ?saon }
  OPTIONAL { ?addr lrcommon:postcode ?postcode }
}
ORDER BY DESC(?date)
LIMIT 500
`.trim();
}

/** Fallback: query by town/locality only */
function buildSparqlQueryTown(town: string): string {
  const townNorm = (town ?? "").trim().replace(/[^\w\s]/g, "").replace(/\s+/g, " ").toUpperCase();
  if (!townNorm || townNorm.length < 2) return "";
  return `
PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>
PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?paon ?saon ?street ?town ?county ?postcode ?amount ?date ?category
WHERE {
  ?addr lrcommon:town ?town.
  FILTER(CONTAINS(LCASE(STR(?town)), "${townNorm.toLowerCase()}"))
  ?transx lrppi:propertyAddress ?addr ;
          lrppi:pricePaid ?amount ;
          lrppi:transactionDate ?date ;
          lrppi:transactionCategory/skos:prefLabel ?category.
  OPTIONAL { ?addr lrcommon:county ?county }
  OPTIONAL { ?addr lrcommon:paon ?paon }
  OPTIONAL { ?addr lrcommon:saon ?saon }
  OPTIONAL { ?addr lrcommon:street ?street }
  OPTIONAL { ?addr lrcommon:postcode ?postcode }
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
    let postcode = (input.postcode ?? input.zip ?? "").trim().replace(/\s+/g, " ");
    const street = (input.street ?? "").trim();
    const city = (input.city ?? "").trim();
    const houseNumber = (input.houseNumber ?? "").trim();
    const fullAddress = (input.fullAddress ?? "").trim();

    const hasStreetAndCity = !!(street && city);
    const hasGeocodeableAddress = !!(fullAddress || (street && city) || city);
    if (!postcode && !hasStreetAndCity && !hasGeocodeableAddress) {
      return {
        message: "UK Land Registry requires a postcode or street and town to look up transactions.",
        error: "INVALID_INPUT",
      } as PropertyValueInsightsError;
    }

    // Resolve postcode via geocoding when missing
    if (!postcode && hasGeocodeableAddress) {
      const geocodeQuery = fullAddress || [houseNumber, street, city].filter(Boolean).join(", ");
      const resolved = await resolvePostcodeFromAddress(geocodeQuery);
      if (resolved) postcode = resolved;
    }

    const hasPostcode = !!postcode.trim();
    const normalizedPostcode = hasPostcode ? normalizeUKPostcode(postcode) : "";
    let query: string;
    let queryMode: string;
    if (hasPostcode) {
      query = buildSparqlQueryExact(normalizedPostcode);
      queryMode = "exact";
    } else if (street && city) {
      query = buildSparqlQueryStreetTown(street, city);
      queryMode = "street";
    } else if (city) {
      query = buildSparqlQueryTown(city);
      queryMode = "locality";
    } else {
      return {
        message: "UK Land Registry requires a postcode or street and town to look up transactions.",
        error: "INVALID_INPUT",
      } as PropertyValueInsightsError;
    }

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
        debug: { request_error: msg, normalized_postcode: normalizedPostcode },
      } as PropertyValueInsightsError;
    }

    if (!res.ok) {
      return {
        message: `UK Land Registry returned ${res.status}`,
        error: "DATA_SOURCE_UNAVAILABLE",
        debug: { http_status: res.status, normalized_postcode: normalizedPostcode },
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

    let bindings = json?.results?.bindings ?? [];
    type FallbackStep = { query: string; mode: string };
    // Prioritize postcode-level fallbacks (outward, area) before street/locality for geographically relevant area data
    const fallbacks: FallbackStep[] = [
      ...(hasPostcode && queryMode !== "outward_postcode" ? [{ query: buildSparqlQueryOutward(normalizedPostcode), mode: "outward_postcode" }] : []),
      ...(hasPostcode && queryMode !== "postcode_area" ? [{ query: buildSparqlQueryArea(normalizedPostcode), mode: "postcode_area" }] : []),
      ...(street && city && queryMode !== "street" ? [{ query: buildSparqlQueryStreetTown(street, city), mode: "street" }] : []),
      ...(city && queryMode !== "locality" ? [{ query: buildSparqlQueryTown(city), mode: "locality" }] : []),
    ].filter((s): s is FallbackStep => typeof (s as FallbackStep).query === "string" && (s as FallbackStep).query.length > 0);

    for (const step of fallbacks) {
      if (bindings.length > 0) break;
      query = step.query;
      queryMode = step.mode;
      try {
        const resFb = await fetch(SPARQL_ENDPOINT, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            Accept: "application/sparql-results+json",
          },
          body: new URLSearchParams({ query }),
          signal: AbortSignal.timeout(15000),
        });
        if (resFb.ok) {
          const jsonFb = await resFb.json();
          bindings = jsonFb?.results?.bindings ?? [];
        }
      } catch {
        // Fallback failed, try next
      }
    }
    const postcodeQueryRawResultCount = bindings.length;

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

    function processBindingsToItems(raw: Record<string, SparqlBinding>[], strict = true): Tx[] {
      const seen = new Set<string>();
      const out: Tx[] = [];
      for (const b of raw) {
        const dateStr = getBinding(b, "date");
        const date = parseDate(dateStr);
        const amount = parseAmount(getBinding(b, "amount"));
        const category = getBinding(b, "category");
        const paon = getBinding(b, "paon");
        const saon = getBinding(b, "saon");
        const addrStreet = getBinding(b, "street");
        const addrTown = getBinding(b, "town");
        if (amount <= 0) continue;
        if (strict && !isValidTransaction(category)) continue;
        const dedupeKey = `${paon}|${saon}|${addrStreet}|${dateStr}|${amount}`;
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);
        const dateMs = date ? new Date(date).getTime() : 0;
        out.push({
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
      return out;
    }

    let items = processBindingsToItems(bindings, true);
    for (const step of fallbacks) {
      if (items.length > 0) break;
      query = step.query;
      queryMode = step.mode;
      try {
        const resFb = await fetch(SPARQL_ENDPOINT, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            Accept: "application/sparql-results+json",
          },
          body: new URLSearchParams({ query }),
          signal: AbortSignal.timeout(15000),
        });
        if (resFb.ok) {
          const jsonFb = await resFb.json();
          bindings = jsonFb?.results?.bindings ?? [];
          items = processBindingsToItems(bindings, true);
        }
      } catch {
        // Fallback failed, try next
      }
    }

    if (bindings.length === 0 && items.length === 0) {
      return {
        message: "no transaction found",
        debug: {
          postcode,
          normalized_postcode: normalizedPostcode,
          postcode_query_executed: queryMode,
          postcode_query_url: SPARQL_ENDPOINT,
          postcode_query_raw_result_count: postcodeQueryRawResultCount,
          records_fetched: 0,
          postcode_results_count: 0,
        },
      } as PropertyValueInsightsNoMatch;
    }

    // When raw results exist but all filtered out, use relaxed filtering for area-level metrics
    if (items.length === 0 && bindings.length > 0) {
      items = processBindingsToItems(bindings, false);
    }

    if (items.length === 0) {
      return {
        message: "no transaction found",
        debug: {
          postcode,
          normalized_postcode: normalizedPostcode,
          postcode_query_executed: queryMode,
          postcode_query_url: SPARQL_ENDPOINT,
          postcode_query_raw_result_count: bindings.length,
          records_fetched: bindings.length,
          postcode_results_count: 0,
        },
      } as PropertyValueInsightsNoMatch;
    }

    const now = Date.now();
    const fiveYearsAgo = now - FIVE_YEARS_MS;
    const postcodeResultsCount = items.length;
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

    const sortedAreaByDate = [...items].sort((a, b) => b.dateMs - a.dateMs);
    const latestFromArea = sortedAreaByDate[0] ?? null;
    const hasBuildingMatch = buildingTxs.length > 0;

    let buildingAveragePrice: number | null = null;
    if (building5y.length >= 2) {
      const sum = building5y.reduce((s, t) => s + t.amount, 0);
      buildingAveragePrice = Math.round(sum / building5y.length);
    }

    let averageAreaPrice: number | null = null;
    if (postcode5y.length > 0) {
      const amounts = postcode5y.map((t) => t.amount).sort((a, b) => a - b);
      const filtered = filterOutliersIQR(amounts);
      if (filtered.length > 0) {
        const sum = filtered.reduce((s, a) => s + a, 0);
        averageAreaPrice = Math.round(sum / filtered.length);
      } else {
        const sum = amounts.reduce((s, a) => s + a, 0);
        averageAreaPrice = Math.round(sum / amounts.length);
      }
    }

    let addressMatchMode: AddressMatchMode = "none";
    if (exactMatches.length > 0) addressMatchMode = "exact";
    else if (fuzzyMatches.length > 0) addressMatchMode = "fuzzy";
    else if (postcodeResultsCount > 0) addressMatchMode = "postcode_only";

    const areaFallbackLevel =
      queryMode === "exact"
        ? ("postcode" as const)
        : queryMode === "outward_postcode"
          ? ("outward_postcode" as const)
          : queryMode === "postcode_area"
            ? ("postcode_area" as const)
            : queryMode === "street"
              ? ("street" as const)
              : queryMode === "locality"
                ? ("locality" as const)
                : ("none" as const);

    const fallbackLevelUsed: "building" | "postcode" | "locality" | "area" = hasBuildingMatch
      ? "building"
      : queryMode === "exact" || queryMode === "outward_postcode"
        ? "postcode"
        : queryMode === "street" || queryMode === "locality"
          ? "locality"
          : "area";

    const matchConfidence: "high" | "medium" | "low" =
      hasBuildingMatch
        ? "high"
        : queryMode === "exact" || queryMode === "outward_postcode" || queryMode === "street" || queryMode === "locality"
          ? "medium"
          : "low";

    const ukDebug: PropertyValueInsightsDebug = {
      normalized_postcode: normalizedPostcode,
      postcode_query_executed: queryMode,
      postcode_query_url: SPARQL_ENDPOINT,
      postcode_query_raw_result_count: postcodeQueryRawResultCount,
      postcode_results_count: postcodeResultsCount,
      exact_building_matches_count: exactMatches.length,
      fuzzy_building_matches_count: fuzzyMatches.length,
      address_match_mode: addressMatchMode,
      fallback_level_used: fallbackLevelUsed,
      postcode_query_snippet: query.slice(0, 300) + (query.length > 300 ? "..." : ""),
    };
    if (typeof process !== "undefined" && process.env?.NODE_ENV === "development") {
      console.debug("[UK Land Registry] postcode:", normalizedPostcode, "query_mode:", queryMode, "raw_count:", postcodeQueryRawResultCount);
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
      latest_nearby_transaction:
        !hasBuildingMatch && latestFromArea
          ? {
              price: latestFromArea.amount,
              date: latestFromArea.date || latestFromArea.dateStr,
              property_type: latestFromArea.category || undefined,
            }
          : null,
      has_building_match: hasBuildingMatch,
      average_area_price: averageAreaPrice,
      area_transaction_count: postcode5y.length,
      area_fallback_level: areaFallbackLevel,
      fallback_level_used: fallbackLevelUsed,
      match_confidence: matchConfidence,
    };

    const effectiveLatest = hasBuildingMatch ? latestBuilding : (latestFromArea ?? latestBuilding);
    const success: PropertyValueInsightsSuccess = {
      address: {
        city: city || postcode,
        street: street || postcode,
        house_number: houseNumber,
      },
      match_quality: buildingTxs.length > 0 ? "exact_building" : "no_reliable_match",
      latest_transaction: effectiveLatest
        ? {
            transaction_date: effectiveLatest.date || effectiveLatest.dateStr,
            transaction_price: effectiveLatest.amount,
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
        latest_building_transaction_price: effectiveLatest?.amount ?? 0,
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
