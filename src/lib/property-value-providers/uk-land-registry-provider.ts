/**
 * UK Land Registry Price Paid Data Provider
 * Uses HM Land Registry SPARQL endpoint: https://landregistry.data.gov.uk/
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

export class UKLandRegistryProvider implements PropertyDataProvider {
  readonly id = "uk-land-registry";
  readonly name = "UK Land Registry (Price Paid Data)";

  async getInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult> {
    const postcode = (input.postcode ?? input.zip ?? "").trim().replace(/\s+/g, " ");
    const street = (input.street ?? "").trim();
    const city = (input.city ?? "").trim();

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

    const items = bindings.map((b) => {
      const dateStr = getBinding(b, "date");
      const date = parseDate(dateStr);
      const amount = parseAmount(getBinding(b, "amount"));
      const category = getBinding(b, "category");
      const dateMs = date ? new Date(date).getTime() : 0;
      const addrStreet = getBinding(b, "street").toLowerCase();
      const addrTown = getBinding(b, "town").toLowerCase();
      return { date, dateStr, amount, category, dateMs, addrStreet, addrTown };
    });

    const transactions = items.filter((t) => t.amount > 0);
    if (transactions.length === 0) {
      return {
        message: "no transaction found",
        debug: { postcode, records_fetched: bindings.length },
      } as PropertyValueInsightsNoMatch;
    }

    const streetLower = street.toLowerCase();
    const cityLower = city.toLowerCase();
    const hasStreetFilter = streetLower.length > 0;
    const hasCityFilter = cityLower.length > 0;

    let filtered = transactions;
    if (hasStreetFilter || hasCityFilter) {
      const addrFiltered = transactions.filter((t) => {
        const matchStreet = !hasStreetFilter || t.addrStreet.includes(streetLower) || streetLower.includes(t.addrStreet);
        const matchCity = !hasCityFilter || t.addrTown.includes(cityLower) || cityLower.includes(t.addrTown);
        return matchStreet && matchCity;
      });
      if (addrFiltered.length > 0) filtered = addrFiltered;
    }

    const filtered5y = filtered.filter((t) => t.dateMs >= fiveYearsAgo);
    const sortedByDate = [...filtered].sort((a, b) => b.dateMs - a.dateMs);
    const filteredLatest = sortedByDate[0];
    const averagePriceArea =
      filtered5y.length > 0 ? Math.round(filtered5y.reduce((s, t) => s + t.amount, 0) / filtered5y.length) : 0;

    const ukData = {
      latest_transaction: {
        price: filteredLatest.amount,
        date: filteredLatest.date || filteredLatest.dateStr,
        property_type: filteredLatest.category || undefined,
      },
      transactions_last_5_years: filtered5y.length,
      average_price_area: filtered5y.length > 0
        ? Math.round(filtered5y.reduce((s, t) => s + t.amount, 0) / filtered5y.length)
        : averagePriceArea,
    };

    const success: PropertyValueInsightsSuccess = {
      address: {
        city: city || postcode,
        street: street || postcode,
        house_number: "",
      },
      match_quality: "exact_building",
      latest_transaction: {
        transaction_date: ukData.latest_transaction.date,
        transaction_price: ukData.latest_transaction.price,
        property_size: 0,
        price_per_m2: 0,
      },
      current_estimated_value: null,
      building_summary_last_3_years: {
        transactions_count_last_3_years: filtered5y.length,
        transactions_count_last_5_years: filtered5y.length,
        latest_building_transaction_price: ukData.latest_transaction.price,
        average_apartment_value_today: ukData.average_price_area,
      },
      market_value_source: "exact_provider",
      source: "uk-land-registry",
      uk_land_registry: ukData,
    };

    return success;
  }
}
