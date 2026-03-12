/**
 * UK House Price Index (HPI) Provider
 * Fetches official HM Land Registry / ONS House Price Index data.
 * Used as fallback when Land Registry Price Paid transactions are unavailable.
 * API: http://landregistry.data.gov.uk/landregistry/query (SPARQL)
 */

const SPARQL_ENDPOINT = "http://landregistry.data.gov.uk/landregistry/query";

export type UKHPIResult = {
  average_area_price: number;
  median_area_price: number | null;
  price_trend: { change_1y_percent: number; ref_month: string };
  data_source: "HPI";
  region_name: string;
  region_slug: string;
} | null;

/** Map city/locality to UK HPI region slug. HPI covers 441+ regions at local authority / London borough level. */
function cityToRegionSlug(city: string): string | null {
  const c = (city ?? "").trim().toLowerCase().replace(/\s+/g, "-").replace(/[^\w-]/g, "");
  if (!c || c.length < 2) return null;

  // London boroughs and major cities - explicit mappings for HPI region URIs
  const mappings: Record<string, string> = {
    london: "london",
    "greater-london": "london",
    camden: "camden",
    westminster: "westminster",
    "city-of-westminster": "westminster",
    "city-of-london": "city-of-london",
    islington: "islington",
    hackney: "hackney",
    "tower-hamlets": "tower-hamlets",
    newham: "newham",
    greenwich: "greenwich",
    lewisham: "lewisham",
    southwark: "southwark",
    lambeth: "lambeth",
    wandsworth: "wandsworth",
    hammersmith: "hammersmith-and-fulham",
    "hammersmith-and-fulham": "hammersmith-and-fulham",
    kensington: "kensington-and-chelsea",
    "kensington-and-chelsea": "kensington-and-chelsea",
    brent: "brent",
    ealing: "ealing",
    hounslow: "hounslow",
    richmond: "richmond-upon-thames",
    "richmond-upon-thames": "richmond-upon-thames",
    kingston: "kingston-upon-thames",
    "kingston-upon-thames": "kingston-upon-thames",
    merton: "merton",
    sutton: "sutton",
    croydon: "croydon",
    bromley: "bromley",
    bexley: "bexley",
    havering: "havering",
    barking: "barking-and-dagenham",
    "barking-and-dagenham": "barking-and-dagenham",
    redbridge: "redbridge",
    waltham: "waltham-forest",
    "waltham-forest": "waltham-forest",
    haringey: "haringey",
    enfield: "enfield",
    barnet: "barnet",
    harrow: "harrow",
    hillingdon: "hillingdon",
    manchester: "manchester",
    birmingham: "birmingham",
    leeds: "leeds",
    liverpool: "liverpool",
    sheffield: "sheffield",
    bristol: "bristol",
    newcastle: "newcastle-upon-tyne",
    "newcastle-upon-tyne": "newcastle-upon-tyne",
    nottingham: "nottingham",
    leicester: "leicester",
    southampton: "southampton",
    brighton: "brighton-and-hove",
    "brighton-and-hove": "brighton-and-hove",
    plymouth: "plymouth",
    reading: "reading",
    oxford: "oxford",
    cambridge: "cambridge",
    edinburgh: "edinburgh",
    glasgow: "glasgow",
    aberdeen: "aberdeen-city",
    "aberdeen-city": "aberdeen-city",
    cardiff: "cardiff",
    swansea: "swansea",
    belfast: "belfast",
  };

  if (mappings[c]) return mappings[c];
  // Try direct slug (many local authorities match)
  return c;
}

/** Build HPI query. UK HPI: observations link to region via refRegion. */
function buildHPISparqlQuerySimple(regionSlug: string): string {
  const regionUri = `http://landregistry.data.gov.uk/id/region/${regionSlug}`;
  return `
PREFIX ukhpi: <http://landregistry.data.gov.uk/def/ukhpi/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?refMonth ?averagePrice ?housePriceIndex
WHERE {
  ?obs ukhpi:refRegion <${regionUri}> ;
       ukhpi:refMonth ?refMonth ;
       ukhpi:housePriceIndex ?housePriceIndex .
  OPTIONAL { ?obs ukhpi:averagePrice ?averagePrice }
}
ORDER BY DESC(?refMonth)
LIMIT 25
`.trim();
}

type SparqlBinding = { value?: string };

function getBinding(binding: Record<string, SparqlBinding>, key: string): string {
  const b = binding[key];
  return b?.value ?? "";
}

/** Map outward postcode (e.g. NW1, SW1) to region for HPI lookup. London postcodes only. */
function postcodeOutwardToRegion(outward: string): string | null {
  const o = (outward ?? "").trim().toUpperCase().replace(/\s/g, "");
  if (!o || o.length < 2) return null;
  // London postcode prefixes: E, EC, N, NW, SE, SW, W, WC
  const londonPrefixes = ["E", "EC", "N", "NW", "SE", "SW", "W", "WC"];
  if (londonPrefixes.some((p) => o.startsWith(p))) return "london";
  return null;
}

/**
 * Fetch UK House Price Index data for a locality (city / local authority) or postcode district.
 * Returns average price, price trend (YoY), and data source.
 */
export async function fetchUKHPIForLocality(city: string, postcode?: string): Promise<UKHPIResult> {
  let regionSlug = cityToRegionSlug(city);
  if (!regionSlug && postcode) {
    const outward = postcode.split(/\s/)[0] || postcode.replace(/\s/g, "").slice(0, 4);
    const fromPostcode = postcodeOutwardToRegion(outward);
    if (fromPostcode) regionSlug = fromPostcode;
  }
  if (!regionSlug) return null;

  const query = buildHPISparqlQuerySimple(regionSlug);

  try {
    const res = await fetch(SPARQL_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/sparql-results+json",
      },
      body: new URLSearchParams({ query }),
      signal: AbortSignal.timeout(3000),
    });

    if (!res.ok) return null;

    const json = (await res.json()) as { results?: { bindings?: Record<string, SparqlBinding>[] } };
    const bindings = json?.results?.bindings ?? [];
    if (bindings.length === 0) return null;

    let latest: Record<string, SparqlBinding> | null = null;
    let averagePrice = 0;
    for (const b of bindings) {
      const avgStr = getBinding(b, "averagePrice");
      const avg = parseFloat(avgStr.replace(/[^\d.]/g, ""));
      if (Number.isFinite(avg) && avg > 0) {
        latest = b;
        averagePrice = avg;
        break;
      }
    }
    if (!latest || averagePrice <= 0) return null;

    const refMonth = getBinding(latest, "refMonth");
    const indexStr = getBinding(latest, "housePriceIndex");
    const housePriceIndex = parseFloat(indexStr.replace(/[^\d.]/g, ""));

    // Compute YoY trend from index if we have multiple months
    let change1yPercent = 0;
    if (bindings.length >= 13) {
      const yearAgo = bindings[12];
      const prevIndexStr = getBinding(yearAgo, "housePriceIndex");
      const prevIndex = parseFloat(prevIndexStr.replace(/[^\d.]/g, ""));
      if (Number.isFinite(prevIndex) && prevIndex > 0) {
        change1yPercent = ((housePriceIndex - prevIndex) / prevIndex) * 100;
      }
    }

    const regionName = regionSlug
      .split("-")
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(" ");

    return {
      average_area_price: Math.round(averagePrice),
      median_area_price: null,
      price_trend: {
        change_1y_percent: Math.round(change1yPercent * 10) / 10,
        ref_month: refMonth ? refMonth.slice(0, 7) : "",
      },
      data_source: "HPI",
      region_name: regionName,
      region_slug: regionSlug,
    };
  } catch {
    return null;
  }
}

export type UKHPIIndexResult = {
  refMonth: string;
  housePriceIndex: number;
  averagePrice?: number;
} | null;

/**
 * Fetch UK HPI index for a region. Returns latest and historical observations for HPI adjustment.
 */
export async function fetchUKHPIIndicesForRegion(regionSlug: string): Promise<Array<{ refMonth: string; housePriceIndex: number; averagePrice?: number }>> {
  const regionUri = `http://landregistry.data.gov.uk/id/region/${regionSlug}`;
  const query = `
PREFIX ukhpi: <http://landregistry.data.gov.uk/def/ukhpi/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?refMonth ?housePriceIndex ?averagePrice
WHERE {
  ?obs ukhpi:refRegion <${regionUri}> ;
       ukhpi:refMonth ?refMonth ;
       ukhpi:housePriceIndex ?housePriceIndex .
  OPTIONAL { ?obs ukhpi:averagePrice ?averagePrice }
}
ORDER BY DESC(?refMonth)
LIMIT 50
`.trim();

  try {
    const res = await fetch(SPARQL_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/sparql-results+json",
      },
      body: new URLSearchParams({ query }),
      signal: AbortSignal.timeout(3000),
    });
    if (!res.ok) return [];

    const json = (await res.json()) as { results?: { bindings?: Record<string, SparqlBinding>[] } };
    const bindings = json?.results?.bindings ?? [];
    return bindings
      .map((b) => {
        const refMonth = getBinding(b, "refMonth");
        const idxStr = getBinding(b, "housePriceIndex");
        const avgStr = getBinding(b, "averagePrice");
        const housePriceIndex = parseFloat(idxStr.replace(/[^\d.]/g, ""));
        const averagePrice = avgStr ? parseFloat(avgStr.replace(/[^\d.]/g, "")) : undefined;
        if (!Number.isFinite(housePriceIndex) || housePriceIndex <= 0) return null;
        return { refMonth, housePriceIndex, averagePrice };
      })
      .filter((x): x is NonNullable<typeof x> => x != null);
  } catch {
    return [];
  }
}

/**
 * Fetch UK HPI indices for a locality (city/postcode). Used for HPI-adjusted value estimate.
 */
export async function fetchUKHPIIndicesForLocality(city: string, postcode?: string): Promise<Array<{ refMonth: string; housePriceIndex: number; averagePrice?: number }>> {
  let regionSlug = cityToRegionSlug(city);
  if (!regionSlug && postcode) {
    const outward = postcode.split(/\s/)[0] || postcode.replace(/\s/g, "").slice(0, 4);
    const fromPostcode = postcodeOutwardToRegion(outward);
    if (fromPostcode) regionSlug = fromPostcode;
  }
  if (!regionSlug) return [];
  return fetchUKHPIIndicesForRegion(regionSlug);
}

/**
 * Estimate current value from last transaction using UK HPI.
 * Formula: lastPrice * (currentIndex / indexAtSaleDate)
 */
export function estimateValueFromHPI(
  lastPrice: number,
  saleDate: string,
  indices: Array<{ refMonth: string; housePriceIndex: number }>
): number | null {
  if (!Number.isFinite(lastPrice) || lastPrice <= 0 || indices.length === 0) return null;
  const saleMonth = saleDate ? saleDate.slice(0, 7) : "";
  const latest = indices[0];
  if (!latest) return null;
  const currentIndex = latest.housePriceIndex;
  const saleIndexEntry = indices.find((i) => i.refMonth.startsWith(saleMonth));
  const saleIndex = saleIndexEntry?.housePriceIndex ?? indices[indices.length - 1]?.housePriceIndex;
  if (!saleIndex || saleIndex <= 0) return null;
  return Math.round(lastPrice * (currentIndex / saleIndex));
}
