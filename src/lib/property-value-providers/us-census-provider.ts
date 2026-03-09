/**
 * US Census ACS Provider
 * Fetches neighborhood statistics (median home value, income, population)
 * from the Census Bureau ACS API using lat/lng coordinates.
 * Does not replace RentCast; augments US property data.
 */

const CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates";
const CENSUS_ACS_URL = "https://api.census.gov/data/2022/acs/acs5";
const CENSUS_TIMEOUT_MS = 10000;

export type NeighborhoodStats = {
  median_home_value: number;
  median_household_income: number;
  population: number;
};

type CensusGeoItem = {
  STATE?: string;
  COUNTY?: string;
  TRACT?: string;
  GEOID?: string;
};

type CensusGeocoderResponse = {
  result?: {
    geographies?:
      | CensusGeoItem[]
      | Record<string, CensusGeoItem[]>;
  };
};

function env(key: string): string {
  return (process.env[key] ?? "").trim();
}

function isConfigured(): boolean {
  return Boolean(env("CENSUS_API_KEY"));
}

function parseNum(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val)) return val;
  const n = parseFloat(String(val ?? "").replace(/[^\d.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

/**
 * Convert lat/lng to state, county, tract using Census geocoder.
 */
async function geocodeCoordinates(
  lat: number,
  lng: number
): Promise<{ state: string; county: string; tract: string } | null> {
  const params = new URLSearchParams({
    x: String(lng),
    y: String(lat),
    benchmark: "Public_AR_Current",
    vintage: "Current_Current",
    layers: "14",
    format: "json",
  });

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), CENSUS_TIMEOUT_MS);

  try {
    const res = await fetch(`${CENSUS_GEOCODER_URL}?${params.toString()}`, {
      signal: controller.signal,
    });
    clearTimeout(id);
    if (!res.ok) return null;

    const data = (await res.json().catch(() => null)) as CensusGeocoderResponse | null;
    const raw = data?.result?.geographies;
    let geos: CensusGeoItem[] = [];
    if (Array.isArray(raw)) {
      geos = raw;
    } else if (raw && typeof raw === "object") {
      for (const v of Object.values(raw)) {
        if (Array.isArray(v) && v.length > 0) {
          geos = v;
          break;
        }
      }
    }
    if (geos.length === 0) return null;

    const first = geos[0];
    let state = (first?.STATE ?? "").trim();
    let county = (first?.COUNTY ?? "").trim();
    let tract = (first?.TRACT ?? "").replace(".", "").padStart(6, "0").slice(0, 6);

    if (!state || !county) {
      const geoid = (first?.GEOID ?? "").trim();
      if (geoid.length >= 5) {
        state = state || geoid.slice(0, 2);
        county = county || geoid.slice(2, 5);
        if (!tract && geoid.length >= 11) tract = geoid.slice(5, 11);
      }
    }
    if (!state || !county) return null;
    return { state, county, tract };
  } catch {
    clearTimeout(id);
    return null;
  }
}

/**
 * Fetch ACS variables for a census tract.
 * B25077_001E = Median Home Value
 * B19013_001E = Median Household Income
 * B01003_001E = Population
 */
async function fetchAcsData(
  state: string,
  county: string,
  tract: string
): Promise<NeighborhoodStats | null> {
  const apiKey = env("CENSUS_API_KEY");
  const variables = "B25077_001E,B19013_001E,B01003_001E";
  const forClause = tract ? `tract:${tract}` : "tract:*";
  const inClause = `state:${state}+county:${county}`;

  const params = new URLSearchParams({
    get: variables,
    for: forClause,
    in: inClause,
  });
  if (apiKey) params.set("key", apiKey);

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), CENSUS_TIMEOUT_MS);

  try {
    const res = await fetch(`${CENSUS_ACS_URL}?${params.toString()}`, {
      signal: controller.signal,
    });
    clearTimeout(id);
    if (!res.ok) return null;

    const rows = (await res.json().catch(() => null)) as unknown;
    if (!Array.isArray(rows) || rows.length < 2) return null;

    const header = rows[0] as string[];
    const dataRow = rows[1] as unknown[];
    const idx = (name: string) => header.indexOf(name);

    const medianHomeValIdx = idx("B25077_001E");
    const medianIncomeIdx = idx("B19013_001E");
    const populationIdx = idx("B01003_001E");

    const median_home_value = medianHomeValIdx >= 0 ? parseNum(dataRow[medianHomeValIdx]) : 0;
    const median_household_income = medianIncomeIdx >= 0 ? parseNum(dataRow[medianIncomeIdx]) : 0;
    const population = populationIdx >= 0 ? parseNum(dataRow[populationIdx]) : 0;

    return {
      median_home_value,
      median_household_income,
      population,
    };
  } catch {
    clearTimeout(id);
    return null;
  }
}

/**
 * Fetch neighborhood statistics for a US location.
 * Returns null on any failure; does not throw.
 */
export async function fetchNeighborhoodStats(
  latitude: number,
  longitude: number
): Promise<NeighborhoodStats | null> {
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return null;
  }

  try {
    const geo = await geocodeCoordinates(latitude, longitude);
    if (!geo) return null;

    const stats = await fetchAcsData(geo.state, geo.county, geo.tract);
    return stats;
  } catch {
    return null;
  }
}

export { isConfigured as isCensusConfigured };
