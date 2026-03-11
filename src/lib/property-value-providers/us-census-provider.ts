/**
 * US Census ACS Provider
 * Fetches neighborhood statistics (median home value, income, population)
 * from the Census Bureau ACS API using lat/lng coordinates.
 * Does not replace RentCast; augments US property data.
 */

const CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates";
const CENSUS_ACS_URL = "https://api.census.gov/data/2022/acs/acs5";
const CENSUS_ACS_URL_2017 = "https://api.census.gov/data/2017/acs/acs5";
const CENSUS_TIMEOUT_MS = 20000;

export type NeighborhoodStats = {
  median_home_value: number;
  median_household_income: number;
  population: number;
  median_rent?: number;
  population_growth_percent?: number;
  income_growth_percent?: number;
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

function computeGrowthPercent(current: number, previous: number): number | undefined {
  if (!Number.isFinite(previous) || previous <= 0 || !Number.isFinite(current)) return undefined;
  return ((current - previous) / previous) * 100;
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
    layers: "14,6",
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
      const allArrs = Object.values(raw).filter((v): v is CensusGeoItem[] => Array.isArray(v) && v.length > 0);
      const withTract = allArrs.find((arr) => {
        const f = arr[0];
        return f && (f.TRACT || ((f.GEOID ?? "").length >= 11));
      });
      geos = withTract ?? allArrs[0] ?? [];
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
 * Fetch B01003_001E (population) and B19013_001E (income) from prior-year ACS for growth calculation.
 */
async function fetchAcsPriorYear(
  baseUrl: string,
  params: URLSearchParams
): Promise<{ population: number; median_household_income: number } | null> {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), CENSUS_TIMEOUT_MS);
  try {
    const res = await fetch(`${baseUrl}?${params.toString()}`, { signal: controller.signal });
    clearTimeout(id);
    if (!res.ok) return null;
    const rows = (await res.json().catch(() => null)) as unknown;
    if (!Array.isArray(rows) || rows.length < 2) return null;
    const header = rows[0] as string[];
    const dataRow = rows[1] as unknown[];
    const idx = (name: string) => header.indexOf(name);
    const population = idx("B01003_001E") >= 0 ? parseNum(dataRow[idx("B01003_001E")]) : 0;
    const median_household_income = idx("B19013_001E") >= 0 ? parseNum(dataRow[idx("B19013_001E")]) : 0;
    return { population, median_household_income };
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
 * B25064_001E = Median Gross Rent
 */
async function fetchAcsData(
  state: string,
  county: string,
  tract: string
): Promise<NeighborhoodStats | null> {
  const apiKey = env("CENSUS_API_KEY");
  const variables = "B25077_001E,B19013_001E,B01003_001E,B25064_001E";
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
    const medianRentIdx = idx("B25064_001E");

    const median_home_value = medianHomeValIdx >= 0 ? parseNum(dataRow[medianHomeValIdx]) : 0;
    const median_household_income = medianIncomeIdx >= 0 ? parseNum(dataRow[medianIncomeIdx]) : 0;
    const population = populationIdx >= 0 ? parseNum(dataRow[populationIdx]) : 0;
    const median_rent = medianRentIdx >= 0 ? parseNum(dataRow[medianRentIdx]) : undefined;

    const priorParams = new URLSearchParams({
      get: "B01003_001E,B19013_001E",
      for: forClause,
      in: inClause,
    });
    if (apiKey) priorParams.set("key", apiKey);
    const prior = await fetchAcsPriorYear(CENSUS_ACS_URL_2017, priorParams);
    const population_growth_percent = prior ? computeGrowthPercent(population, prior.population) : undefined;
    const income_growth_percent = prior ? computeGrowthPercent(median_household_income, prior.median_household_income) : undefined;

    return {
      median_home_value,
      median_household_income,
      population,
      ...(median_rent != null && median_rent > 0 && { median_rent }),
      ...(population_growth_percent != null && { population_growth_percent }),
      ...(income_growth_percent != null && { income_growth_percent }),
    };
  } catch {
    clearTimeout(id);
    return null;
  }
}

/**
 * Fetch ACS data by ZCTA (ZIP Code Tabulation Area). Fallback when geocoder fails.
 */
async function fetchAcsByZcta(zip: string): Promise<NeighborhoodStats | null> {
  const z = (zip ?? "").replace(/\D/g, "").slice(0, 5);
  if (z.length < 5) return null;

  const apiKey = env("CENSUS_API_KEY");
  const variables = "B25077_001E,B19013_001E,B01003_001E,B25064_001E";
  const params = new URLSearchParams({
    get: variables,
    for: `zip code tabulation area:${z}`,
  });
  if (apiKey) params.set("key", apiKey);

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), CENSUS_TIMEOUT_MS);
  try {
    const res = await fetch(`${CENSUS_ACS_URL}?${params.toString()}`, { signal: controller.signal });
    clearTimeout(id);
    if (!res.ok || res.status === 204) return null;
    const text = await res.text();
    if (!text.trim()) return null;
    const rows = JSON.parse(text) as unknown;
    if (!Array.isArray(rows) || rows.length < 2) return null;
    const header = rows[0] as string[];
    const dataRow = rows[1] as unknown[];
    const idx = (name: string) => header.indexOf(name);
    const median_rent = idx("B25064_001E") >= 0 ? parseNum(dataRow[idx("B25064_001E")]) : 0;
    const median_home_value = idx("B25077_001E") >= 0 ? parseNum(dataRow[idx("B25077_001E")]) : 0;
    const median_household_income = idx("B19013_001E") >= 0 ? parseNum(dataRow[idx("B19013_001E")]) : 0;
    const population = idx("B01003_001E") >= 0 ? parseNum(dataRow[idx("B01003_001E")]) : 0;

    const priorParams = new URLSearchParams({
      get: "B01003_001E,B19013_001E",
      for: `zip code tabulation area:${z}`,
    });
    if (apiKey) priorParams.set("key", apiKey);
    const prior = await fetchAcsPriorYear(CENSUS_ACS_URL_2017, priorParams);
    const population_growth_percent = prior ? computeGrowthPercent(population, prior.population) : undefined;
    const income_growth_percent = prior ? computeGrowthPercent(median_household_income, prior.median_household_income) : undefined;

    return {
      median_home_value,
      median_household_income,
      population,
      ...(median_rent > 0 && { median_rent }),
      ...(population_growth_percent != null && { population_growth_percent }),
      ...(income_growth_percent != null && { income_growth_percent }),
    };
  } catch {
    clearTimeout(id);
    return null;
  }
}

/**
 * Fetch neighborhood statistics for a US location.
 * Prefers ZCTA (ZIP) when available (faster, more reliable); falls back to lat/lng geocoder.
 * Returns null on any failure; does not throw.
 */
export async function fetchNeighborhoodStats(
  latitude: number,
  longitude: number,
  options?: { zip?: string }
): Promise<NeighborhoodStats | null> {
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    if (options?.zip) return fetchAcsByZcta(options.zip);
    return null;
  }

  try {
    if (options?.zip) {
      const zctaStats = await fetchAcsByZcta(options.zip);
      if (zctaStats && (zctaStats.median_home_value > 0 || zctaStats.median_household_income > 0 || zctaStats.population > 0)) {
        return zctaStats;
      }
    }
    const geo = await geocodeCoordinates(latitude, longitude);
    if (geo) {
      const stats = await fetchAcsData(geo.state, geo.county, geo.tract);
      if (stats) return stats;
    }
    if (options?.zip) return fetchAcsByZcta(options.zip);
    return null;
  } catch {
    if (options?.zip) return fetchAcsByZcta(options.zip);
    return null;
  }
}

export { isConfigured as isCensusConfigured };
