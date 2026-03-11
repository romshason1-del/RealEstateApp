/**
 * US FHFA Housing Price Index Provider
 * Fetches official housing market trend data from the Federal Housing Finance Agency.
 * Uses FRED API (api.fhfa.gov does not resolve). Requires FRED_API_KEY.
 * Independent from RentCast and Census. Does not break property card on failure.
 */

const FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations";
const FRED_TIMEOUT_MS = 10000;

/** State abbreviation to FRED series ID (All-Transactions House Price Index) */
const STATE_FRED_SERIES: Record<string, string> = {
  AL: "ALSTHPI", AK: "AKSTHPI", AZ: "AZSTHPI", AR: "ARSTHPI", CA: "CASTHPI",
  CO: "COSTHPI", CT: "CTSTHPI", DE: "DESTHPI", FL: "FLSTHPI", GA: "GASTHPI",
  HI: "HISTHPI", ID: "IDSTHPI", IL: "ILSTHPI", IN: "INSTHPI", IA: "IASTHPI",
  KS: "KSSTHPI", KY: "KYSTHPI", LA: "LASTHPI", ME: "MESTHPI", MD: "MDSTHPI",
  MA: "MASTHPI", MI: "MISTHPI", MN: "MNSTHPI", MS: "MSSTHPI", MO: "MOSTHPI",
  MT: "MTSTHPI", NE: "NESTHPI", NV: "NVSTHPI", NH: "NHSTHPI", NJ: "NJSTHPI",
  NM: "NMSTHPI", NY: "NYSTHPI", NC: "NCSTHPI", ND: "NDSTHPI", OH: "OHSTHPI",
  OK: "OKSTHPI", OR: "ORSTHPI", PA: "PASTHPI", RI: "RISTHPI", SC: "SCSTHPI",
  SD: "SDSTHPI", TN: "TNSTHPI", TX: "TXSTHPI", UT: "UTSTHPI", VT: "VTSTHPI",
  VA: "VASTHPI", WA: "WASTHPI", WV: "WVSTHPI", WI: "WISTHPI", WY: "WYSTHPI",
  DC: "DCSTHPI",
};

export type MarketTrend = {
  hpi_index: number;
  change_1y_percent: number;
  /** Latest observation date (YYYY-MM) when available */
  latest_date?: string;
};

function parseNum(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val)) return val;
  const n = parseFloat(String(val ?? "").replace(/[^\d.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function getFredSeriesId(state: string): string | null {
  const s = (state ?? "").trim().toUpperCase().slice(0, 2);
  return STATE_FRED_SERIES[s] ?? null;
}

/**
 * Fetch FHFA HPI data via FRED API for a state.
 * Returns latest index and 1-year percentage change.
 */
export async function fetchMarketTrend(input: {
  state?: string;
  county?: string;
  zip?: string;
  latitude?: number;
  longitude?: number;
}): Promise<{ market_trend: MarketTrend } | null> {
  const seriesId = getFredSeriesId(input.state ?? "");
  if (!seriesId) return null;

  const apiKey = (process.env.FRED_API_KEY ?? "").trim();
  if (!apiKey) return null;

  const params = new URLSearchParams({
    series_id: seriesId,
    api_key: apiKey,
    file_type: "json",
    sort_order: "desc",
    limit: "20",
  });

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), FRED_TIMEOUT_MS);

  try {
    const url = `${FRED_API_URL}?${params.toString()}`;
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    clearTimeout(id);
    if (!res.ok) return null;

    const data = (await res.json().catch(() => null)) as { observations?: Array<{ value: string; date: string }> } | null;
    const observations = data?.observations;
    if (!Array.isArray(observations) || observations.length === 0) return null;

    const valid = observations
      .filter((o) => o?.value && o.value !== ".")
      .map((o) => ({ value: parseNum(o.value), date: (o.date ?? "").trim() }))
      .filter((o) => o.value > 0);

    if (valid.length === 0) return null;

    const latest = valid[0];
    const latestIndex = latest!.value;
    const latestDate = latest!.date;

    const oneYearAgo = new Date(latestDate + "-01");
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
    const priorYearStr = oneYearAgo.getFullYear().toString();

    const priorEntry = valid.find((o) => o.date.startsWith(priorYearStr));
    const priorIndex = priorEntry?.value ?? (valid.length >= 4 ? valid[3]?.value : valid[valid.length - 1]?.value) ?? latestIndex;

    const change1y = priorIndex > 0 ? ((latestIndex - priorIndex) / priorIndex) * 100 : 0;

    return {
      market_trend: {
        hpi_index: Math.round(latestIndex * 10) / 10,
        change_1y_percent: Math.round(change1y * 10) / 10,
        latest_date: latestDate || undefined,
      },
    };
  } catch {
    clearTimeout(id);
    return null;
  }
}
