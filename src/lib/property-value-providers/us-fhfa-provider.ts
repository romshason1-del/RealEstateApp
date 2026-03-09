/**
 * US FHFA Housing Price Index Provider
 * Fetches official housing market trend data from the Federal Housing Finance Agency.
 * Independent from RentCast and Census. Does not break property card on failure.
 */

const FHFA_API_URL = "https://api.fhfa.gov/public/hpi";
const FHFA_TIMEOUT_MS = 10000;

/** State abbreviation to FIPS code */
const STATE_FIPS: Record<string, string> = {
  AL: "01", AK: "02", AZ: "04", AR: "05", CA: "06", CO: "08", CT: "09",
  DE: "10", FL: "12", GA: "13", HI: "15", ID: "16", IL: "17", IN: "18",
  IA: "19", KS: "20", KY: "21", LA: "22", ME: "23", MD: "24", MA: "25",
  MI: "26", MN: "27", MS: "28", MO: "29", MT: "30", NE: "31", NV: "32",
  NH: "33", NJ: "34", NM: "35", NY: "36", NC: "37", ND: "38", OH: "39",
  OK: "40", OR: "41", PA: "42", RI: "44", SC: "45", SD: "46", TN: "47",
  TX: "48", UT: "49", VT: "50", VA: "51", WA: "53", WV: "54", WI: "55",
  WY: "56", DC: "11",
};

export type MarketTrend = {
  hpi_index: number;
  change_1y_percent: number;
};

function parseNum(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val)) return val;
  const n = parseFloat(String(val ?? "").replace(/[^\d.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function getStateFips(state: string): string | null {
  const s = (state ?? "").trim().toUpperCase().slice(0, 2);
  return STATE_FIPS[s] ?? null;
}

/**
 * Fetch FHFA HPI data for a state.
 * Returns latest index and 1-year percentage change.
 */
export async function fetchMarketTrend(input: {
  state?: string;
  county?: string;
  zip?: string;
  latitude?: number;
  longitude?: number;
}): Promise<{ market_trend: MarketTrend } | null> {
  const stateFips = getStateFips(input.state ?? "");
  if (!stateFips) return null;

  const params = new URLSearchParams();
  params.set("state", stateFips);
  if (input.county?.trim()) params.set("county", input.county.trim());
  if (input.zip?.trim()) params.set("zip", input.zip.trim());

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), FHFA_TIMEOUT_MS);

  try {
    const url = `${FHFA_API_URL}?${params.toString()}`;
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    clearTimeout(id);
    if (!res.ok) return null;

    const data = (await res.json().catch(() => null)) as unknown;
    if (!data || typeof data !== "object") return null;

    const obj = data as Record<string, unknown>;
    const records = Array.isArray(obj.data) ? obj.data : Array.isArray(obj.records) ? obj.records : Array.isArray(obj) ? obj : null;
    if (!records || records.length === 0) return null;

    const parseRecord = (r: unknown): { index?: number; period?: string } => {
      if (!r || typeof r !== "object") return {};
      const rec = r as Record<string, unknown>;
      const index = parseNum(rec.index ?? rec.hpi ?? rec.value ?? rec.HPI ?? 0);
      const period = String(rec.period ?? rec.date ?? rec.yr ?? rec.year ?? "").trim();
      return { index: index > 0 ? index : undefined, period };
    };

    const entries = records
      .map(parseRecord)
      .filter((e) => e.index != null && e.index > 0 && e.period)
      .sort((a, b) => (b.period ?? "").localeCompare(a.period ?? ""));

    if (entries.length === 0) return null;

    const latest = entries[0];
    const latestIndex = latest.index ?? 0;

    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
    const priorYearStr = oneYearAgo.getFullYear().toString();

    const priorEntry = entries.find((e) => (e.period ?? "").startsWith(priorYearStr));
    const priorIndex = priorEntry?.index ?? (entries.length >= 12 ? entries[11]?.index : entries[entries.length - 1]?.index) ?? latestIndex;

    const change1y = priorIndex > 0 ? ((latestIndex - priorIndex) / priorIndex) * 100 : 0;

    return {
      market_trend: {
        hpi_index: Math.round(latestIndex * 10) / 10,
        change_1y_percent: Math.round(change1y * 10) / 10,
      },
    };
  } catch {
    clearTimeout(id);
    return null;
  }
}
