/**
 * Zillow Research Data Provider
 * Uses downloadable ZHVI and median sale price datasets.
 * Data: https://www.zillow.com/research/data/
 * Updated monthly on the 16th. No API key required.
 */

import {
  getByZip,
  getByCityState,
  setZip,
  setCityState,
  type CachedMarketRecord,
} from "./us-market-data-cache";

const ZILLOW_ZIP_ZHVI_URL =
  "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv";
const ZILLOW_CITY_ZHVI_URL =
  "https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv";
const ZILLOW_METRO_MEDIAN_SALE_URL =
  "https://files.zillowstatic.com/research/public_csvs/sales_count_now/Metro_median_sale_price_uc_sfrcondo_month.csv";

/** Get cached Zillow market data. Returns null if not in cache. */
export function getZillowMarketData(zip?: string, city?: string, state?: string): CachedMarketRecord | null {
  if (zip) {
    const r = getByZip(zip);
    if (r && r.sources.includes("zillow")) return r;
  }
  if (city && state) {
    const r = getByCityState(city, state);
    if (r && r.sources.includes("zillow")) return r;
  }
  return null;
}

/** Parse CSV line handling quoted fields */
function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      inQuotes = !inQuotes;
    } else if (inQuotes) {
      current += c;
    } else if (c === ",") {
      result.push(current.trim());
      current = "";
    } else {
      current += c;
    }
  }
  result.push(current.trim());
  return result;
}

/** Extract latest month value from ZHVI row (columns are 2000-01, 2000-02, ...) */
function getLatestValue(row: string[], headers: string[]): number | null {
  const monthCols = headers
    .map((h, i) => ({ h, i }))
    .filter(({ h }) => /^\d{4}-\d{2}$/.test(h))
    .sort((a, b) => b.h.localeCompare(a.h));
  for (const { h, i } of monthCols) {
    const val = parseFloat((row[i] ?? "").replace(/[^\d.]/g, ""));
    if (Number.isFinite(val) && val > 0) return Math.round(val);
  }
  return null;
}

/** Compute YoY change from row */
function getYoYChange(row: string[], headers: string[]): number | null {
  const monthCols = headers
    .map((h, i) => ({ h, i }))
    .filter(({ h }) => /^\d{4}-\d{2}$/.test(h))
    .sort((a, b) => b.h.localeCompare(a.h));
  if (monthCols.length < 13) return null;
  const current = parseFloat((row[monthCols[0]!.i] ?? "").replace(/[^\d.]/g, ""));
  const yearAgo = parseFloat((row[monthCols[12]!.i] ?? "").replace(/[^\d.]/g, ""));
  if (!Number.isFinite(current) || !Number.isFinite(yearAgo) || yearAgo <= 0) return null;
  return Math.round(((current - yearAgo) / yearAgo) * 1000) / 10;
}

/** Sync ZIP-level ZHVI into cache. Safe to call periodically. */
export async function syncZillowZipData(): Promise<{ count: number; error?: string }> {
  try {
    const res = await fetch(ZILLOW_ZIP_ZHVI_URL, { signal: AbortSignal.timeout(60000) });
    if (!res.ok) return { count: 0, error: `HTTP ${res.status}` };
    const text = await res.text();
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (lines.length < 2) return { count: 0, error: "Empty or invalid CSV" };

    const headers = parseCSVLine(lines[0]!);
    const regionNameIdx = headers.findIndex((h) => /regionname|region_name|name/i.test(h));
    const stateIdx = headers.findIndex((h) => /statename|state_name|state/i.test(h));
    const regionTypeIdx = headers.findIndex((h) => /regiontype|region_type|type/i.test(h));

    let count = 0;
    for (let i = 1; i < Math.min(lines.length, 35000); i++) {
      const row = parseCSVLine(lines[i]!);
      const regionType = (row[regionTypeIdx ?? -1] ?? "").toLowerCase();
      if (regionType && regionType !== "zip" && regionType !== "zip code" && !/zip/i.test(regionType)) continue;

      const regionName = (row[regionNameIdx >= 0 ? regionNameIdx : 2] ?? "").trim();
      const zip = regionName.replace(/\D/g, "").slice(0, 5);
      if (zip.length < 5) continue;

      const value = getLatestValue(row, headers);
      if (value == null || value <= 0) continue;

      const yoy = getYoYChange(row, headers);
      setZip(zip, {
        estimated_area_price: value,
        median_sale_price: value,
        market_trend_yoy: yoy ?? undefined,
        sources: ["zillow"],
      });
      count++;
    }
    return { count };
  } catch (err) {
    return { count: 0, error: err instanceof Error ? err.message : "Unknown error" };
  }
}

/** Sync City-level ZHVI into cache. */
export async function syncZillowCityData(): Promise<{ count: number; error?: string }> {
  try {
    const res = await fetch(ZILLOW_CITY_ZHVI_URL, { signal: AbortSignal.timeout(60000) });
    if (!res.ok) return { count: 0, error: `HTTP ${res.status}` };
    const text = await res.text();
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (lines.length < 2) return { count: 0, error: "Empty or invalid CSV" };

    const headers = parseCSVLine(lines[0]!);
    const regionNameIdx = headers.findIndex((h) => /region|name/i.test(h));
    const stateIdx = headers.findIndex((h) => /state/i.test(h));

    let count = 0;
    for (let i = 1; i < Math.min(lines.length, 25000); i++) {
      const row = parseCSVLine(lines[i]!);
      const city = (row[regionNameIdx] ?? "").trim();
      const state = (row[stateIdx] ?? "").trim().toUpperCase().slice(0, 2);
      if (!city || !state) continue;

      const value = getLatestValue(row, headers);
      if (value == null || value <= 0) continue;

      const yoy = getYoYChange(row, headers);
      setCityState(city, state, {
        estimated_area_price: value,
        median_sale_price: value,
        market_trend_yoy: yoy ?? undefined,
        sources: ["zillow"],
      });
      count++;
    }
    return { count };
  } catch (err) {
    return { count: 0, error: err instanceof Error ? err.message : "Unknown error" };
  }
}
