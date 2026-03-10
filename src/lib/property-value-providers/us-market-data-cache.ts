/**
 * US Market Data Cache
 * Stores normalized Zillow Research and Redfin Data Center data.
 * TTL: 21 days (suitable for monthly market datasets).
 * Persistence: in-memory + optional JSON file (no Supabase required).
 */

export type CachedMarketRecord = {
  /** Estimated/home value baseline (ZHVI or similar) */
  estimated_area_price?: number;
  /** Median sale price */
  median_sale_price?: number;
  /** Median price per sqft */
  median_price_per_sqft?: number;
  /** YoY change percent */
  market_trend_yoy?: number;
  /** Inventory count or signal */
  inventory_signal?: number;
  /** Days on market */
  days_on_market?: number;
  /** Source(s) that contributed */
  sources: ("zillow" | "redfin")[];
  /** Geographic level used */
  level: "zip" | "city" | "county" | "metro";
  /** Cached at timestamp */
  cached_at: number;
};

const TTL_MS = 21 * 24 * 60 * 60 * 1000; // 21 days
const CACHE_FILE = ".us-market-cache.json";

const memory = new Map<string, CachedMarketRecord>();

function cacheKey(level: "zip" | "city" | "county" | "metro", value: string): string {
  const v = (value ?? "").trim().toLowerCase().replace(/\s+/g, "-");
  return `${level}:${v}`;
}

function isExpired(record: CachedMarketRecord): boolean {
  return Date.now() - record.cached_at > TTL_MS;
}

export function getByZip(zip: string): CachedMarketRecord | null {
  const z = (zip ?? "").replace(/\D/g, "").slice(0, 5);
  if (!z || z.length < 5) return null;
  const r = memory.get(cacheKey("zip", z));
  return r && !isExpired(r) ? r : null;
}

export function getByCityState(city: string, state: string): CachedMarketRecord | null {
  const c = (city ?? "").trim();
  const s = (state ?? "").trim().toUpperCase().slice(0, 2);
  if (!c || !s) return null;
  const key = cacheKey("city", `${c},${s}`);
  const r = memory.get(key);
  return r && !isExpired(r) ? r : null;
}

export function getByCountyState(county: string, state: string): CachedMarketRecord | null {
  const co = (county ?? "").trim();
  const s = (state ?? "").trim().toUpperCase().slice(0, 2);
  if (!co || !s) return null;
  const key = cacheKey("county", `${co},${s}`);
  const r = memory.get(key);
  return r && !isExpired(r) ? r : null;
}

export function set(level: "zip" | "city" | "county" | "metro", value: string, record: Omit<CachedMarketRecord, "cached_at" | "level">): void {
  const key = cacheKey(level, value);
  const existing = memory.get(key);
  const sources = [...new Set([...(existing?.sources ?? []), ...record.sources])];
  const a = record.estimated_area_price ?? record.median_sale_price;
  const b = existing?.estimated_area_price ?? existing?.median_sale_price;
  const estimated_area_price = a != null && b != null ? Math.round((a + b) / 2) : a ?? b ?? undefined;
  const medA = record.median_sale_price;
  const medB = existing?.median_sale_price;
  const median_sale_price = medA != null && medB != null ? Math.round((medA + medB) / 2) : medA ?? medB ?? undefined;
  const merged: Omit<CachedMarketRecord, "cached_at"> = {
    estimated_area_price,
    median_sale_price,
    median_price_per_sqft: record.median_price_per_sqft ?? existing?.median_price_per_sqft,
    market_trend_yoy: record.market_trend_yoy ?? existing?.market_trend_yoy,
    inventory_signal: record.inventory_signal ?? existing?.inventory_signal,
    days_on_market: record.days_on_market ?? existing?.days_on_market,
    sources,
    level,
  };
  memory.set(key, { ...merged, cached_at: Date.now() });
}

export function setZip(zip: string, record: Omit<CachedMarketRecord, "cached_at" | "level">): void {
  const z = (zip ?? "").replace(/\D/g, "").slice(0, 5);
  if (z && z.length >= 5) set("zip", z, record);
}

export function setCityState(city: string, state: string, record: Omit<CachedMarketRecord, "cached_at" | "level">): void {
  const c = (city ?? "").trim();
  const s = (state ?? "").trim().toUpperCase().slice(0, 2);
  if (c && s) set("city", `${c},${s}`, record);
}

/** Lookup: try ZIP first, then city+state */
export function lookup(zip?: string, city?: string, state?: string): CachedMarketRecord | null {
  if (zip) {
    const byZip = getByZip(zip);
    if (byZip) return byZip;
  }
  if (city && state) return getByCityState(city, state);
  return null;
}

/** Load cache from file (Node.js only). Call at startup if file exists. */
export async function loadFromFile(): Promise<number> {
  if (typeof process === "undefined" || !process.cwd) return 0;
  try {
    const fs = await import("fs/promises");
    const path = await import("path");
    const filePath = path.join(process.cwd(), CACHE_FILE);
    const raw = await fs.readFile(filePath, "utf-8");
    const data = JSON.parse(raw) as Array<{ key: string; record: CachedMarketRecord }>;
    let count = 0;
    for (const { key, record } of data) {
      if (record && !isExpired(record)) {
        memory.set(key, record);
        count++;
      }
    }
    return count;
  } catch {
    return 0;
  }
}

/** Save cache to file (Node.js only). Call after sync. */
export async function saveToFile(): Promise<boolean> {
  if (typeof process === "undefined" || !process.cwd) return false;
  try {
    const fs = await import("fs/promises");
    const path = await import("path");
    const filePath = path.join(process.cwd(), CACHE_FILE);
    const data = Array.from(memory.entries()).map(([key, record]) => ({ key, record }));
    await fs.writeFile(filePath, JSON.stringify(data, null, 0), "utf-8");
    return true;
  } catch {
    return false;
  }
}

export const CACHE_TTL_DAYS = 21;

/** Get sample cache keys for logging (1 ZIP, 1 city). */
export function getSampleCacheKeys(): { zip?: string; city?: string } {
  const entries = Array.from(memory.entries());
  const zip = entries.find(([k]) => k.startsWith("zip:"))?.[0]?.replace("zip:", "");
  const city = entries.find(([k]) => k.startsWith("city:"))?.[0]?.replace("city:", "");
  return { zip, city };
}

/** Get total cache entry count. */
export function getCacheSize(): number {
  return memory.size;
}
