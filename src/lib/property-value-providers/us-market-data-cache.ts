/**
 * US Market Data Cache
 * Stores normalized Zillow Research and Redfin Data Center data.
 * TTL: 21 days (suitable for monthly market datasets).
 * Persistence:
 * - Local: in-memory + .us-market-cache.json (development)
 * - Production: Supabase us_market_data table (Vercel has no local file)
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
const SUPABASE_TABLE = "us_market_data";

const memory = new Map<string, CachedMarketRecord>();

function cacheKey(level: "zip" | "city" | "county" | "metro", value: string): string {
  const v = (value ?? "").trim().toLowerCase().replace(/\s+/g, "-");
  return `${level}:${v}`;
}

function isExpired(record: CachedMarketRecord): boolean {
  return Date.now() - record.cached_at > TTL_MS;
}

function isSupabaseConfigured(): boolean {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const key = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  return Boolean(url && key);
}

/** Create Supabase client for reads (anon key, works in serverless) */
async function getSupabaseReadClient() {
  const { createClient } = await import("@supabase/supabase-js");
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const anonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  return createClient(url, anonKey);
}

/** Create Supabase admin client for writes (service role, sync script only) */
async function getSupabaseAdminClient() {
  const { createClient } = await import("@supabase/supabase-js");
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
  if (!serviceKey) throw new Error("SUPABASE_SERVICE_ROLE_KEY required for sync");
  return createClient(url, serviceKey);
}

function rowToRecord(row: {
  level: string;
  value: string;
  estimated_area_price?: number | null;
  median_sale_price?: number | null;
  median_price_per_sqft?: number | null;
  market_trend_yoy?: number | null;
  inventory_signal?: number | null;
  days_on_market?: number | null;
  sources: unknown;
  cached_at: string;
}): CachedMarketRecord | null {
  if (!row?.level || !row?.value) return null;
  const cachedAt = row.cached_at ? new Date(row.cached_at).getTime() : Date.now();
  if (Date.now() - cachedAt > TTL_MS) return null;
  const sources = Array.isArray(row.sources)
    ? (row.sources as string[]).filter((s): s is "zillow" | "redfin" => s === "zillow" || s === "redfin")
    : [];
  return {
    estimated_area_price: row.estimated_area_price ?? undefined,
    median_sale_price: row.median_sale_price ?? undefined,
    median_price_per_sqft: row.median_price_per_sqft ?? undefined,
    market_trend_yoy: row.market_trend_yoy ?? undefined,
    inventory_signal: row.inventory_signal ?? undefined,
    days_on_market: row.days_on_market ?? undefined,
    sources,
    level: row.level as CachedMarketRecord["level"],
    cached_at: cachedAt,
  };
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

/** Lookup (sync): memory only. Use lookupAsync for production (memory + Supabase). */
export function lookup(zip?: string, city?: string, state?: string): CachedMarketRecord | null {
  if (zip) {
    const byZip = getByZip(zip);
    if (byZip) return byZip;
  }
  if (city && state) return getByCityState(city, state);
  return null;
}

/** Lookup from Supabase by zip or city+state */
async function lookupFromSupabase(zip?: string, city?: string, state?: string): Promise<CachedMarketRecord | null> {
  if (!isSupabaseConfigured()) return null;
  try {
    const supabase = await getSupabaseReadClient();
    if (zip) {
      const z = (zip ?? "").replace(/\D/g, "").slice(0, 5);
      if (z.length >= 5) {
        const { data } = await supabase.from(SUPABASE_TABLE).select("*").eq("level", "zip").eq("value", z).maybeSingle();
        const rec = data ? rowToRecord(data) : null;
        if (rec) memory.set(cacheKey("zip", z), rec);
        return rec;
      }
    }
    if (city && state) {
      const c = (city ?? "").trim();
      const s = (state ?? "").trim().toUpperCase().slice(0, 2);
      if (c && s) {
        const val = `${c},${s}`.toLowerCase().replace(/\s+/g, "-");
        const { data } = await supabase.from(SUPABASE_TABLE).select("*").eq("level", "city").eq("value", val).maybeSingle();
        const rec = data ? rowToRecord(data) : null;
        if (rec) memory.set(cacheKey("city", val), rec);
        return rec;
      }
    }
  } catch {
    // Supabase unavailable, fall through
  }
  return null;
}

/** Lookup: memory first, then Supabase (production fallback). Use this in orchestrator. */
export async function lookupAsync(zip?: string, city?: string, state?: string): Promise<CachedMarketRecord | null> {
  const fromMem = lookup(zip, city, state);
  if (fromMem) return fromMem;
  return lookupFromSupabase(zip, city, state);
}

/** Load cache from file (Node.js only). Call at startup for local dev. */
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

/** Save cache to file (Node.js only). Call after sync for local dev. */
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

const UPSERT_BATCH_SIZE = 500;

/** Save cache to Supabase (sync script). Uses service role. */
export async function saveToSupabase(): Promise<{ count: number; error?: string }> {
  if (!isSupabaseConfigured() || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
    return { count: 0, error: "Supabase not configured (NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY, SUPABASE_SERVICE_ROLE_KEY)" };
  }
  try {
    const supabase = await getSupabaseAdminClient();
    const entries = Array.from(memory.entries());
    const rows: Array<Record<string, unknown>> = [];
    for (const [key, record] of entries) {
      if (!record || isExpired(record)) continue;
      const [level, value] = key.split(":", 2);
      if (!level || !value) continue;
      rows.push({
        level,
        value,
        estimated_area_price: record.estimated_area_price ?? null,
        median_sale_price: record.median_sale_price ?? null,
        median_price_per_sqft: record.median_price_per_sqft ?? null,
        market_trend_yoy: record.market_trend_yoy ?? null,
        inventory_signal: record.inventory_signal ?? null,
        days_on_market: record.days_on_market ?? null,
        sources: record.sources,
        cached_at: new Date(record.cached_at).toISOString(),
      });
    }
    let count = 0;
    for (let i = 0; i < rows.length; i += UPSERT_BATCH_SIZE) {
      const batch = rows.slice(i, i + UPSERT_BATCH_SIZE);
      const { error } = await supabase.from(SUPABASE_TABLE).upsert(batch, { onConflict: "level,value" });
      if (error) throw error;
      count += batch.length;
    }
    return { count };
  } catch (err) {
    return { count: 0, error: err instanceof Error ? err.message : String(err) };
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
