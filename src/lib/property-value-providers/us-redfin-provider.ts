/**
 * Redfin Data Center Provider
 * Uses downloadable housing market data from Redfin's public S3 bucket.
 * Data: https://www.redfin.com/news/data-center/
 * Updated monthly (third Friday). No API key required.
 */

import {
  getByZip,
  getByCityState,
  setZip,
  setCityState,
  type CachedMarketRecord,
} from "./us-market-data-cache";

const REDFIN_ZIP_URL =
  "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz";
const REDFIN_CITY_URL =
  "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz";

/** Get cached Redfin market data. Returns null if not in cache. */
export function getRedfinMarketData(zip?: string, city?: string, state?: string): CachedMarketRecord | null {
  if (zip) {
    const r = getByZip(zip);
    if (r && r.sources.includes("redfin")) return r;
  }
  if (city && state) {
    const r = getByCityState(city, state);
    if (r && r.sources.includes("redfin")) return r;
  }
  return null;
}

/** Decompress gzip buffer (Node.js) */
async function gunzip(buf: ArrayBuffer): Promise<string> {
  const zlib = await import("zlib");
  const { promisify } = await import("util");
  const gunzipAsync = promisify(zlib.gunzip);
  const decompressed = (await gunzipAsync(Buffer.from(buf))) as Buffer;
  return decompressed.toString("utf-8");
}

/** Parse TSV line */
function parseTSVLine(line: string): string[] {
  return line.split("\t").map((c) => c.trim());
}

/** Sync ZIP-level Redfin data into cache. */
export async function syncRedfinZipData(): Promise<{ count: number; error?: string }> {
  try {
    const res = await fetch(REDFIN_ZIP_URL, { signal: AbortSignal.timeout(90000) });
    if (!res.ok) return { count: 0, error: `HTTP ${res.status}` };
    const buf = await res.arrayBuffer();
    const text = await gunzip(buf);
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (lines.length < 2) return { count: 0, error: "Empty or invalid TSV" };

    const headers = parseTSVLine(lines[0]!).map((h) => h.toLowerCase());
    const zipIdx = headers.findIndex((h) => h === "zip_code" || h === "zip");
    const medianSaleIdx = headers.findIndex((h) => h === "median_sale_price");
    const medianPpsfIdx = headers.findIndex((h) => h === "median_sale_price_per_square_foot" || h === "median_ppsf");
    const domIdx = headers.findIndex((h) => h === "median_days_on_market" || h === "days_on_market");
    const inventoryIdx = headers.findIndex((h) => h === "inventory" || h === "homes_for_sale");

    if (zipIdx < 0 || medianSaleIdx < 0) return { count: 0, error: "Missing required columns" };

    let count = 0;
    for (let i = 1; i < Math.min(lines.length, 40000); i++) {
      const row = parseTSVLine(lines[i]!);
      const zipRaw = (row[zipIdx] ?? "").replace(/\D/g, "");
      const zip = zipRaw.slice(0, 5);
      if (zip.length < 5) continue;

      const medianSale = parseFloat((row[medianSaleIdx] ?? "").replace(/[^\d.]/g, ""));
      if (!Number.isFinite(medianSale) || medianSale <= 0) continue;

      const medianPpsf = medianPpsfIdx >= 0 ? parseFloat((row[medianPpsfIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;
      const dom = domIdx >= 0 ? parseFloat((row[domIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;
      const inventory = inventoryIdx >= 0 ? parseFloat((row[inventoryIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;

      setZip(zip, {
        median_sale_price: Math.round(medianSale),
        median_price_per_sqft: medianPpsf != null && Number.isFinite(medianPpsf) && medianPpsf > 0 ? Math.round(medianPpsf) : undefined,
        days_on_market: dom != null && Number.isFinite(dom) && dom > 0 ? Math.round(dom) : undefined,
        inventory_signal: inventory != null && Number.isFinite(inventory) && inventory >= 0 ? Math.round(inventory) : undefined,
        sources: ["redfin"],
      });
      count++;
    }
    return { count };
  } catch (err) {
    return { count: 0, error: err instanceof Error ? err.message : "Unknown error" };
  }
}

/** Sync City-level Redfin data into cache. */
export async function syncRedfinCityData(): Promise<{ count: number; error?: string }> {
  try {
    const res = await fetch(REDFIN_CITY_URL, { signal: AbortSignal.timeout(90000) });
    if (!res.ok) return { count: 0, error: `HTTP ${res.status}` };
    const buf = await res.arrayBuffer();
    const text = await gunzip(buf);
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (lines.length < 2) return { count: 0, error: "Empty or invalid TSV" };

    const headers = parseTSVLine(lines[0]!).map((h) => h.toLowerCase());
    const cityIdx = headers.findIndex((h) => h === "city");
    const stateIdx = headers.findIndex((h) => h === "state_code" || h === "state");
    const medianSaleIdx = headers.findIndex((h) => h === "median_sale_price");
    const medianPpsfIdx = headers.findIndex((h) => h === "median_sale_price_per_square_foot" || h === "median_ppsf");
    const domIdx = headers.findIndex((h) => h === "median_days_on_market" || h === "days_on_market");

    if (cityIdx < 0 || stateIdx < 0 || medianSaleIdx < 0) return { count: 0, error: "Missing required columns" };

    let count = 0;
    for (let i = 1; i < Math.min(lines.length, 25000); i++) {
      const row = parseTSVLine(lines[i]!);
      const city = (row[cityIdx] ?? "").trim();
      const state = (row[stateIdx] ?? "").trim().toUpperCase().slice(0, 2);
      if (!city || !state) continue;

      const medianSale = parseFloat((row[medianSaleIdx] ?? "").replace(/[^\d.]/g, ""));
      if (!Number.isFinite(medianSale) || medianSale <= 0) continue;

      const medianPpsf = medianPpsfIdx >= 0 ? parseFloat((row[medianPpsfIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;
      const dom = domIdx >= 0 ? parseFloat((row[domIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;

      setCityState(city, state, {
        median_sale_price: Math.round(medianSale),
        median_price_per_sqft: medianPpsf != null && Number.isFinite(medianPpsf) && medianPpsf > 0 ? Math.round(medianPpsf) : undefined,
        days_on_market: dom != null && Number.isFinite(dom) && dom > 0 ? Math.round(dom) : undefined,
        sources: ["redfin"],
      });
      count++;
    }
    return { count };
  } catch (err) {
    return { count: 0, error: err instanceof Error ? err.message : "Unknown error" };
  }
}
