/**
 * Redfin Data Center Provider
 * Uses downloadable housing market data from Redfin's public S3 bucket.
 * Data: https://www.redfin.com/news/data-center/
 * Updated monthly (third Friday). No API key required.
 * Uses streaming for large files (1.5GB+ gzip) to avoid timeout and OOM.
 */

import * as https from "https";
import * as zlib from "zlib";
import * as readline from "readline";
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

const REDFIN_TIMEOUT_MS = 30 * 60 * 1000; // 30 min for large files

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

/** Parse TSV line (strips surrounding quotes from cells) */
function parseTSVLine(line: string): string[] {
  return line.split("\t").map((c) => c.trim().replace(/^"|"$/g, ""));
}

/** Normalize header for column lookup (lowercase, no quotes) */
function normHeader(h: string): string {
  return h.toLowerCase().replace(/^"|"$/g, "");
}

/** Stream and parse gzipped TSV from URL */
function streamGzipTsv(
  url: string,
  onHeaders: (headers: string[], status: number, contentType: string) => void,
  onRow: (row: string[], headers: string[]) => boolean
): Promise<{ totalRows: number; acceptedRows: number }> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const req = https.get(
      {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: "GET",
      },
      (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        const gunzip = zlib.createGunzip();
        res.pipe(gunzip);
        const rl = readline.createInterface({ input: gunzip, crlfDelay: Infinity });
        let headers: string[] = [];
        let isFirst = true;
        let totalRows = 0;
        let acceptedRows = 0;

        rl.on("line", (line) => {
          if (!line.trim()) return;
          if (isFirst) {
            headers = parseTSVLine(line).map((h) => normHeader(h));
            onHeaders(headers, res.statusCode ?? 0, res.headers["content-type"] ?? "");
            isFirst = false;
            return;
          }
          totalRows++;
          const row = parseTSVLine(line);
          if (onRow(row, headers)) acceptedRows++;
        });

        rl.on("close", () => resolve({ totalRows, acceptedRows }));
        rl.on("error", reject);
        gunzip.on("error", reject);
      }
    );
    req.on("error", reject);
    req.setTimeout(REDFIN_TIMEOUT_MS, () => {
      req.destroy(new Error("Download timeout"));
    });
  });
}

export type SyncResult = { count: number; error?: string; totalRows?: number; samples?: Array<Record<string, unknown>> };

/** Sync ZIP-level Redfin data into cache (streaming). */
export async function syncRedfinZipData(): Promise<SyncResult> {
  try {
    let zipIdx = -1;
    let regionTypeIdx = -1;
    let medianSaleIdx = -1;
    let medianPpsfIdx = -1;
    let domIdx = -1;
    let inventoryIdx = -1;
    let count = 0;
    const samples: Array<Record<string, unknown>> = [];

    const { totalRows } = await streamGzipTsv(
      REDFIN_ZIP_URL,
      (h) => {
        zipIdx = h.findIndex((c) => c === "zip_code" || c === "zip" || c === "region");
        regionTypeIdx = h.findIndex((c) => c === "region_type");
        medianSaleIdx = h.findIndex((c) => c === "median_sale_price");
        medianPpsfIdx = h.findIndex((c) => c === "median_sale_price_per_square_foot" || c === "median_ppsf");
        domIdx = h.findIndex((c) => c === "median_days_on_market" || c === "days_on_market" || c === "median_dom");
        inventoryIdx = h.findIndex((c) => c === "inventory" || c === "homes_for_sale");
      },
      (row) => {
        if (zipIdx < 0 || medianSaleIdx < 0) return false;
        const regionType = (row[regionTypeIdx] ?? "").toLowerCase().replace(/^"|"$/g, "");
        if (regionTypeIdx >= 0 && regionType && !/zip|region/i.test(regionType)) return false;
        const zipRaw = (row[zipIdx] ?? "").replace(/^"|"$/g, "").replace(/\D/g, "");
        const zip = zipRaw.slice(0, 5);
        if (zip.length < 5) return false;

        const medianSale = parseFloat((row[medianSaleIdx] ?? "").replace(/[^\d.]/g, ""));
        if (!Number.isFinite(medianSale) || medianSale <= 0) return false;

        const medianPpsf = medianPpsfIdx >= 0 ? parseFloat((row[medianPpsfIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;
        const dom = domIdx >= 0 ? parseFloat((row[domIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;
        const inventory = inventoryIdx >= 0 ? parseFloat((row[inventoryIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;

        const record = {
          zip,
          median_sale_price: Math.round(medianSale),
          median_price_per_sqft: medianPpsf != null && Number.isFinite(medianPpsf) && medianPpsf > 0 ? Math.round(medianPpsf) : undefined,
          days_on_market: dom != null && Number.isFinite(dom) && dom > 0 ? Math.round(dom) : undefined,
          sources: ["redfin"],
        };
        setZip(zip, {
          median_sale_price: record.median_sale_price,
          median_price_per_sqft: record.median_price_per_sqft,
          days_on_market: record.days_on_market,
          inventory_signal: inventory != null && Number.isFinite(inventory) && inventory >= 0 ? Math.round(inventory) : undefined,
          sources: ["redfin"],
        });
        if (samples.length < 5) samples.push(record);
        count++;
        return true;
      }
    );

    if (zipIdx < 0 || medianSaleIdx < 0) return { count: 0, error: "Missing required columns (zip_code, median_sale_price)" };
    return { count, totalRows, samples };
  } catch (err) {
    return { count: 0, error: err instanceof Error ? err.message : String(err) };
  }
}

/** Sync City-level Redfin data into cache (streaming). */
export async function syncRedfinCityData(): Promise<SyncResult> {
  try {
    let cityIdx = -1;
    let stateIdx = -1;
    let medianSaleIdx = -1;
    let medianPpsfIdx = -1;
    let domIdx = -1;
    let count = 0;
    const samples: Array<Record<string, unknown>> = [];

    const { totalRows } = await streamGzipTsv(
      REDFIN_CITY_URL,
      (h) => {
        cityIdx = h.findIndex((c) => c === "city");
        stateIdx = h.findIndex((c) => c === "state_code");
        if (stateIdx < 0) stateIdx = h.findIndex((c) => c === "state");
        medianSaleIdx = h.findIndex((c) => c === "median_sale_price");
        medianPpsfIdx = h.findIndex((c) => c === "median_sale_price_per_square_foot" || c === "median_ppsf");
        domIdx = h.findIndex((c) => c === "median_days_on_market" || c === "days_on_market" || c === "median_dom");
      },
      (row) => {
        if (cityIdx < 0 || stateIdx < 0 || medianSaleIdx < 0) return false;
        const city = (row[cityIdx] ?? "").replace(/^"|"$/g, "").trim();
        const state = (row[stateIdx] ?? "").replace(/^"|"$/g, "").trim().toUpperCase().slice(0, 2);
        if (!city || !state) return false;

        const medianSale = parseFloat((row[medianSaleIdx] ?? "").replace(/[^\d.]/g, ""));
        if (!Number.isFinite(medianSale) || medianSale <= 0) return false;

        const medianPpsf = medianPpsfIdx >= 0 ? parseFloat((row[medianPpsfIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;
        const dom = domIdx >= 0 ? parseFloat((row[domIdx] ?? "").replace(/[^\d.]/g, "")) : undefined;

        const record = {
          city,
          state,
          median_sale_price: Math.round(medianSale),
          median_price_per_sqft: medianPpsf != null && Number.isFinite(medianPpsf) && medianPpsf > 0 ? Math.round(medianPpsf) : undefined,
          days_on_market: dom != null && Number.isFinite(dom) && dom > 0 ? Math.round(dom) : undefined,
          sources: ["redfin"],
        };
        setCityState(city, state, {
          median_sale_price: record.median_sale_price,
          median_price_per_sqft: record.median_price_per_sqft,
          days_on_market: record.days_on_market,
          sources: ["redfin"],
        });
        if (samples.length < 5) samples.push(record);
        count++;
        return true;
      }
    );

    if (cityIdx < 0 || stateIdx < 0 || medianSaleIdx < 0)
      return { count: 0, error: "Missing required columns (city, state_code, median_sale_price)" };
    return { count, totalRows, samples };
  } catch (err) {
    return { count: 0, error: err instanceof Error ? err.message : String(err) };
  }
}
