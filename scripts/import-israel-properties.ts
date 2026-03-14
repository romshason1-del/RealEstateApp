/**
 * Import Israel real estate transaction data into Supabase properties_israel.
 *
 * DATA SOURCE: odata.org.il - מאגר עסקאות הנדל"ן (Real Estate Transactions Database)
 * Download: https://www.odata.org.il/dataset/84f2bc2d-87a0-474e-a3ea-63d7bb9b5447/resource/5eb859da-6236-4b67-bcd1-ec4b90875739/download/.zip
 *
 * STEPS:
 * 1. Download the ZIP from the URL above (manual or curl)
 * 2. Extract the ZIP - it contains CSV file(s) with transaction data
 * 3. Run: npx ts-node scripts/import-israel-properties.ts <path-to-csv>
 *
 * CSV columns (adapt to actual odata.org.il schema when you inspect the file):
 * - address / כתובת
 * - price / מחיר
 * - date / תאריך
 * - street / רחוב
 * - city / עיר
 * - etc.
 *
 * This script maps CSV rows to properties_israel:
 * - address: normalized address (city + street + house number)
 * - current_value: latest transaction price or computed estimate
 * - last_sale_info: "PRICE · DATE" from most recent transaction
 * - street_avg_price: average of transactions on same street
 * - neighborhood_quality: derived from price tier or left null
 */

import { createClient } from "@supabase/supabase-js";
import * as fs from "fs";
import * as path from "path";

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function normalizeAddress(parts: { city?: string; street?: string; house?: string }): string {
  const city = (parts.city ?? "").trim();
  const street = (parts.street ?? "").trim();
  const house = (parts.house ?? "").trim();
  return [street, house, city].filter(Boolean).join(", ").trim() || "unknown";
}

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      inQuotes = !inQuotes;
    } else if ((c === "," && !inQuotes) || c === "\t") {
      result.push(current.trim());
      current = "";
    } else {
      current += c;
    }
  }
  result.push(current.trim());
  return result;
}

async function main() {
  const csvPath = process.argv[2];
  if (!csvPath || !fs.existsSync(csvPath)) {
    console.error(`
Usage: npx ts-node scripts/import-israel-properties.ts <path-to-csv>

Download the Israel real estate data:
  curl -L -o israel-nadlan.zip "https://www.odata.org.il/dataset/84f2bc2d-87a0-474e-a3ea-63d7bb9b5447/resource/5eb859da-6236-4b67-bcd1-ec4b90875739/download/.zip"
  unzip israel-nadlan.zip

Then run this script with the path to the extracted CSV file.
The CSV schema may vary - inspect the file and update the column indices below.
`);
    process.exit(1);
  }

  const content = fs.readFileSync(csvPath, "utf-8");
  const lines = content.split(/\r?\n/).filter((l) => l.trim());
  const header = parseCSVLine(lines[0]);
  console.log("CSV columns:", header);

  // Map column indices - ADAPT THESE to the actual odata.org.il CSV schema
  // Common Hebrew/English column names: city=עיר, street=רחוב, house=מספר בית, price=מחיר, date=תאריך
  const idx = {
    city: header.findIndex((h) => /city|עיר|יישוב/i.test(h)),
    street: header.findIndex((h) => /street|רחוב|שם_רחוב/i.test(h)),
    house: header.findIndex((h) => /house|number|מספר|בית/i.test(h)),
    price: header.findIndex((h) => /price|מחיר|שווי/i.test(h)),
    date: header.findIndex((h) => /date|תאריך|date_sale/i.test(h)),
  };

  const addressToData = new Map<
    string,
    { current_value: number; last_sale_info: string; prices: number[] }
  >();

  for (let i = 1; i < lines.length; i++) {
    const cols = parseCSVLine(lines[i]);
    if (cols.length < 2) continue;

    const city = idx.city >= 0 ? cols[idx.city] ?? "" : "";
    const street = idx.street >= 0 ? cols[idx.street] ?? "" : "";
    const house = idx.house >= 0 ? cols[idx.house] ?? "" : "";
    const priceStr = idx.price >= 0 ? cols[idx.price] ?? "" : "";
    const dateStr = idx.date >= 0 ? cols[idx.date] ?? "" : "";

    const price = parseFloat(priceStr.replace(/[^\d.-]/g, ""));
    if (!Number.isFinite(price) || price <= 0) continue;

    const addr = normalizeAddress({ city, street, house });
    if (!addr || addr === "unknown") continue;

    const existing = addressToData.get(addr);
    const saleInfo = dateStr ? `${Math.round(price).toLocaleString()} ₪ · ${dateStr}` : `${Math.round(price).toLocaleString()} ₪`;

    if (!existing) {
      addressToData.set(addr, {
        current_value: price,
        last_sale_info: saleInfo,
        prices: [price],
      });
    } else {
      existing.prices.push(price);
      const dates = existing.last_sale_info.split(" · ");
      const existingDate = dates[1] ?? "";
      if (dateStr && (!existingDate || new Date(dateStr) > new Date(existingDate))) {
        existing.current_value = price;
        existing.last_sale_info = saleInfo;
      }
    }
  }

  const streetAverages = new Map<string, number[]>();
  for (const [addr, data] of addressToData) {
    const streetPart = addr.split(",")[0]?.trim() ?? "";
    if (streetPart) {
      const list = streetAverages.get(streetPart) ?? [];
      list.push(...data.prices);
      streetAverages.set(streetPart, list);
    }
  }

  const rows: Array<{
    address: string;
    current_value: number;
    last_sale_info: string;
    street_avg_price: number | null;
    neighborhood_quality: string | null;
  }> = [];

  for (const [addr, data] of addressToData) {
    const streetPart = addr.split(",")[0]?.trim() ?? "";
    const streetPrices = streetAverages.get(streetPart) ?? data.prices;
    const streetAvg =
      streetPrices.length > 0
        ? streetPrices.reduce((a, b) => a + b, 0) / streetPrices.length
        : null;

    rows.push({
      address: addr,
      current_value: data.current_value,
      last_sale_info: data.last_sale_info,
      street_avg_price: streetAvg,
      neighborhood_quality: null,
    });
  }

  const BATCH = 500;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const { error } = await supabase.from("properties_israel").upsert(batch, {
      onConflict: "address",
    });
    if (error) {
      console.error("Batch error:", error);
    } else {
      inserted += batch.length;
      console.log(`Upserted ${inserted}/${rows.length}`);
    }
  }

  console.log(`Done. Imported ${inserted} properties.`);
}

main().catch(console.error);
