#!/usr/bin/env tsx
/**
 * RentCast diagnostic: trace config, endpoints, and 3 direct requests.
 * Run: npx tsx scripts/diagnose-rentcast.ts
 */
import { config } from "dotenv";
config({ path: ".env.local" });

const RENTCAST_BASE = process.env.RENTCAST_API_BASE_URL || "https://api.rentcast.io/v1";
const RENTCAST_KEY = (process.env.RENTCAST_API_KEY ?? "").trim();
const PROP_US = (process.env.PROPERTY_PROVIDER_US ?? "").trim();

const TEST_ADDRESSES = [
  "123 Oak St, Springfield, IL 62701",
  "456 Maple Ave, Columbus, OH 43215",
  "789 Elm St, Kansas City, MO 64108",
];

async function traceAddress(address: string) {
  const encoded = encodeURIComponent(address);
  const urls = [
    `${RENTCAST_BASE}/avm/value?address=${encoded}`,
    `${RENTCAST_BASE}/avm/rent/long-term?address=${encoded}`,
    `${RENTCAST_BASE}/properties?address=${encoded}`,
  ];

  console.log(`\n--- Address: ${address} ---`);
  console.log("Normalized (from parse):", address);
  console.log("Geocoded: (not fetched - would need Google Geocoding)");

  for (const url of urls) {
    const endpoint = url.replace(RENTCAST_BASE, "").split("?")[0];
    console.log(`\n  Request: ${endpoint}`);
    console.log(`  Full URL: ${url.replace(RENTCAST_KEY, "***")}`);
    console.log(`  Params: address=${address}`);

    try {
      const res = await fetch(url, {
        method: "GET",
        headers: { Accept: "application/json", "X-Api-Key": RENTCAST_KEY },
        signal: AbortSignal.timeout(15000),
      });
      console.log(`  Status: ${res.status} ${res.statusText}`);

      const body = await res.json().catch(() => null);
      if (res.ok && body) {
        if (endpoint.includes("avm/value")) {
          const price = body?.price ?? body?.value;
          console.log(`  AVM price: ${price ?? "none"}`);
          console.log(`  subjectProperty: ${body?.subjectProperty ? "yes" : "no"}`);
        } else if (endpoint.includes("avm/rent")) {
          console.log(`  Rent: ${body?.rent ?? "none"}`);
        } else if (endpoint.includes("properties")) {
          const records = Array.isArray(body) ? body : body?.data ?? [];
          console.log(`  Property records: ${records.length}`);
          if (records[0]) {
            const r = records[0];
            console.log(`  lastSalePrice: ${r?.lastSalePrice ?? "none"}`);
            console.log(`  history: ${r?.history ? Object.keys(r.history).length + " entries" : "none"}`);
          }
        }
      } else {
        console.log(`  Body: ${JSON.stringify(body)?.slice(0, 200)}`);
      }
    } catch (e) {
      console.log(`  Error: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
}

async function main() {
  console.log("=== RentCast Diagnostic ===\n");

  console.log("1. RUNTIME CONFIG");
  console.log("   PROPERTY_PROVIDER_US:", PROP_US || "(unset)");
  console.log("   RENTCAST_API_KEY present:", !!RENTCAST_KEY);
  console.log("   RENTCAST_API_KEY length:", RENTCAST_KEY.length);
  console.log("   RENTCAST_API_KEY value:", RENTCAST_KEY ? `${RENTCAST_KEY.slice(0, 8)}...` : "(empty)");
  console.log("   RENTCAST_BASE_URL:", RENTCAST_BASE);

  console.log("\n2. isUSRentcastConfigured() check");
  const wouldCallRentcast = PROP_US === "rentcast" && !!RENTCAST_KEY;
  console.log("   PROP_US === 'rentcast':", PROP_US === "rentcast");
  console.log("   Would call RentCast:", wouldCallRentcast);

  console.log("\n3. ENDPOINTS");
  console.log("   AVM Value: GET /avm/value?address=<encoded>");
  console.log("   AVM Rent:  GET /avm/rent/long-term?address=<encoded>");
  console.log("   Properties: GET /properties?address=<encoded>");
  console.log("   Auth: X-Api-Key header");

  console.log("\n4. DIRECT TRACES (3 addresses)");
  for (const addr of TEST_ADDRESSES) {
    await traceAddress(addr);
  }

  console.log("\n5. ROOT CAUSE");
  if (PROP_US !== "rentcast") {
    console.log("   Orchestrator SKIPS RentCast: PROPERTY_PROVIDER_US is not 'rentcast'");
    console.log("   Current value:", JSON.stringify(PROP_US));
  }
  if (!RENTCAST_KEY || RENTCAST_KEY === "your_new_rentcast_api_key") {
    console.log("   Invalid API key: placeholder or missing");
  }
}

main().catch(console.error);
