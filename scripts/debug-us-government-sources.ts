#!/usr/bin/env tsx
/**
 * Debug Census and FHFA providers for address: 350 5th Ave, New York, NY 10118, USA
 * Run: npx tsx scripts/debug-us-government-sources.ts
 */
import { config } from "dotenv";
config({ path: ".env.local" });

const ADDRESS = "350 5th Ave, New York, NY 10118, USA";
const LAT = 40.7484;
const LNG = -73.9857;
const STATE = "NY";
const ZIP = "10118";

async function main() {
  console.log("=== US Government Sources Debug ===\n");
  console.log("Address:", ADDRESS);
  console.log("Parsed: city=New York, state=NY, zip=10118");
  console.log("Lat/Lng:", LAT, LNG);
  console.log("CENSUS_API_KEY:", process.env.CENSUS_API_KEY ? "SET" : "NOT SET");
  console.log("");

  console.log("--- 1. Census Geocoder ---");
  const geoUrl = `https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x=${LNG}&y=${LAT}&benchmark=Public_AR_Current&vintage=Current_Current&layers=14&format=json`;
  console.log("URL:", geoUrl);
  try {
    const geoRes = await fetch(geoUrl, { signal: AbortSignal.timeout(10000) });
    console.log("Status:", geoRes.status, geoRes.statusText);
    const geoData = await geoRes.json().catch(() => null);
    console.log("Response keys:", geoData ? Object.keys(geoData) : "null");
    const geos = geoData?.result?.geographies;
    let geosArr: unknown[] = [];
    if (Array.isArray(geos)) geosArr = geos;
    else if (geos && typeof geos === "object") {
      for (const v of Object.values(geos)) {
        if (Array.isArray(v) && v.length > 0) {
          geosArr = v;
          break;
        }
      }
    }
    console.log("Geographies found:", geosArr.length);
    if (geosArr.length > 0) {
      const first = geosArr[0] as Record<string, unknown>;
      console.log("First geo:", JSON.stringify(first, null, 2));
    }
  } catch (e) {
    console.error("Census geocoder error:", e);
  }

  console.log("\n--- 2. Census ACS API ---");
  const state = "36";
  const county = "061";
  const tract = "007500";
  const vars = "B25077_001E,B19013_001E,B01003_001E";
  const forClause = `tract:${tract}`;
  const inClause = `state:${state}+county:${county}`;
  let acsUrl = `https://api.census.gov/data/2022/acs/acs5?get=${vars}&for=${forClause}&in=${inClause}`;
  if (process.env.CENSUS_API_KEY) acsUrl += "&key=" + process.env.CENSUS_API_KEY;
  console.log("URL (truncated):", acsUrl.slice(0, 120) + "...");
  try {
    const acsRes = await fetch(acsUrl, { signal: AbortSignal.timeout(10000) });
    console.log("Status:", acsRes.status);
    const acsData = await acsRes.json().catch(() => null);
    console.log("Response:", Array.isArray(acsData) ? `Array[${acsData.length}]` : typeof acsData);
    if (Array.isArray(acsData) && acsData.length >= 2) {
      console.log("Header:", acsData[0]);
      console.log("Row 1:", acsData[1]);
    } else {
      console.log("Raw:", JSON.stringify(acsData)?.slice(0, 500));
    }
  } catch (e) {
    console.error("Census ACS error:", e);
  }

  console.log("\n--- 3. FHFA API ---");
  const fhfaUrl = `https://api.fhfa.gov/public/hpi?state=36`;
  console.log("URL:", fhfaUrl);
  try {
    const fhfaRes = await fetch(fhfaUrl, {
      signal: AbortSignal.timeout(10000),
      headers: { Accept: "application/json" },
    });
    console.log("Status:", fhfaRes.status);
    const fhfaData = await fhfaRes.json().catch(() => null);
    console.log("Response keys:", fhfaData && typeof fhfaData === "object" ? Object.keys(fhfaData) : "null");
    if (fhfaData) {
      const records = Array.isArray(fhfaData.data) ? fhfaData.data : Array.isArray(fhfaData.records) ? fhfaData.records : Array.isArray(fhfaData) ? fhfaData : null;
      console.log("Records count:", records?.length ?? 0);
      if (records && records.length > 0) {
        console.log("First record:", JSON.stringify(records[0]));
      }
    }
  } catch (e) {
    console.error("FHFA error:", e);
  }

  console.log("\n--- 4. fetchNeighborhoodStats (with ZCTA fallback) ---");
  try {
    const { fetchNeighborhoodStats } = await import("../src/lib/property-value-providers/us-census-provider");
    const stats = await fetchNeighborhoodStats(LAT, LNG, { zip: ZIP });
    console.log("Result:", stats);
  } catch (e) {
    console.error("fetchNeighborhoodStats error:", e);
  }

  console.log("\n--- 5. fetchMarketTrend (FRED API) ---");
  console.log("FRED_API_KEY:", process.env.FRED_API_KEY ? "SET" : "NOT SET");
  try {
    const { fetchMarketTrend } = await import("../src/lib/property-value-providers/us-fhfa-provider");
    const trend = await fetchMarketTrend({ state: STATE, zip: ZIP, latitude: LAT, longitude: LNG });
    console.log("Result:", trend);
  } catch (e) {
    console.error("fetchMarketTrend error:", e);
  }

  console.log("\n=== Done ===");
}

main().catch(console.error);
