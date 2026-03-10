#!/usr/bin/env tsx
/** Fetch first line (header) of Redfin TSV to inspect column names */
import * as https from "https";
import * as zlib from "zlib";
import * as readline from "readline";

const REDFIN_ZIP_URL =
  "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz";

const parsed = new URL(REDFIN_ZIP_URL);
const req = https.get(
  { hostname: parsed.hostname, path: parsed.pathname, method: "GET" },
  (res) => {
    console.log("Status:", res.statusCode);
    console.log("Content-Type:", res.headers["content-type"]);
    const gunzip = zlib.createGunzip();
    res.pipe(gunzip);
    const rl = readline.createInterface({ input: gunzip, crlfDelay: Infinity });
    rl.once("line", (line) => {
      console.log("Header line:", line);
      const cols = line.split("\t").map((c, i) => `${i}:${c}`);
      console.log("Columns:", cols.slice(0, 20).join(" | "));
      console.log("All:", cols.length);
      process.exit(0);
    });
  }
);
req.on("error", (e) => {
  console.error(e);
  process.exit(1);
});
