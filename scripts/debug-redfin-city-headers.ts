#!/usr/bin/env tsx
/** Fetch first 2 lines of Redfin city TSV */
import * as https from "https";
import * as zlib from "zlib";
import * as readline from "readline";

const url = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz";
const parsed = new URL(url);
let lineCount = 0;
const req = https.get({ hostname: parsed.hostname, path: parsed.pathname, method: "GET" }, (res) => {
  const gunzip = zlib.createGunzip();
  res.pipe(gunzip);
  const rl = readline.createInterface({ input: gunzip, crlfDelay: Infinity });
  rl.on("line", (line) => {
    lineCount++;
    console.log(`Line ${lineCount}:`, line.slice(0, 500));
    if (lineCount >= 2) process.exit(0);
  });
});
req.on("error", (e) => {
  console.error(e);
  process.exit(1);
});
