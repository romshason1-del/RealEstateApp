/**
 * TEMPORARY debug harness for NYC DOB filings (US only). Not for production or UI.
 * Run: `npx tsx src/lib/us/dob/dob-debug-test.ts` from the project root.
 */

import { dobJobFilingsResourcePath } from "./dob-config";
import { dobSocrataGet } from "./dob-client";
import { fetchDobFilingsByAddress } from "./dob-filings";

const DEBUG_HOUSE_NUMBER = "350";
const DEBUG_STREET_NAME = "W 42 STREET";

/** Unfiltered sample to inspect real field names and address columns. */
export async function runDobSchemaSampleInspection(): Promise<void> {
  const res = await dobSocrataGet<Record<string, unknown>[]>(dobJobFilingsResourcePath(), {
    $limit: "5",
  });

  if (!res.ok) {
    console.error("[dob-debug] Schema sample fetch failed:", res.error, res.status);
    return;
  }

  const data = Array.isArray(res.data) ? res.data : [];
  console.log("[dob-debug] Unfiltered sample row count:", data.length);

  const first = data[0];
  if (first && typeof first === "object") {
    console.log("[dob-debug] First row keys (sorted):", Object.keys(first).sort());
  } else {
    console.log("[dob-debug] First row keys: (no rows)");
  }

  console.log("[dob-debug] First 3 rows (raw JSON):", JSON.stringify(data.slice(0, 3), null, 2));
}

export async function runDobRoyceStreetDebugTest(): Promise<void> {
  const result = await fetchDobFilingsByAddress({
    houseNumber: DEBUG_HOUSE_NUMBER,
    streetName: DEBUG_STREET_NAME,
  });

  if (!result.success) {
    console.error("[dob-debug] Address fetch failed:", result.error, result.status);
    return;
  }

  const { rows } = result;
  console.log("[dob-debug] Address-filter row count:", rows.length);
  console.log("[dob-debug] Address-filter first 5 rows:", JSON.stringify(rows.slice(0, 5), null, 2));
}

export async function runDobDebugTest(): Promise<void> {
  console.log("[dob-debug] --- Schema sample (no $where, limit 5) ---");
  await runDobSchemaSampleInspection();
  console.log("[dob-debug] --- Address filter (350 / W 42 STREET) ---");
  await runDobRoyceStreetDebugTest();
}

void runDobDebugTest().catch((err) => {
  console.error(err);
  process.exit(1);
});
