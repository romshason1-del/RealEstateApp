/**
 * TEMPORARY debug harness for NYC DOB building insights (US only). Not for production or UI.
 * Run: `npx tsx src/lib/us/dob/dob-truth-debug-test.ts` from the project root.
 */

import { fetchDobNycBuildingInsights } from "./dob-truth";

const CASES: ReadonlyArray<{ houseNumber: string; streetName: string; label: string }> = [
  { houseNumber: "350", streetName: "W 42 STREET", label: "350 W 42 STREET" },
  { houseNumber: "2154", streetName: "ROYCE STREET", label: "2154 ROYCE STREET" },
];

export async function runDobTruthDebugTest(): Promise<void> {
  for (const c of CASES) {
    console.log(`[dob-truth-debug] --- ${c.label} ---`);
    const r = await fetchDobNycBuildingInsights({
      houseNumber: c.houseNumber,
      streetName: c.streetName,
    });

    if (!r.success) {
      console.log("[dob-truth-debug] error:", r.error, r.status);
      continue;
    }

    console.log("[dob-truth-debug] address_key:", r.address_key);
    console.log("[dob-truth-debug] has_filings:", r.has_filings);
    console.log("[dob-truth-debug] filing_count:", r.filing_count);
    console.log("[dob-truth-debug] building_type:", r.building_type);
    console.log("[dob-truth-debug] existing_units:", r.existing_units);
    console.log("[dob-truth-debug] proposed_units:", r.proposed_units);
  }
}

void runDobTruthDebugTest().catch((err) => {
  console.error(err);
  process.exit(1);
});
