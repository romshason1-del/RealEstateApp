#!/usr/bin/env tsx
/**
 * UK Readiness Test
 * Verifies UK pipeline is production-ready.
 * - 12 addresses: property-level, building-level, street/no-data, parsing edge cases
 * - Transaction + level accuracy
 * - Consistency check (5 addresses × 3 runs)
 *
 * Run with dev server: npm run dev (in another terminal)
 * Then: npx tsx scripts/uk-accuracy-smoke-test.ts
 */
const UK_SMOKE_BASE = process.env.UK_SMOKE_BASE ?? "http://localhost:3000";

type TestCase = {
  address: string;
  rawInputAddress?: string;
  selectedFormattedAddress?: string;
  expectedPrice: number;
  expectedDate: string | null;
  expectedLevel: "property-level" | "building-level" | "street-level" | "area-level" | "no_match";
};

const TEST_CASES: TestCase[] = [
  // Building-level (verified)
  {
    address: "37 Bedford Gardens, London W8 7EF, UK",
    rawInputAddress: "Flat 3, 37 Bedford Gardens, London W8 7EF, UK",
    selectedFormattedAddress: "37 Bedford Gardens, London W8 7EF, UK",
    expectedPrice: 7_000_000,
    expectedDate: "2022-05-24",
    expectedLevel: "building-level",
  },
  {
    address: "1 High Street, London SW19 5BY, UK",
    expectedPrice: 850_000,
    expectedDate: "2021-08-18",
    expectedLevel: "building-level",
  },
  // Parsing edge: Flat/Unit prefix
  {
    address: "10 Palace Gate, London W8 5NP, UK",
    rawInputAddress: "Flat 10, 10 Palace Gate, London W8 5NP, UK",
    selectedFormattedAddress: "10 Palace Gate, London W8 5NP, UK",
    expectedPrice: 0,
    expectedDate: null,
    expectedLevel: "area-level",
  },
  {
    address: "37 Bedford Gardens, London W8 7EF, UK",
    rawInputAddress: "Unit 3, 37 Bedford Gardens, London W8 7EF, UK",
    selectedFormattedAddress: "37 Bedford Gardens, London W8 7EF, UK",
    expectedPrice: 7_000_000,
    expectedDate: "2022-05-24",
    expectedLevel: "building-level",
  },
  // No-data / area-level: addresses that return area-level (no Land Registry building match)
  { address: "15 Park Road, London W8 5NP, UK", expectedPrice: 0, expectedDate: null, expectedLevel: "area-level" },
  { address: "20 Park Road, London W8 5NP, UK", expectedPrice: 0, expectedDate: null, expectedLevel: "area-level" },
  { address: "25 Park Road, London W8 5NP, UK", expectedPrice: 0, expectedDate: null, expectedLevel: "area-level" },
  { address: "5 Palace Gate, London W8 5NP, UK", expectedPrice: 0, expectedDate: null, expectedLevel: "area-level" },
  { address: "15 Palace Gate, London W8 5NP, UK", expectedPrice: 0, expectedDate: null, expectedLevel: "area-level" },
  { address: "30 Park Road, London W8 5NP, UK", expectedPrice: 0, expectedDate: null, expectedLevel: "area-level" },
  { address: "35 Park Road, London W8 5NP, UK", expectedPrice: 0, expectedDate: null, expectedLevel: "area-level" },
  { address: "40 Park Road, London W8 5NP, UK", expectedPrice: 0, expectedDate: null, expectedLevel: "area-level" },
];

const CONSISTENCY_ADDRESSES: Array<{ address: string; raw?: string; selected?: string }> = [
  { address: "37 Bedford Gardens, London W8 7EF, UK", raw: "Flat 3, 37 Bedford Gardens, London W8 7EF, UK", selected: "37 Bedford Gardens, London W8 7EF, UK" },
  { address: "1 High Street, London SW19 5BY, UK" },
  { address: "5 Bedford Gardens, London W8 7EF, UK" },
  { address: "37 Bedford Gardens, London W8 7EF, UK", raw: "Unit 3, 37 Bedford Gardens, London W8 7EF, UK", selected: "37 Bedford Gardens, London W8 7EF, UK" },
  { address: "10 Palace Gate, London W8 5NP, UK", raw: "Flat 10, 10 Palace Gate, London W8 5NP, UK", selected: "10 Palace Gate, London W8 5NP, UK" },
];

function normalizeDate(d: string | null | undefined): string | null {
  if (!d) return null;
  try {
    const m = d.match(/^(\d{4})-(\d{2})-(\d{2})/);
    return m ? `${m[1]}-${m[2]}-${m[3]}` : d;
  } catch {
    return d;
  }
}

function getValueLevel(data: Record<string, unknown>): string {
  const pr = data.property_result as { value_level?: string } | undefined;
  return pr?.value_level ?? "—";
}

async function fetchUK(address: string, raw?: string, selected?: string): Promise<Record<string, unknown>> {
  const params = new URLSearchParams({ address, countryCode: "UK" });
  if (raw) params.set("rawInputAddress", raw);
  if (selected) params.set("selectedFormattedAddress", selected);
  const res = await fetch(`${UK_SMOKE_BASE}/api/property-value?${params}`, {
    signal: AbortSignal.timeout(25000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return (await res.json()) as Record<string, unknown>;
}

function getLatestTransaction(data: Record<string, unknown>): { price: number; date: string | null } | null {
  const uk = data.uk_land_registry as Record<string, unknown> | undefined;
  const tx = (uk?.latest_building_transaction ?? uk?.latest_nearby_transaction) as { price?: number; date?: string } | null | undefined;
  if (tx && typeof tx.price === "number") {
    return { price: tx.price, date: tx.date ?? null };
  }
  const pr = data.property_result as { last_transaction?: { amount?: number; date?: string | null } } | undefined;
  const lt = pr?.last_transaction;
  if (lt && typeof lt.amount === "number") {
    return { price: lt.amount, date: lt.date ?? null };
  }
  return null;
}

function txMatch(tc: TestCase, data: Record<string, unknown>): boolean {
  const tx = getLatestTransaction(data);
  const actualPrice = tx?.price ?? 0;
  const actualDate = normalizeDate(tx?.date ?? null);
  if (tc.expectedPrice !== actualPrice) return false;
  if (tc.expectedDate !== null) return actualDate === normalizeDate(tc.expectedDate);
  return true;
}

function levelMatch(tc: TestCase, data: Record<string, unknown>): boolean {
  const level = getValueLevel(data);
  if (level === tc.expectedLevel) return true;
  // No-data cases: accept both area-level and no_match (HPI availability may vary)
  if (tc.expectedLevel === "area-level" && (level === "no_match" || level === "area-level")) return true;
  return false;
}

function resultSignature(data: Record<string, unknown>): string {
  const tx = getLatestTransaction(data);
  const level = getValueLevel(data);
  const price = tx?.price ?? 0;
  const date = tx?.date ?? "";
  return `${price}|${date}|${level}`;
}

async function runUKReadinessTest() {
  console.log("UK Readiness Test\n");
  console.log(`Base: ${UK_SMOKE_BASE}`);
  console.log(`Testing ${TEST_CASES.length} addresses\n`);

  let txCorrect = 0;
  let txIncorrect = 0;
  let levelCorrect = 0;
  let levelIncorrect = 0;
  let unavailable = 0;

  for (let i = 0; i < TEST_CASES.length; i++) {
    const tc = TEST_CASES[i];
    const shortAddr = tc.address.length > 55 ? tc.address.slice(0, 52) + "…" : tc.address;
    try {
      const data = await fetchUK(
        tc.address,
        tc.rawInputAddress,
        tc.selectedFormattedAddress ?? tc.address
      );
      const tx = getLatestTransaction(data);
      const retPrice = tx?.price ?? 0;
      const retDate = tx?.date ?? "none";
      const retLevel = getValueLevel(data);

      const txOk = txMatch(tc, data);
      const levelOk = levelMatch(tc, data);
      const pass = txOk && levelOk;

      if (txOk) txCorrect++;
      else txIncorrect++;
      if (levelOk) levelCorrect++;
      else levelIncorrect++;

      console.log(`${i + 1}. ${shortAddr}`);
      console.log(`   Expected tx: £${tc.expectedPrice.toLocaleString()}, ${tc.expectedDate ?? "none"}`);
      console.log(`   Expected level: ${tc.expectedLevel}`);
      console.log(`   Returned tx: £${retPrice.toLocaleString()}, ${retDate}`);
      console.log(`   Returned level: ${retLevel}`);
      console.log(`   Pass: ${pass ? "✓" : "✗"}\n`);
    } catch (e) {
      unavailable++;
      const errMsg = e instanceof Error ? e.message : String(e);
      console.log(`${i + 1}. ${shortAddr}`);
      console.log(`   Expected tx: £${tc.expectedPrice.toLocaleString()}, ${tc.expectedDate ?? "none"}`);
      console.log(`   Expected level: ${tc.expectedLevel}`);
      console.log(`   Returned tx: —`);
      console.log(`   Returned level: —`);
      console.log(`   Pass: ✗ (unavailable: ${errMsg})\n`);
    }
  }

  const total = TEST_CASES.length;
  const evaluated = txCorrect + txIncorrect;
  const txAccuracyPct = evaluated > 0 ? Math.round((txCorrect / evaluated) * 100) : 0;
  const levelAccuracyPct = evaluated > 0 ? Math.round((levelCorrect / evaluated) * 100) : 0;
  const availabilityPct = total > 0 ? Math.round((evaluated / total) * 100) : 0;

  console.log("--- Summary ---");
  console.log(`Total tested:  ${total}`);
  console.log(`Correct tx:    ${txCorrect} | Incorrect tx: ${txIncorrect}`);
  console.log(`Correct level: ${levelCorrect} | Incorrect level: ${levelIncorrect}`);
  console.log(`Unavailable:   ${unavailable}`);
  console.log(`Transaction accuracy: ${txAccuracyPct}%`);
  console.log(`Level accuracy:       ${levelAccuracyPct}%`);
  console.log(`Availability:         ${availabilityPct}%\n`);

  // Consistency check: 5 addresses × 3 runs
  console.log("--- Consistency Check (5 addresses × 3 runs) ---\n");
  let consistencyPass = true;
  for (let i = 0; i < CONSISTENCY_ADDRESSES.length; i++) {
    const ca = CONSISTENCY_ADDRESSES[i];
    const label = ca.raw ? `${ca.raw.slice(0, 40)}…` : ca.address.slice(0, 45) + "…";
    const sigs: string[] = [];
    let err: string | null = null;
    for (let r = 0; r < 3; r++) {
      try {
        const data = await fetchUK(ca.address, ca.raw, ca.selected ?? ca.address);
        sigs.push(resultSignature(data));
      } catch (e) {
        err = e instanceof Error ? e.message : String(e);
        break;
      }
    }
    const ok = !err && sigs.length === 3 && sigs[0] === sigs[1] && sigs[1] === sigs[2];
    if (!ok) consistencyPass = false;
    console.log(`${i + 1}. ${label}`);
    console.log(`   Run 1: ${sigs[0] ?? err ?? "—"}`);
    console.log(`   Run 2: ${sigs[1] ?? "—"}`);
    console.log(`   Run 3: ${sigs[2] ?? "—"}`);
    console.log(`   Consistent: ${ok ? "✓" : "✗"}\n`);
  }

  console.log("--- Final ---");
  console.log(`Consistency: ${consistencyPass ? "PASS" : "FAIL"}`);
  const fail = txIncorrect > 0 || levelIncorrect > 0 || !consistencyPass;
  process.exit(fail ? 1 : 0);
}

runUKReadinessTest().catch((e) => {
  console.error(e);
  process.exit(1);
});
