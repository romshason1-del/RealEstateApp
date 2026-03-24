/**
 * Runtime validation of France baseline addresses.
 * Run: npx tsx scripts/validate-france-baseline.ts
 * Requires: npm run dev (or next dev) running on port 3000
 */

const BASE = "http://localhost:3000";
const FETCH_TIMEOUT_MS = 90000; // 90s per address
const BASELINE = [
  {
    label: "1) 25 Avenue du Golf, 64200 Biarritz",
    address: "25 Avenue du Golf, 64200 Biarritz",
    postcode: "64200",
    aptNumber: "",
    expected: {
      estimatedValueDisplay: "14,908 €/m²",
      lastTransaction: "none",
      basedOn: "Based on recent sales on this street",
    },
  },
  {
    label: "2) 6 Chemin du Vallon, 06400 Cannes",
    address: "6 Chemin du Vallon, 06400 Cannes",
    postcode: "06400",
    aptNumber: "",
    expected: {
      estimatedValueDisplay: "255,000 €",
      lastTransaction: "255,000 € • 3 Sept 2024",
      basedOn: "Based on recent sales in this building",
    },
  },
  {
    label: "3) 12 Allée des Pins, 83120 Sainte-Maxime",
    address: "12 Allée des Pins, 83120 Sainte-Maxime",
    postcode: "83120",
    aptNumber: "",
    expected: {
      estimatedValueDisplay: "5,617 €/m²",
      lastTransaction: "none",
      basedOn: "Based on recent sales on this street",
    },
  },
  {
    label: "4) 18 Chemin des Oliviers, 06600 Antibes",
    address: "18 Chemin des Oliviers, 06600 Antibes",
    postcode: "06600",
    aptNumber: "",
    expected: {
      estimatedValueDisplay: "245,000 €",
      lastTransaction: "245,000 € • 11 Oct 2024",
      source: "270, CHEMIN DES OLIVIERS, 06600 ANTIBES",
      basedOn: "Based on recent sales data",
    },
  },
  {
    label: "5) 7 Rue des Vignes, 92500 Rueil-Malmaison",
    address: "7 Rue des Vignes, 92500 Rueil-Malmaison",
    postcode: "92500",
    aptNumber: "",
    expected: {
      estimatedValueDisplay: "5,608 €/m²",
      lastTransaction: "none",
      basedOn: "Based on similar properties in the area",
    },
  },
  {
    label: "6) 10 Rue de Turenne, 75004 Paris",
    address: "10 Rue de Turenne, 75004 Paris",
    postcode: "75004",
    aptNumber: "",
    expected: {
      estimatedValueDisplay: "12,596 €/m²",
      lastTransaction: "none",
      basedOn: "Based on similar properties in the area",
    },
  },
  {
    label: "7) 24 Rue du Faubourg Saint-Martin, 75010 Paris",
    address: "24 Rue du Faubourg Saint-Martin, 75010 Paris",
    postcode: "75010",
    aptNumber: "",
    expected: {
      estimatedValueDisplay: "9,411 €/m²",
      lastTransaction: "none",
      basedOn: "Based on similar properties in the area",
    },
  },
  {
    label: "8) 5 Rue des Capucins, 69001 Lyon (apt 17)",
    address: "5 Rue des Capucins, 69001 Lyon",
    postcode: "69001",
    aptNumber: "17",
    expected: {
      estimatedValueDisplay: "400,000 €",
      lastTransaction: "400,000 € • 14 Apr 2022",
      basedOn: "Based on recent sales in this building",
    },
  },
  {
    label: "9) 12 Rue Paradis, 13001 Marseille (apt 4)",
    address: "12 Rue Paradis, 13001 Marseille",
    postcode: "13001",
    aptNumber: "4",
    expected: {
      estimatedValueDisplay: "130,000 €",
      lastTransaction: "130,000 € • 8 Feb 2022",
      basedOn: "Based on recent sales in this building",
    },
  },
  {
    label: "10) 8 Rue Fondaudège, 33000 Bordeaux",
    address: "8 Rue Fondaudège, 33000 Bordeaux",
    postcode: "33000",
    aptNumber: "",
    expected: {
      estimatedValueDisplay: "4,865 €/m²",
      lastTransaction: "none",
      basedOn: "Based on recent sales on this street",
    },
  },
  {
    label: "11) 83 Rue du Redon, 13009 Marseille (apt 38)",
    address: "83 Rue du Redon, 13009 Marseille",
    postcode: "13009",
    aptNumber: "38",
    expected: {
      estimatedValueDisplay: "168,000 €",
      lastTransaction: "168,000 € • 28 Aug 2024",
      basedOn: "Based on recent sales in this building",
    },
  },
];

function norm(s: string | null | undefined): string {
  if (s == null) return "";
  return String(s).trim();
}

function formatEuro(n: number): string {
  return new Intl.NumberFormat("fr-FR", { style: "currency", currency: "EUR", maximumFractionDigits: 0 }).format(n);
}

function extractFromResponse(data: any): {
  estimatedValueDisplay: string;
  lastTransaction: string;
  source: string;
  basedOn: string;
} {
  const pr = data?.property_result ?? {};
  const fr = data?.fr ?? {};
  const fv = data?.fr_valuation_display ?? {};
  const rd = data?.fr_runtime_debug ?? {};
  const ev = data?.estimated_value ?? fv?.estimated_value ?? pr?.exact_value ?? data?.display_value ?? fv?.display_value;
  const ppm = data?.price_per_m2 ?? fv?.price_per_m2 ?? pr?.street_average;
  const dispType = fv?.display_value_type ?? data?.display_value_type;
  const hasEv = ev != null && typeof ev === "number" && ev > 0;
  const hasPpm = ppm != null && typeof ppm === "number" && ppm > 0;
  let estimatedValueDisplay = "";
  if (dispType === "estimated_total" && hasEv) {
    estimatedValueDisplay = formatEuro(ev);
  } else if (hasPpm) {
    estimatedValueDisplay = formatEuro(ppm) + "/m²";
  } else if (hasEv) {
    estimatedValueDisplay = formatEuro(ev);
  }
  const lt = pr?.last_transaction ?? {};
  const amt = lt?.amount ?? fr?.property?.transactionValue ?? fv?.last_sale_price;
  const d = lt?.date ?? fr?.property?.transactionDate ?? fv?.last_sale_date;
  let lastTransaction = "none";
  if (amt != null && typeof amt === "number" && amt > 0) {
    const fmt = formatEuro(amt);
    lastTransaction = d != null && String(d).trim() ? `${fmt} • ${String(d).trim()}` : fmt;
  }
  const source = lt?.source_address ?? fv?.last_transaction_source_address ?? "";
  const ws = rd?.winning_step ?? fv?.winning_step ?? "";
  const streetMsg = pr?.street_average_message ?? "";
  let basedOn = "";
  if (streetMsg && String(streetMsg).trim()) basedOn = String(streetMsg).trim();
  else if (String(ws) === "street_fallback") basedOn = "Based on recent sales on this street";
  else if (String(ws) === "building_similar_unit" || String(ws) === "building_level") basedOn = "Based on recent sales in this building";
  else if (String(ws) === "commune_fallback" || String(ws) === "nearby_fallback") basedOn = "Based on similar properties in the area";
  else if (fr?.matchExplanation) basedOn = String(fr.matchExplanation).trim();
  else if (fv?.winning_source_label) basedOn = String(fv.winning_source_label).trim();
  else basedOn = "Based on recent sales data";
  return { estimatedValueDisplay, lastTransaction, source: norm(source), basedOn: norm(basedOn) };
}

async function main() {
  console.log("France baseline validation – runtime check\n");
  for (const b of BASELINE) {
    try {
      const params = new URLSearchParams({
        address: b.address,
        countryCode: "FR",
        postcode: b.postcode,
      });
      if (b.aptNumber) params.set("apt_number", b.aptNumber);
      const url = `${BASE}/api/property-value?${params}`;
      const ctrl = new AbortController();
      const to = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
      const res = await fetch(url, { signal: ctrl.signal });
      clearTimeout(to);
      if (!res.ok) {
        console.log(`${b.label}\n  MATCHES BASELINE: NO\n  Error: HTTP ${res.status}\n`);
        continue;
      }
      const data = await res.json();
      const actual = extractFromResponse(data);
      const exp = b.expected as Record<string, string>;
      const diffs: string[] = [];
      const normalizeNum = (s: string) => s.replace(/[\s,]/g, "");
      const expEst = normalizeNum(norm(exp.estimatedValueDisplay ?? ""));
      const actEst = normalizeNum(norm(actual.estimatedValueDisplay));
      if (expEst && actEst !== expEst) {
        diffs.push(`estimated: expected "${exp.estimatedValueDisplay}" got "${actual.estimatedValueDisplay}"`);
      }
      const expLast = (exp.lastTransaction ?? "").trim();
      const actLast = actual.lastTransaction.trim();
      if (expLast.toLowerCase() === "none") {
        if (actLast.toLowerCase() !== "none") diffs.push(`last tx: expected none got "${actual.lastTransaction}"`);
      } else {
        const expAmt = expLast.split(" • ")[0]?.trim() ?? "";
        if (!actLast.includes(expAmt.replace(/\s/g, "\u00a0"))) {
          const actAmt = actLast.split(" • ")[0]?.trim() ?? "";
          if (norm(actAmt) !== norm(expAmt)) diffs.push(`last tx: expected "${exp.lastTransaction}" got "${actual.lastTransaction}"`);
        }
      }
      if (exp.source && norm(actual.source) !== norm(exp.source)) {
        diffs.push(`source: expected "${exp.source}" got "${actual.source}"`);
      }
      const expBased = norm(exp.basedOn ?? "");
      const actBased = norm(actual.basedOn);
      if (expBased && actBased !== expBased) {
        diffs.push(`based on: expected "${exp.basedOn}" got "${actual.basedOn}"`);
      }
      const matches = diffs.length === 0;
      console.log(`${b.label}`);
      console.log(`  MATCHES BASELINE: ${matches ? "YES" : "NO"}`);
      if (!matches) for (const d of diffs) console.log(`  - ${d}`);
      console.log("");
    } catch (e) {
      console.log(`${b.label}\n  MATCHES BASELINE: NO\n  Error: ${(e as Error).message}\n`);
    }
  }
}

main().catch(console.error);
