/**
 * QA script: France, UK, Italy API isolation check
 * Run: npx tsx scripts/qa-france-uk-italy.ts
 * Requires: dev server on BASE (default localhost:3000)
 */

const QA_BASE_URL = process.env.QA_BASE ?? "http://localhost:3000";

async function fetchAPI(params: Record<string, string>): Promise<unknown> {
  const q = new URLSearchParams(params).toString();
  const res = await fetch(`${QA_BASE_URL}/api/property-value?${q}`, {
    signal: AbortSignal.timeout(25000),
  });
  return res.json();
}

function extract4Lines(data: unknown): string[] {
  const d = data as Record<string, unknown>;
  const pr = d?.property_result as Record<string, unknown> | undefined;
  if (!pr) return ["(no property_result)"];
  const exact = pr.exact_value ?? pr.exact_value_message ?? "—";
  const lastTx = pr.last_transaction as { amount?: number; date?: string | null; message?: string } | undefined;
  const lastTxStr =
    lastTx?.amount && lastTx.amount > 0
      ? `${lastTx.amount}${lastTx.date ? ` · ${lastTx.date}` : ""}`
      : lastTx?.message ?? "—";
  const streetAvg = pr.street_average ?? pr.street_average_message ?? "—";
  const livability = pr.livability_rating ?? "—";
  return [
    `1. exact_value: ${exact}`,
    `2. last_transaction: ${lastTxStr}`,
    `3. street_average: ${streetAvg}`,
    `4. livability_rating: ${livability}`,
  ];
}

async function main() {
  console.log("QA: France, UK, Italy isolation check\nBase:", QA_BASE_URL, "\n");

  // France (need lat/lng)
  const frTests = [
    {
      name: "Rue de Rivoli 10, Paris, France",
      params: {
        address: "Rue de Rivoli 10, Paris, France",
        countryCode: "FR",
        latitude: "48.8606",
        longitude: "2.3376",
      },
    },
    {
      name: "Avenue des Champs-Élysées 50, Paris, France",
      params: {
        address: "Avenue des Champs-Élysées 50, Paris, France",
        countryCode: "FR",
        latitude: "48.8702",
        longitude: "2.3076",
      },
    },
    {
      name: "Rue Paradis 20, Marseille, France",
      params: {
        address: "Rue Paradis 20, Marseille, France",
        countryCode: "FR",
        latitude: "43.2965",
        longitude: "5.3698",
      },
    },
  ];

  console.log("=== FRANCE (3 addresses) ===\n");
  for (const t of frTests) {
    try {
      const data = await fetchAPI(t.params);
      const err = (data as { error?: string; message?: string }).error ?? (data as { message?: string }).message;
      if (err && (data as { error?: string }).error) {
        console.log(t.name, "\n  Error:", err, "\n");
        continue;
      }
      const lines = extract4Lines(data);
      console.log(t.name);
      lines.forEach((l) => console.log("  ", l));
      const fr = (data as { fr_dvf?: { transaction_count?: number; radius_used_m?: number } }).fr_dvf;
      if (fr) console.log("  ", `[fr_dvf: ${fr.transaction_count} tx, radius ${fr.radius_used_m}m]`);
      console.log();
    } catch (e) {
      console.log(t.name, "\n  Fetch error:", e instanceof Error ? e.message : String(e), "\n");
    }
  }

  // UK
  console.log("=== UK ===\n");
  try {
    const ukData = await fetchAPI({
      address: "Flat 3, 37 Bedford Gardens, London W8 7EF",
      countryCode: "UK",
    });
    const lines = extract4Lines(ukData);
    console.log("Flat 3, 37 Bedford Gardens, London W8 7EF");
    lines.forEach((l) => console.log("  ", l));
    const uk = (ukData as { uk_land_registry?: unknown }).uk_land_registry;
    console.log("  ", uk ? "[uk_land_registry present]" : "[no uk_land_registry]");
    console.log();
  } catch (e) {
    console.log("UK fetch error:", e instanceof Error ? e.message : String(e), "\n");
  }

  // Italy
  console.log("=== ITALY ===\n");
  try {
    const itData = await fetchAPI({
      address: "Via del Corso 10, Roma, Italia",
      countryCode: "IT",
      latitude: "41.9028",
      longitude: "12.4964",
    });
    const lines = extract4Lines(itData);
    console.log("Via del Corso 10, Roma, Italia");
    lines.forEach((l) => console.log("  ", l));
    const it = (itData as { it_omi?: unknown }).it_omi;
    console.log("  ", it ? "[it_omi present]" : "[no it_omi]");
    console.log();
  } catch (e) {
    console.log("Italy fetch error:", e instanceof Error ? e.message : String(e), "\n");
  }

  console.log("=== DONE ===");
}

main().catch(console.error);
