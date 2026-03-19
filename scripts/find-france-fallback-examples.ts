import { BigQuery } from "@google-cloud/bigquery";
import fs from "node:fs";
import path from "node:path";
import { getBigQueryConfig } from "@/lib/bigquery-client";

function loadDotEnvLocal(): void {
  const p = path.resolve(process.cwd(), ".env.local");
  if (!fs.existsSync(p)) return;
  const content = fs.readFileSync(p, "utf8");
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq <= 0) continue;
    const key = trimmed.slice(0, eq).trim();
    const value = trimmed.slice(eq + 1).trim();
    if (process.env[key] === undefined) process.env[key] = value;
  }
}

type BuildingKey = {
  code_postal: string;
  commune: string;
  no_voie: string;
  type_de_voie: string;
  voie: string;
  any_lot: string | null;
};

function toAddressString(b: BuildingKey): string {
  const no = (b.no_voie ?? "").trim();
  const street = `${(b.type_de_voie ?? "").trim()} ${(b.voie ?? "").trim()}`.trim();
  const pc = (b.code_postal ?? "").trim();
  const com = (b.commune ?? "").trim();
  // Use a user-like formatting that our FR parser handles well.
  return `${no} ${street}, ${pc} ${com}, France`.replace(/\s+/g, " ").trim();
}

async function callApi(address: string, postcode: string, aptNumber?: string) {
  const qs = new URLSearchParams({ countryCode: "FR", address, postcode });
  if (aptNumber) qs.set("apt_number", aptNumber);
  const res = await fetch(`http://localhost:3000/api/property-value?${qs.toString()}`);
  const json = await res.json();
  return { status: res.status, json };
}

async function main() {
  loadDotEnvLocal();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const bq = new BigQuery({ projectId });
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  const query = `
    WITH base AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        ANY_VALUE(TRIM(CAST(\`1er lot\` AS STRING))) AS any_lot,
        COUNT(*) AS row_count
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
      GROUP BY code_postal, commune, no_voie, type_de_voie, voie
      HAVING row_count >= 8
      ORDER BY row_count DESC
      LIMIT 300
    )
    SELECT * FROM base
  `;

  const [rows] = await bq.query({ query, location });
  const buildings = (rows as BuildingKey[]).filter((r) => r.code_postal && r.commune && r.voie);

  const wanted = new Set([
    "exact_apartment",
    "similar_apartment_same_building",
    "building_level",
    "nearby_comparable",
  ]);
  const found: Record<string, any> = {};

  // 1) Find exact_apartment using a real lot when present.
  for (const b of buildings) {
    if (!b.any_lot || !b.any_lot.trim()) continue;
    const address = toAddressString(b);
    const { json } = await callApi(address, b.code_postal, b.any_lot.trim());
    const rt = json?.fr?.resultType;
    if (rt === "exact_apartment") {
      found.exact_apartment = { address, postcode: b.code_postal, commune: b.commune, lot: b.any_lot.trim(), response: json.fr };
      wanted.delete("exact_apartment");
      break;
    }
  }

  // 2) Find each fallback type by probing apt_number=9999.
  for (const b of buildings) {
    if (wanted.size === 0) break;
    const address = toAddressString(b);
    const { json } = await callApi(address, b.code_postal, "9999");
    const rt = json?.fr?.resultType;
    if (wanted.has(rt)) {
      found[rt] = { address, postcode: b.code_postal, commune: b.commune, lot: "9999", response: json.fr };
      wanted.delete(rt);
    }
  }

  // 3) Always include a deterministic no_result example (nonsense address).
  const noRes = await callApi("1 Rue Inexistante, 99999 Nowhere, France", "99999", "9999");
  found.no_result = { address: "1 Rue Inexistante, 99999 Nowhere, France", postcode: "99999", commune: "Nowhere", lot: "9999", response: noRes.json?.fr ?? null };

  console.log(JSON.stringify({ projectId, dataset, table, found }, null, 2));
}

main().catch((e) => {
  console.error("FAILED", e);
  process.exit(1);
});

