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

type Candidate = {
  code_postal: string;
  commune: string;
  no_voie: string;
  type_de_voie: string;
  voie: string;
};

async function callApi(c: Candidate) {
  const qs = new URLSearchParams({
    countryCode: "FR",
    city: c.commune,
    street: `${c.type_de_voie} ${c.voie}`.trim(),
    houseNumber: c.no_voie ?? "",
    postcode: c.code_postal,
    apt_number: "9999",
  });
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 5000);
  const res = await fetch(`http://localhost:3000/api/property-value?${qs.toString()}`, { signal: ctrl.signal }).finally(() => clearTimeout(t));
  const json = await res.json();
  return { status: res.status, json };
}

async function main() {
  loadDotEnvLocal();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const bq = new BigQuery({ projectId });
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  const query = `
    SELECT
      TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
      TRIM(CAST(\`Commune\` AS STRING)) AS commune,
      TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
      TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
      TRIM(CAST(\`Voie\` AS STRING)) AS voie,
      COUNT(*) AS row_count
    FROM ${fullTable}
    WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
    GROUP BY code_postal, commune, no_voie, type_de_voie, voie
    HAVING row_count >= 10
    ORDER BY row_count DESC
    LIMIT 800
  `;

  const [rows] = await bq.query({ query, location });
  const candidates = (rows as Candidate[]).filter((r) => r.code_postal && r.commune && r.voie);

  let tried = 0;
  for (const c of candidates) {
    tried++;
    try {
      const { json } = await callApi(c);
      const rt = json?.fr?.resultType;
      if (rt === "nearby_comparable") {
        console.log(JSON.stringify({ tried, candidate: c, fr: json.fr, legacy: { result_level: json.result_level, apartment_not_matched: json.apartment_not_matched } }, null, 2));
        return;
      }
    } catch {
      // ignore
    }
    if (tried >= 180) break;
  }

  console.log(JSON.stringify({ message: "No nearby_comparable found in first batch.", tried }, null, 2));
}

main().catch((e) => {
  console.error("FAILED", e);
  process.exit(1);
});

