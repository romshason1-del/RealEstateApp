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
  row_count: number;
  primary_rows: number;
  primary_with_surface: number;
};

async function callApi(c: Candidate, apt: string) {
  const qs = new URLSearchParams({
    countryCode: "FR",
    city: c.commune,
    street: `${c.type_de_voie} ${c.voie}`.trim(),
    houseNumber: c.no_voie,
    postcode: c.code_postal,
    apt_number: apt,
  });
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 4000);
  const res = await fetch(`http://localhost:3000/api/property-value?${qs.toString()}`, { signal: ctrl.signal }).finally(() => clearTimeout(t));
  const json = await res.json();
  return { status: res.status, json };
}

async function main() {
  loadDotEnvLocal();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const bq = new BigQuery({ projectId });
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  const q = `
    SELECT
      TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
      TRIM(CAST(\`Commune\` AS STRING)) AS commune,
      TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
      TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
      TRIM(CAST(\`Voie\` AS STRING)) AS voie,
      COUNT(*) AS row_count,
      COUNTIF(LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')) AS primary_rows,
      COUNTIF(LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
        AND SAFE_CAST(REPLACE(REPLACE(TRIM(CAST(\`Surface reelle bati\` AS STRING)), ' ', ''), ',', '.') AS FLOAT64) > 0
      ) AS primary_with_surface
    FROM ${fullTable}
    WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
    GROUP BY code_postal, commune, no_voie, type_de_voie, voie
    HAVING row_count >= 8
    ORDER BY row_count DESC
    LIMIT 800
  `;
  const [rows] = await bq.query({ query: q, location });
  const all = (rows as Candidate[]).filter((r) => r.code_postal && r.commune && r.voie);

  let buildingLevel: any = null;
  for (const c of all.filter((c) => c.primary_rows >= 1 && c.primary_with_surface < 2).slice(0, 60)) {
    try {
      const { json } = await callApi(c, "9999");
      if (json?.fr?.resultType === "building_level") {
        buildingLevel = { candidate: c, fr: json.fr, legacy: { result_level: json.result_level, apartment_not_matched: json.apartment_not_matched } };
        break;
      }
    } catch {}
  }

  let nearby: any = null;
  for (const c of all.filter((c) => c.primary_rows === 0).slice(0, 120)) {
    try {
      const { json } = await callApi(c, "9999");
      if (json?.fr?.resultType === "nearby_comparable") {
        nearby = { candidate: c, fr: json.fr, legacy: { result_level: json.result_level, apartment_not_matched: json.apartment_not_matched } };
        break;
      }
    } catch {}
  }

  console.log(JSON.stringify({ projectId, dataset, table, buildingLevel, nearby }, null, 2));
}

main().catch((e) => {
  console.error("FAILED", e);
  process.exit(1);
});

