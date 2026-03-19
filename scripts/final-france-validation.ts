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

type Building = {
  code_postal: string;
  commune: string;
  no_voie: string;
  type_de_voie: string;
  voie: string;
  any_lot: string | null;
  primary_with_surface: number;
  primary_rows: number;
  row_count: number;
};

async function callApi(params: Record<string, string>) {
  const qs = new URLSearchParams({ countryCode: "FR", ...params });
  const res = await fetch(`http://localhost:3000/api/property-value?${qs.toString()}`);
  const json = await res.json();
  return { status: res.status, json };
}

async function main() {
  loadDotEnvLocal();
  process.env.FR_FAST_VALIDATE = "1";

  const { projectId, dataset, table, location } = getBigQueryConfig();
  const bq = new BigQuery({ projectId });
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  const q = `
    WITH b AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        ANY_VALUE(TRIM(CAST(\`1er lot\` AS STRING))) AS any_lot,
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
      LIMIT 600
    )
    SELECT * FROM b
  `;

  const [rows] = await bq.query({ query: q, location });
  const buildings = (rows as Building[]).filter((r) => r.code_postal && r.commune && r.voie);

  const results: any[] = [];

  async function findByResultType(target: string, predicate: (b: Building) => boolean, apt: string, maxTries = 1200) {
    let tries = 0;
    for (const b of buildings) {
      if (!predicate(b)) continue;
      tries++;
      const params = {
        city: b.commune,
        street: `${b.type_de_voie} ${b.voie}`.trim(),
        houseNumber: b.no_voie,
        postcode: b.code_postal,
        apt_number: apt,
      };
      const { json } = await callApi(params);
      const rt = json?.fr?.resultType;
      if (rt === target) {
        results.push({ target, params, fr: json.fr, legacy: { result_level: json.result_level, apartment_not_matched: json.apartment_not_matched } });
        return true;
      }
      if (tries >= maxTries) break;
    }
    return false;
  }

  // exact_apartment: require a lot
  for (const b of buildings) {
    if (!b.any_lot || !b.any_lot.trim()) continue;
    const params = {
      city: b.commune,
      street: `${b.type_de_voie} ${b.voie}`.trim(),
      houseNumber: b.no_voie,
      postcode: b.code_postal,
      apt_number: b.any_lot.trim(),
    };
    const { json } = await callApi(params);
    if (json?.fr?.resultType === "exact_apartment") {
      results.push({ target: "exact_apartment", params, fr: json.fr, legacy: { result_level: json.result_level, apartment_not_matched: json.apartment_not_matched } });
      break;
    }
  }

  // 2 similar_same_building (>=2 primary_with_surface helps)
  let similarFound = 0;
  for (const b of buildings) {
    if (similarFound >= 2) break;
    if (!(b.primary_with_surface >= 2)) continue;
    const params = {
      city: b.commune,
      street: `${b.type_de_voie} ${b.voie}`.trim(),
      houseNumber: b.no_voie,
      postcode: b.code_postal,
      apt_number: "9999",
    };
    const { json } = await callApi(params);
    if (json?.fr?.resultType === "similar_apartment_same_building") {
      results.push({ target: "similar_apartment_same_building", params, fr: json.fr, legacy: { result_level: json.result_level, apartment_not_matched: json.apartment_not_matched } });
      similarFound++;
    }
  }

  // building_level: primary_rows>=1 but primary_with_surface<2 (and lot not found)
  await findByResultType(
    "building_level",
    (b) => b.primary_rows >= 1 && b.primary_with_surface < 2,
    "9999",
    1200
  );

  // nearby_comparable: try many; relies on our backend choosing that type
  await findByResultType(
    "nearby_comparable",
    (b) => b.primary_rows === 0,
    "9999",
    2000
  );

  // no_result: deterministic nonsense
  {
    const params = { address: "1 Rue Inexistante, 99999 Nowhere, France", postcode: "99999", apt_number: "9999" };
    const { json } = await callApi(params);
    results.push({ target: "no_result", params, fr: json?.fr ?? null, legacy: { result_level: json?.result_level, apartment_not_matched: json?.apartment_not_matched } });
  }

  console.log(JSON.stringify({ projectId, dataset, table, foundCount: results.length, results }, null, 2));
}

main().catch((e) => {
  console.error("FAILED", e);
  process.exit(1);
});

