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
  const t = setTimeout(() => ctrl.abort(), 12000);
  const res = await fetch(`http://localhost:3000/api/property-value?${qs.toString()}`, { signal: ctrl.signal }).finally(() => clearTimeout(t));
  const json = await res.json();
  return { status: res.status, json };
}

async function main() {
  loadDotEnvLocal();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const bq = new BigQuery({ projectId });
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  // Find "weak building" candidates (aligned to current code):
  // buildingStrong requires: >=2 building sales, avg > 0, at least one dated sale, at least one sale with surface.
  //
  // After staged-nearby changes, we don't need to pre-prove street-only comps. We'll just find buildings
  // where the building itself is likely "weak" under our definition and let the API try staged nearby.
  const query = `
    WITH building AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        COUNT(*) AS row_count,
        COUNTIF(LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')) AS primary_rows,
        COUNTIF(
          LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
          AND SAFE_CAST(REPLACE(REPLACE(TRIM(CAST(\`Surface reelle bati\` AS STRING)), ' ', ''), ',', '.') AS FLOAT64) > 0
        ) AS primary_with_surface,
        COUNTIF(
          LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
          AND SAFE.PARSE_DATE('%d/%m/%Y', CAST(\`Date mutation\` AS STRING)) IS NOT NULL
        ) AS primary_with_date
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
      GROUP BY code_postal, commune, no_voie, type_de_voie, voie
       HAVING primary_rows = 1 AND row_count >= 6
    )
    SELECT b.code_postal, b.commune, b.no_voie, b.type_de_voie, b.voie, b.row_count
    FROM building b
    ORDER BY b.row_count DESC
    LIMIT 120
  `;

  const [rows] = await bq.query({ query, location });
  const candidates = rows as Candidate[];
  console.log(JSON.stringify({ candidateCount: candidates.length }));

  for (const [idx, c] of candidates.slice(0, 12).entries()) {
    try {
      console.log(JSON.stringify({ trying: idx + 1, code_postal: c.code_postal, commune: c.commune, no_voie: c.no_voie, type_de_voie: c.type_de_voie, voie: c.voie }));
      const { json } = await callApi(c);
      console.log(JSON.stringify({
        resultType: json?.fr?.resultType,
        buildingRejectedAsWeak: json?.fr?.debug?.buildingRejectedAsWeak,
        candidateCountNearby: json?.fr?.debug?.candidateCountNearby,
        nearbyFilterStats: json?.fr?.debug?.nearbyFilterStats,
      }));
      if (json?.fr?.resultType === "nearby_comparable" && json?.fr?.debug?.buildingRejectedAsWeak === true) {
        console.log(JSON.stringify({ candidate: c, fr: json.fr, legacy: { result_level: json.result_level, apartment_not_matched: json.apartment_not_matched } }, null, 2));
        return;
      }
      if (json?.fr?.debug?.nearbyStageCounts) {
        console.log(JSON.stringify({ nearbyStageCounts: json.fr.debug.nearbyStageCounts, selectedNearbyStrategy: json.fr.debug.selectedNearbyStrategy, comparableScope: json.fr.debug.comparableScope }, null, 2));
      }
    } catch {
      // continue
    }
  }

  console.log(JSON.stringify({ message: "No nearby_comparable case found under current thresholds in scanned candidates.", scanned: candidates.length }, null, 2));
}

main().catch((e) => {
  console.error("FAILED", e);
  process.exit(1);
});

