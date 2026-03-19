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

async function callApi(c: Candidate, apt: string) {
  const qs = new URLSearchParams({
    countryCode: "FR",
    city: c.commune,
    street: `${c.type_de_voie} ${c.voie}`.trim(),
    houseNumber: c.no_voie ?? "",
    postcode: c.code_postal,
    apt_number: apt,
  });
  const res = await fetch(`http://localhost:3000/api/property-value?${qs.toString()}`);
  const json = await res.json();
  return { status: res.status, json };
}

async function main() {
  loadDotEnvLocal();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const bq = new BigQuery({ projectId });
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  // Candidate A: building-level expected
  // - building has "appartement/maison" sales (so buildingSales exists)
  // - but all those sales have surface <= 0 or null (so similar is blocked by guardrails)
  const qBuildingLevel = `
    WITH b AS (
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
      HAVING primary_rows >= 3 AND primary_with_surface = 0 AND row_count >= 8
      ORDER BY row_count DESC
      LIMIT 20
    )
    SELECT code_postal, commune, no_voie, type_de_voie, voie FROM b
  `;

  // Candidate B: nearby_comparable expected
  // - building match has zero primary rows (so buildingSales empty/avg 0)
  // - same street+postcode has primary rows elsewhere (so nearbyComparable query can succeed)
  const qNearbyComparable = `
    WITH building_no_primary AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        COUNT(*) AS row_count,
        COUNTIF(LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison','')) AS primary_rows
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
      GROUP BY code_postal, commune, no_voie, type_de_voie, voie
      HAVING primary_rows = 0 AND row_count >= 6
    ),
    street_primary AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        UPPER(TRIM(CONCAT(COALESCE(TRIM(CAST(\`Type de voie\` AS STRING)), ''), ' ', COALESCE(TRIM(CAST(\`Voie\` AS STRING)), '')))) AS street_key,
        COUNTIF(LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
          AND SAFE_CAST(REPLACE(REPLACE(TRIM(CAST(\`Surface reelle bati\` AS STRING)), ' ', ''), ',', '.') AS FLOAT64) > 0
        ) AS primary_with_surface_street
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
      GROUP BY code_postal, street_key
      HAVING primary_with_surface_street >= 2
    )
    SELECT b.code_postal, b.commune, b.no_voie, b.type_de_voie, b.voie
    FROM building_no_primary b
    JOIN street_primary s
      ON s.code_postal = b.code_postal
     AND s.street_key = UPPER(TRIM(CONCAT(COALESCE(b.type_de_voie,''), ' ', COALESCE(b.voie,''))))
    ORDER BY b.row_count DESC
    LIMIT 30
  `;

  const [blRows] = await bq.query({ query: qBuildingLevel, location });
  const [nbRows] = await bq.query({ query: qNearbyComparable, location });
  const blCand = (blRows as Candidate[])[0];
  const nbCand = (nbRows as Candidate[])[0];

  if (!blCand) throw new Error("No building_level candidate found by query.");
  if (!nbCand) throw new Error("No nearby_comparable candidate found by query.");

  const buildingLevel = await callApi(blCand, "9999");
  const nearbyComparable = await callApi(nbCand, "9999");

  console.log(JSON.stringify({
    projectId, dataset, table,
    building_level_candidate: { candidate: blCand, status: buildingLevel.status, fr: buildingLevel.json?.fr, legacy: { result_level: buildingLevel.json?.result_level, apartment_not_matched: buildingLevel.json?.apartment_not_matched } },
    nearby_comparable_candidate: { candidate: nbCand, status: nearbyComparable.status, fr: nearbyComparable.json?.fr, legacy: { result_level: nearbyComparable.json?.result_level, apartment_not_matched: nearbyComparable.json?.apartment_not_matched } },
  }, null, 2));
}

main().catch((e) => {
  console.error("FAILED", e);
  process.exit(1);
});

