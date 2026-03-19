import { BigQuery } from "@google-cloud/bigquery";
import { getBigQueryConfig } from "@/lib/bigquery-client";
import fs from "node:fs";
import path from "node:path";

function loadDotEnvLocal(): void {
  // Minimal .env loader for scripts (Next.js loads env automatically, tsx does not).
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

type SampleRow = {
  code_postal: string | null;
  commune: string | null;
  no_voie: string | null;
  type_de_voie: string | null;
  voie: string | null;
  lot_1er: string | null;
  date_mutation: string | null;
  valeur_fonciere: string | null;
  surface_reelle_bati: string | null;
  type_local: string | null;
};

function str(x: unknown): string {
  return x == null ? "" : String(x);
}

function buildDisplayAddress(r: SampleRow): string {
  const no = str(r.no_voie).trim();
  const t = str(r.type_de_voie).trim();
  const v = str(r.voie).trim();
  const street = [t, v].filter(Boolean).join(" ").trim();
  const pc = str(r.code_postal).trim();
  const com = str(r.commune).trim();
  return [no, street, pc, com, "France"].filter(Boolean).join(", ");
}

function canonicalLotCandidates(lotRaw: string): string[] {
  const raw = lotRaw.trim();
  if (!raw) return [];
  const canonicalizeSeg = (seg: string) => {
    const s = seg
      .toLowerCase()
      .trim()
      .replace(/^lot[\s.:#-]*/i, "")
      .replace(/[()]/g, "")
      .trim();
    if (!s) return "";
    const cleaned = s.replace(/[^0-9a-z]/gi, "");
    if (!cleaned) return "";
    if (/^\d+$/.test(cleaned)) return cleaned.replace(/^0+/, "") || "0";
    const m = cleaned.match(/^0+(\d.*)$/);
    return m ? m[1] : cleaned;
  };
  const parts = raw.split(/[\/\\-]/g).map(canonicalizeSeg).filter(Boolean);
  const collapsed = canonicalizeSeg(raw);
  const all = [...parts, ...(collapsed ? [collapsed] : [])];
  return all.filter((v, i) => all.indexOf(v) === i);
}

async function main() {
  loadDotEnvLocal();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const bq = new BigQuery({ projectId });
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  // Targeted candidates for each resultType.
  const qSimilarBuildings = `
    WITH b AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        COUNT(*) AS row_count,
        COUNTIF(LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
          AND SAFE_CAST(REPLACE(REPLACE(TRIM(CAST(\`Surface reelle bati\` AS STRING)), ' ', ''), ',', '.') AS FLOAT64) > 0
        ) AS primary_with_surface
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
      GROUP BY code_postal, commune, no_voie, type_de_voie, voie
      HAVING primary_with_surface >= 2 AND row_count >= 5
      ORDER BY primary_with_surface DESC, row_count DESC
      LIMIT 30
    )
    SELECT * FROM b
  `;

  const qBuildingLevelBuildings = `
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
      HAVING primary_rows >= 1 AND primary_with_surface < 2 AND row_count >= 5
      ORDER BY row_count DESC
      LIMIT 30
    )
    SELECT * FROM b
  `;

  const qNearbyOnlyBuildings = `
    WITH b AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        COUNT(*) AS row_count,
        COUNTIF(LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')) AS primary_rows
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
      GROUP BY code_postal, commune, no_voie, type_de_voie, voie
      HAVING primary_rows = 0 AND row_count >= 3
      ORDER BY row_count DESC
      LIMIT 60
    ),
    street_primary AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        UPPER(TRIM(CONCAT(COALESCE(TRIM(CAST(\`Type de voie\` AS STRING)), ''), ' ', COALESCE(TRIM(CAST(\`Voie\` AS STRING)), '')))) AS street_key,
        COUNTIF(LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')) AS primary_rows_street
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
      GROUP BY code_postal, street_key
    )
    SELECT b.*
    FROM b
    JOIN street_primary s
      ON s.code_postal = b.code_postal
     AND s.street_key = UPPER(TRIM(CONCAT(COALESCE(b.type_de_voie,''), ' ', COALESCE(b.voie,''))))
    WHERE s.primary_rows_street >= 1
    LIMIT 30
  `;

  const qExactBuildings = `
    WITH b AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        COUNT(*) AS row_count
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
        AND LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
        AND TRIM(CAST(\`1er lot\` AS STRING)) IS NOT NULL
        AND TRIM(CAST(\`1er lot\` AS STRING)) != ''
      GROUP BY code_postal, commune, no_voie, type_de_voie, voie
      HAVING row_count >= 3
      ORDER BY row_count DESC
      LIMIT 30
    )
    SELECT * FROM b
  `;

  const [similarBuildings] = await bq.query({ query: qSimilarBuildings, location });
  const [buildingLevelBuildings] = await bq.query({ query: qBuildingLevelBuildings, location });
  const [nearbyOnlyBuildings] = await bq.query({ query: qNearbyOnlyBuildings, location });
  const [exactBuildings] = await bq.query({ query: qExactBuildings, location });
  console.log(JSON.stringify({
    candidateCounts: {
      similarBuildings: (similarBuildings as any[]).length,
      buildingLevelBuildings: (buildingLevelBuildings as any[]).length,
      nearbyOnlyBuildings: (nearbyOnlyBuildings as any[]).length,
      exactBuildings: (exactBuildings as any[]).length,
    }
  }));

  // Helper to fetch concrete rows for a building.
  async function fetchRowsForBuilding(b: any, limit = 25): Promise<SampleRow[]> {
    const query = `
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        TRIM(CAST(\`1er lot\` AS STRING)) AS lot_1er,
        TRIM(CAST(\`Date mutation\` AS STRING)) AS date_mutation,
        TRIM(CAST(\`Valeur fonciere\` AS STRING)) AS valeur_fonciere,
        TRIM(CAST(\`Surface reelle bati\` AS STRING)) AS surface_reelle_bati,
        TRIM(CAST(\`Type local\` AS STRING)) AS type_local
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
        AND TRIM(CAST(\`Code postal\` AS STRING)) = @pc
        AND LOWER(TRIM(CAST(\`Commune\` AS STRING))) = LOWER(@commune)
        AND (
          TRIM(CAST(\`No voie\` AS STRING)) = @noVoie
          OR (@noVoie = '' AND (TRIM(CAST(\`No voie\` AS STRING)) = '' OR \`No voie\` IS NULL))
        )
        AND UPPER(TRIM(CONCAT(COALESCE(TRIM(CAST(\`Type de voie\` AS STRING)), ''), ' ', COALESCE(TRIM(CAST(\`Voie\` AS STRING)), '')))) =
            UPPER(TRIM(CONCAT(COALESCE(@typeDeVoie, ''), ' ', COALESCE(@voie, ''))))
      ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', CAST(\`Date mutation\` AS STRING)) DESC NULLS LAST
      LIMIT ${limit}
    `;
    const params = {
      pc: String(b.code_postal ?? "").trim(),
      commune: String(b.commune ?? "").trim(),
      noVoie: String(b.no_voie ?? "").trim(),
      typeDeVoie: String(b.type_de_voie ?? "").trim(),
      voie: String(b.voie ?? "").trim(),
    };
    const [rows] = await bq.query({ query, params, location });
    return (rows as SampleRow[]) || [];
  }

  const samples: any[] = [];

  // 1) 2 similar_apartment_same_building expected
  for (const b of (similarBuildings as any[]).slice(0, 2)) {
    const rows = await fetchRowsForBuilding(b, 50);
    // pick a primary row with surface if possible (helps UX)
    const picked = rows.find((r) => /appartement|maison/i.test(str(r.type_local)) && (parseFloat(str(r.surface_reelle_bati)) || 0) > 0) ?? rows[0];
    if (picked) samples.push({ kind: "similar_apartment_same_building", building: b, row: picked, suggestedNonExistingLot: "9999" });
  }
  // 2) 1 nearby_comparable expected
  if ((nearbyOnlyBuildings as any[]).length > 0) {
    for (const b of (nearbyOnlyBuildings as any[]).slice(0, 10)) {
      const rows = await fetchRowsForBuilding(b, 50);
      const picked = rows.find((r) => {
        const v = str(r.valeur_fonciere).trim();
        return v !== "" && v !== "0";
      });
      if (!picked) continue;
      samples.push({ kind: "nearby_comparable", building: b, row: picked, suggestedNonExistingLot: "9999" });
      break;
    }
  }
  // 3) 1 building_level expected
  if ((buildingLevelBuildings as any[]).length > 0) {
    const b = (buildingLevelBuildings as any[])[0];
    const rows = await fetchRowsForBuilding(b, 80);
    // pick a primary row even if surface missing (this is why it should be building_level, not similar)
    const picked = rows.find((r) => /appartement|maison/i.test(str(r.type_local))) ?? rows[0];
    if (picked) samples.push({ kind: "building_level", building: b, row: picked, suggestedNonExistingLot: "9999" });
  }
  // 4) 1 exact_apartment guaranteed
  if ((exactBuildings as any[]).length > 0) {
    const b = (exactBuildings as any[])[0];
    const rows = await fetchRowsForBuilding(b);
    const withLot = rows.find((r) => str(r.lot_1er).trim()) ?? rows[0];
    const lot = str(withLot.lot_1er).trim();
    if (withLot) samples.push({ kind: "exact_apartment", building: b, row: withLot, suggestedLots: lot ? canonicalLotCandidates(lot) : [] });
  }

  // Print as JSON for copy/paste into validation docs.
  const out = samples.map((s, idx) => {
    const r: SampleRow = s.row;
    return {
      i: idx + 1,
      kind: s.kind,
      fullMatchedAddress: buildDisplayAddress(r),
      postalCode: str(r.code_postal) || null,
      commune: str(r.commune) || null,
      lotRaw: str(r.lot_1er) || null,
      lotSuggestedInputs: s.suggestedLots ?? null,
      suggestedNonExistingLot: s.suggestedNonExistingLot ?? null,
      transactionDate: str(r.date_mutation) || null,
      transactionValue: str(r.valeur_fonciere) || null,
      surfaceArea: str(r.surface_reelle_bati) || null,
      typeLocal: str(r.type_local) || null,
    };
  });

  console.log(JSON.stringify({ projectId, dataset, table, count: out.length, samples: out }, null, 2));
}

main().catch((e) => {
  console.error("FAILED", e);
  process.exit(1);
});

