import { getUSBigQueryClient } from "../../../src/lib/us/bigquery-client";
import { getNycAppOutputTableReference } from "../../../src/lib/us/us-nyc-app-output-constants";

async function main() {
  const c = getUSBigQueryClient();
  const t = getNycAppOutputTableReference();
  const n = (col: string) => `UPPER(TRIM(REGEXP_REPLACE(COALESCE(\`${col}\`, ''), r'\\s+', ' ')))`;

  const [exact285] = await c.query({
    query: `
      SELECT lookup_address, property_address FROM \`${t}\`
      WHERE ${n("lookup_address")} = '285 FULTON STREET' OR ${n("property_address")} = '285 FULTON STREET'
      LIMIT 3`,
    location: "US",
  });
  const [exact350] = await c.query({
    query: `
      SELECT lookup_address, property_address FROM \`${t}\`
      WHERE ${n("lookup_address")} IN ('350 5TH AVENUE', '350 FIFTH AVENUE')
         OR ${n("property_address")} IN ('350 5TH AVENUE', '350 FIFTH AVENUE')
      LIMIT 5`,
    location: "US",
  });
  console.log("Exact norm 285 FULTON STREET:", JSON.stringify(exact285, null, 2));
  console.log("Exact norm 350 5TH/FIFTH AVENUE:", JSON.stringify(exact350, null, 2));
}

main().catch(console.error);
