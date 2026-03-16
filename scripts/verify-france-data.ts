/**
 * Verify properties_france has data with lot_number.
 * Run: npx tsx scripts/verify-france-data.ts
 */
import { createClient } from "@supabase/supabase-js";
import * as path from "path";

async function main() {
  try {
    const dotenv = await import("dotenv");
    dotenv.config({ path: path.join(process.cwd(), ".env.local") });
  } catch {}

  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
  if (!url || !key) {
    console.error("Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env.local");
    process.exit(1);
  }

  const supabase = createClient(url, key);

  const { count: totalCount } = await supabase
    .from("properties_france")
    .select("*", { count: "exact", head: true });
  console.log("Total rows:", totalCount ?? "error");

  const { count: countAnglais } = await supabase
    .from("properties_france")
    .select("*", { count: "exact", head: true })
    .ilike("address", "%ANGLAIS%");
  console.log("Rows with ANGLAIS in address:", countAnglais ?? "error");

  const { data: anglaisRows, error: errAnglais } = await supabase
    .from("properties_france")
    .select("address, current_value")
    .ilike("address", "%ANGLAIS%")
    .limit(5);
  if (errAnglais) console.log("Error:", errAnglais.message);
  if (anglaisRows && anglaisRows.length > 0) {
    console.log("Sample ANGLAIS:", anglaisRows.map((r) => ({ address: r.address, value: r.current_value })));
  }

  let countWithLot = 0;
  try {
    const { data: withLot } = await supabase
      .from("properties_france")
      .select("address, lot_number")
      .ilike("address", "%ANGLAIS%")
      .limit(300);
    countWithLot = withLot?.filter((r) => (r as { lot_number?: string }).lot_number !== "").length ?? 0;
  } catch {
    console.log("lot_number column missing - run supabase/apply_france_schema.sql in Supabase SQL Editor");
  }
  console.log("Rows with ANGLAIS and non-empty lot_number:", countWithLot);

  const { data: sample15 } = await supabase
    .from("properties_france")
    .select("address, current_value, lot_number, code_postal")
    .ilike("address", "%15%")
    .ilike("address", "%ANGLAIS%")
    .limit(5);
  console.log("Sample 15+ANGLAIS:", JSON.stringify(sample15, null, 2));

  const { data: sampleAnglais } = await supabase
    .from("properties_france")
    .select("address, current_value, lot_number")
    .ilike("address", "%ANGLAIS%")
    .limit(3);
  console.log("Sample ANGLAIS only:", JSON.stringify(sampleAnglais, null, 2));
}

main().catch(console.error);
