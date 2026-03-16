/** Quick check: SELECT address, lot_number, current_value FROM properties_france WHERE address ILIKE '%ANGLAIS%' LIMIT 5 */
import { createClient } from "@supabase/supabase-js";
import * as path from "path";
(async () => {
  try {
    const dotenv = await import("dotenv");
    dotenv.config({ path: path.join(process.cwd(), ".env.local") });
  } catch {}
  const supabase = createClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
  );
  const { data, error } = await supabase
    .from("properties_france")
    .select("address, lot_number, current_value")
    .ilike("address", "%ANGLAIS%")
    .limit(5);
  console.log("ANGLAIS LIMIT 5:", JSON.stringify({ data, error: error?.message }, null, 2));
  const { data: nice } = await supabase
    .from("properties_france")
    .select("address, lot_number, current_value, code_postal")
    .ilike("address", "%ANGLAIS%")
    .or("code_postal.eq.6000,code_postal.eq.6200,code_postal.eq.06000")
    .limit(10);
  console.log("\nANGLAIS + Nice (6000/6200/06000):", JSON.stringify(nice, null, 2));
})();
