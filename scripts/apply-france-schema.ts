/**
 * Apply lot_number schema to properties_france.
 * Requires: DATABASE_URL in .env.local (from Supabase Dashboard > Project Settings > Database > Connection string)
 * Run: npx tsx scripts/apply-france-schema.ts
 */
import * as path from "path";

async function main() {
  try {
    const dotenv = await import("dotenv");
    dotenv.config({ path: path.join(process.cwd(), ".env.local") });
  } catch {}

  const dbUrl = process.env.DATABASE_URL ?? process.env.SUPABASE_DB_URL ?? "";
  if (!dbUrl) {
    console.error(`
Set DATABASE_URL in .env.local.

Get it from: Supabase Dashboard > Project Settings > Database > Connection string (URI)
Example: postgresql://postgres.[ref]:[YOUR-PASSWORD]@aws-0-[region].pooler.supabase.com:6543/postgres
`);
    process.exit(1);
  }

  const { Client } = await import("pg");
  const client = new Client({ connectionString: dbUrl });

  try {
    await client.connect();
    console.log("Connected. Applying schema...");

    await client.query(`
      ALTER TABLE public.properties_france ADD COLUMN IF NOT EXISTS lot_number text DEFAULT '';
      UPDATE public.properties_france SET lot_number = '' WHERE lot_number IS NULL;
      ALTER TABLE public.properties_france ALTER COLUMN lot_number SET DEFAULT '';
      ALTER TABLE public.properties_france ALTER COLUMN lot_number SET NOT NULL;
    `);
    console.log("Added lot_number column.");

    await client.query(`ALTER TABLE public.properties_france DROP CONSTRAINT IF EXISTS properties_france_pkey;`);
    await client.query(`ALTER TABLE public.properties_france ADD PRIMARY KEY (address, lot_number);`);
    console.log("Updated primary key to (address, lot_number).");

    console.log("Schema applied. Run: npm run import:france");
  } catch (err) {
    console.error("Error:", err);
    process.exit(1);
  } finally {
    await client.end();
  }
}

main();
