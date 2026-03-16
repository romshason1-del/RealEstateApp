/**
 * Run migration: 20250320000000_search_france_properties.sql
 * Requires: DATABASE_URL or SUPABASE_DB_URL in .env.local
 * Get from: Supabase Dashboard > Project Settings > Database > Connection string (URI)
 */
import * as path from "path";
import * as fs from "fs";

async function main() {
  try {
    const dotenv = await import("dotenv");
    dotenv.config({ path: path.join(process.cwd(), ".env.local") });
  } catch {}

  const dbUrl = process.env.DATABASE_URL ?? process.env.SUPABASE_DB_URL ?? "";
  if (!dbUrl) {
    console.error(`
DATABASE_URL not found in .env.local.

Add to .env.local:
  DATABASE_URL=postgresql://postgres.emvawjbceigceaabsrhz:[YOUR-DB-PASSWORD]@aws-1-eu-west-1.pooler.supabase.com:5432/postgres

Get the database password from: Supabase Dashboard > Project Settings > Database > Connection string
`);
    process.exit(1);
  }

  const sqlPath = path.join(process.cwd(), "supabase", "migrations", "20250320000000_search_france_properties.sql");
  const sql = fs.readFileSync(sqlPath, "utf8");

  const { Client } = await import("pg");
  const client = new Client({ connectionString: dbUrl });

  try {
    await client.connect();
    console.log("Connected. Running migration 20250320000000_search_france_properties.sql...");
    await client.query(sql);
    console.log("Migration applied successfully.");
  } catch (err) {
    console.error("Migration failed:", err);
    process.exit(1);
  } finally {
    await client.end();
  }
}

main();
