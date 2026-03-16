/**
 * Import France DVF for 2022, 2023, and 2024 into properties_france.
 * Run: npm run import:france:all
 *
 * Place ValeursFoncieres-2022.txt, ValeursFoncieres-2023.txt, ValeursFoncieres-2024.txt
 * in the project root. Each file is processed and upserted (newest wins for same address+lot).
 *
 * Target: ~3.5M rows total across all years.
 */

import { spawn } from "child_process";
import * as path from "path";

const YEARS = [2022, 2023, 2024];
const root = process.cwd();

async function main() {
  console.log("France DVF multi-year import (2022, 2023, 2024)");
  console.log("Target: ~3.5M rows. Each year upserts into properties_france.\n");

  for (const year of YEARS) {
    const filePath = path.join(root, `ValeursFoncieres-${year}.txt`);
    const fs = await import("fs");
    if (!fs.existsSync(filePath)) {
      console.warn(`[SKIP] ${filePath} not found. Download from https://www.data.gouv.fr/fr/datasets/5c4ae55a634f4117716d5656/`);
      continue;
    }

    console.log(`\n=== Importing ${year} ===`);
    await new Promise<void>((resolve, reject) => {
      const child = spawn("npx", ["tsx", "scripts/import-france-properties.ts", filePath], {
        stdio: "inherit",
        cwd: root,
        shell: true,
      });
      child.on("close", (code) => {
        if (code === 0) resolve();
        else reject(new Error(`Import ${year} exited with code ${code}`));
      });
    });
  }

  console.log("\n=== All years imported ===");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
