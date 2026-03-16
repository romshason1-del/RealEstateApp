/**
 * Download DVF files (2022, 2023, 2024) from data.gouv.fr.
 * Run: npx tsx scripts/download-dvf.ts
 *
 * Files are saved as ValeursFoncieres-YYYY.txt in project root.
 */

import * as fs from "fs";
import * as path from "path";
import * as https from "https";

const API_URL = "https://www.data.gouv.fr/api/1/datasets/5c4ae55a634f4117716d5656/";
const YEARS = [2022, 2023, 2024];
const root = process.cwd();

async function fetchJson<T>(url: string): Promise<T> {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = "";
      res.on("data", (ch) => (data += ch));
      res.on("end", () => {
        try {
          resolve(JSON.parse(data) as T);
        } catch {
          reject(new Error("Invalid JSON"));
        }
      });
    }).on("error", reject);
  });
}

async function downloadFile(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(url, (res) => {
      if (res.statusCode === 302 || res.statusCode === 301) {
        const loc = res.headers.location;
        if (loc) return downloadFile(loc.startsWith("http") ? loc : `https:${loc}`, dest).then(resolve).catch(reject);
      }
      res.pipe(file);
      file.on("finish", () => {
        file.close();
        resolve();
      });
    }).on("error", (err) => {
      fs.unlink(dest, () => {});
      reject(err);
    });
  });
}

function findTxtRecursive(dir: string): string | null {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      const found = findTxtRecursive(full);
      if (found) return found;
    } else if (e.name.endsWith(".txt")) return full;
  }
  return null;
}

async function unzip(zipPath: string, outDir: string): Promise<string | null> {
  const unzipper = await import("unzipper");
  await fs.promises.mkdir(outDir, { recursive: true });
  await new Promise<void>((resolve, reject) => {
    fs.createReadStream(zipPath)
      .pipe(unzipper.Extract({ path: outDir }))
      .on("close", resolve)
      .on("error", reject);
  });
  return findTxtRecursive(outDir);
}

async function main() {
  console.log("Fetching DVF resource URLs from data.gouv.fr...");
  const dataset = await fetchJson<{ resources: Array<{ title: string; url: string }> }>(API_URL);
  const byYear = new Map<number, string>();
  for (const r of dataset.resources) {
    const m = r.title.match(/Valeurs foncières (\d{4})/i) || r.title.match(/valeursfoncieres-(\d{4})/i);
    if (m) byYear.set(parseInt(m[1], 10), r.url);
  }

  for (const year of YEARS) {
    const destTxt = path.join(root, `ValeursFoncieres-${year}.txt`);
    if (fs.existsSync(destTxt)) {
      console.log(`[SKIP] ${destTxt} exists`);
      continue;
    }

    const url = byYear.get(year);
    if (!url) {
      console.warn(`[SKIP] No URL for ${year}`);
      continue;
    }

    const zipPath = path.join(root, `ValeursFoncieres-${year}.txt.zip`);
    const extractDir = path.join(root, `.dvf-extract-${year}`);
    console.log(`Downloading ${year}...`);
    await downloadFile(url, zipPath);
    console.log(`Extracting ${year}...`);
    const extracted = await unzip(zipPath, extractDir);
    if (extracted) fs.renameSync(extracted, destTxt);
    fs.unlinkSync(zipPath);
    fs.rmSync(extractDir, { recursive: true, force: true });
    console.log(`Done: ValeursFoncieres-${year}.txt`);
  }

  console.log("\nDownload complete. Run: npm run import:france:all");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
