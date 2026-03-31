/**
 * Deterministic NYC address normalization for matching precomputed card `full_address` (US only).
 * Not fuzzy — fixed rules only.
 */

import { preserveQueensInAddressLineIfUserTypedQueens } from "./us-nyc-preserve-queens";

/** Bump when candidate rules change (client cache key + API debug). */
export const NYC_CANDIDATE_GENERATOR_VERSION = 7;

const NYC_BOROUGHS_AND_CITY = new Set([
  "BROOKLYN",
  "MANHATTAN",
  "QUEENS",
  "BRONX",
  "STATEN ISLAND",
  "NEW YORK",
  "NYC",
  "THE BRONX",
]);

/** Longer / multi-token keys first where relevant. */
const STREET_ABBREV: readonly [RegExp, string][] = [
  [/\bPKWY\.?\b/g, "PARKWAY"],
  [/\bBLVD\.?\b/g, "BOULEVARD"],
  [/\bAVE\.?\b/g, "AVENUE"],
  [/\bAPT\.?\b/g, "APARTMENT"],
  [/\bSTE\.?\b/g, "SUITE"],
  [/\bTER\.?\b/g, "TERRACE"],
  [/\bPL\.?\b/g, "PLACE"],
  [/\bCT\.?\b/g, "COURT"],
  [/\bLN\.?\b/g, "LANE"],
  [/\bRD\.?\b/g, "ROAD"],
  [/\bDR\.?\b/g, "DRIVE"],
  [/\bST\.?\b/g, "STREET"],
];

/** Suffix for building-only fallback (after abbrev expansion). */
const UNIT_SUFFIX_RE =
  /\s+(?:(?:APARTMENT|UNIT|SUITE|STE|APT|FLOOR|FL|RM|ROOM)\s+#?\s*|[#])\s*[A-Z0-9-]+$/i;

/** Bidirectional long ↔ short (word-boundary). Order: try long form first in generator where relevant. */
const NYC_STREET_TYPE_PAIRS: readonly [string, string][] = [
  ["STREET", "ST"],
  ["AVENUE", "AVE"],
  ["ROAD", "RD"],
  ["BOULEVARD", "BLVD"],
  ["PLACE", "PL"],
  ["PARKWAY", "PKWY"],
];

function collapseSpaces(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

/** Never treat these as the "borough" token for candidate suffixes — avoids LIC replacing Queens. */
const NYC_NON_BOROUGH_LOCALITY_LABELS = new Set(["LONG ISLAND CITY", "LIC"]);

/**
 * If the user typed an NYC borough name (e.g. Queens), prefer that in geo suffix candidates
 * so we do not rely only on "NEW YORK, NY" — PLUTO and many gold tables use borough labels.
 * Explicit borough (especially QUEENS) wins over neighborhood labels; we never substitute Long Island City for Queens.
 */
export function extractPreferredNycBoroughFromUserInput(raw: string): string | null {
  const parts = raw.split(",").map((p) => p.trim()).filter(Boolean);
  const upperParts = parts.map((p) => p.toUpperCase());
  if (upperParts.includes("QUEENS")) return "QUEENS";
  for (const p of parts) {
    const u = p.toUpperCase();
    if (NYC_NON_BOROUGH_LOCALITY_LABELS.has(u)) continue;
    if (NYC_BOROUGHS_AND_CITY.has(u) && u !== "NEW YORK" && u !== "NYC") {
      return u;
    }
  }
  return null;
}

/** When both "Long Island City" and "Queens" appear (e.g. geocoder noise), keep Queens and drop LIC from the core. */
function preferBoroughOverConflictingNeighborhoodLabels(parts: readonly string[]): string[] {
  const upper = parts.map((p) => p.trim().toUpperCase());
  const hasQueens = upper.some((x) => x === "QUEENS");
  if (!hasQueens) return [...parts];
  return parts.filter((p, i) => !NYC_NON_BOROUGH_LOCALITY_LABELS.has(upper[i]!));
}

function isGeographicOnlyPart(part: string): boolean {
  const p = part.trim();
  if (!p) return true;
  if (/^(USA|UNITED STATES|UNITED STATES OF AMERICA)$/i.test(p)) return true;
  if (/^\d{5}(-\d{4})?$/.test(p)) return true;
  if (/^[A-Z]{2}\s+\d{5}(-\d{4})?$/.test(p)) return true;
  if (/^[A-Z]{2}$/.test(p)) return true;
  if (NYC_BOROUGHS_AND_CITY.has(p)) return true;
  return false;
}

/**
 * Strip trailing ", USA" / ", NY 11234" / ", Brooklyn" style tails when there are no commas
 * in the street segment (single-token tail removal from full line).
 */
function stripTrailingGeoFromUncommaLine(line: string): string {
  let s = collapseSpaces(line);
  const patterns: RegExp[] = [
    /\s*,\s*(USA|UNITED STATES|UNITED STATES OF AMERICA)\s*$/i,
    /\s*,\s*[A-Z]{2}\s+\d{5}(-\d{4})?\s*$/i,
    /\s*,\s*\d{5}(-\d{4})?\s*$/i,
    /\s*,\s*[A-Z]{2}\s*$/i,
    /\s*,\s*(BROOKLYN|MANHATTAN|QUEENS|BRONX|STATEN ISLAND|NEW YORK|NYC|THE BRONX)\s*$/i,
    /\s+(USA|UNITED STATES|UNITED STATES OF AMERICA)\s*$/i,
    /\s+(BROOKLYN|MANHATTAN|QUEENS|BRONX|STATEN ISLAND|NEW YORK|NYC|THE BRONX)\s+[A-Z]{2}\s+\d{5}(-\d{4})?\s*$/i,
    /\s+[A-Z]{2}\s+\d{5}(-\d{4})?\s*$/i,
    /\s+\d{5}(-\d{4})?\s*$/i,
    /\s+(BROOKLYN|MANHATTAN|QUEENS|BRONX|STATEN ISLAND|NEW YORK|NYC|THE BRONX)\s*$/i,
    /\s+[A-Z]{2}\s*$/i,
  ];
  let changed = true;
  while (changed) {
    changed = false;
    for (const re of patterns) {
      const next = s.replace(re, "").trim();
      if (next !== s) {
        s = next;
        changed = true;
      }
    }
  }
  return collapseSpaces(s.replace(/^,+|,+$/g, ""));
}

function expandStreetAbbreviations(line: string): string {
  let s = line;
  for (const [re, rep] of STREET_ABBREV) {
    s = s.replace(re, rep);
  }
  return collapseSpaces(s);
}

function buildingLineWithoutUnit(normalizedWithPossibleUnit: string): string | null {
  const m = normalizedWithPossibleUnit.match(UNIT_SUFFIX_RE);
  if (!m) return null;
  const stripped = normalizedWithPossibleUnit.replace(UNIT_SUFFIX_RE, "").trim();
  return stripped.length > 0 ? collapseSpaces(stripped) : null;
}

/** Extract 5-digit (or ZIP+4) ZIP from raw input for NYC suffix candidates. */
export function extractNycZipFromRawInput(raw: string): string | null {
  const m = raw.match(/\b(\d{5})(?:-(\d{4}))?\b/);
  if (!m) return null;
  return m[2] ? `${m[1]}-${m[2]}` : m[1];
}

function pushUniqueOrdered(out: string[], s: string): void {
  const t = collapseSpaces(s);
  if (!t || out.includes(t)) return;
  out.push(t);
}

/**
 * W ↔ WEST, E ↔ EAST, N ↔ NORTH, S ↔ SOUTH (Manhattan grid); Central Park W/E ↔ spelled out.
 */
function expandNycGridDirectionalAliasesBothWays(line: string): string[] {
  const s = line.trim();
  if (!s) return [];
  const acc: string[] = [];
  pushUniqueOrdered(acc, s);

  const westLong = s.replace(/^(\d+)\s+W\s+(?=\d)/i, "$1 WEST ");
  if (westLong !== s) pushUniqueOrdered(acc, westLong);
  const westShort = s.replace(/^(\d+)\s+WEST\s+(?=\d)/i, "$1 W ");
  if (westShort !== s) pushUniqueOrdered(acc, westShort);

  const eastLong = s.replace(/^(\d+)\s+E\s+(?=\d)/i, "$1 EAST ");
  if (eastLong !== s) pushUniqueOrdered(acc, eastLong);
  const eastShort = s.replace(/^(\d+)\s+EAST\s+(?=\d)/i, "$1 E ");
  if (eastShort !== s) pushUniqueOrdered(acc, eastShort);

  const northLong = s.replace(/^(\d+)\s+N\s+(?=\d)/i, "$1 NORTH ");
  if (northLong !== s) pushUniqueOrdered(acc, northLong);
  const northShort = s.replace(/^(\d+)\s+NORTH\s+(?=\d)/i, "$1 N ");
  if (northShort !== s) pushUniqueOrdered(acc, northShort);

  const southLong = s.replace(/^(\d+)\s+S\s+(?=\d)/i, "$1 SOUTH ");
  if (southLong !== s) pushUniqueOrdered(acc, southLong);
  const southShort = s.replace(/^(\d+)\s+SOUTH\s+(?=\d)/i, "$1 S ");
  if (southShort !== s) pushUniqueOrdered(acc, southShort);

  const cpwLong = s.replace(/\bCENTRAL\s+PARK\s+W\b/i, "CENTRAL PARK WEST");
  if (cpwLong !== s) pushUniqueOrdered(acc, cpwLong);
  const cpwShort = s.replace(/\bCENTRAL\s+PARK\s+WEST\b/i, "CENTRAL PARK W");
  if (cpwShort !== s) pushUniqueOrdered(acc, cpwShort);

  const cpeLong = s.replace(/\bCENTRAL\s+PARK\s+E\b/i, "CENTRAL PARK EAST");
  if (cpeLong !== s) pushUniqueOrdered(acc, cpeLong);
  const cpeShort = s.replace(/\bCENTRAL\s+PARK\s+EAST\b/i, "CENTRAL PARK E");
  if (cpeShort !== s) pushUniqueOrdered(acc, cpeShort);

  return acc;
}

/**
 * ST ↔ STREET, AVE ↔ AVENUE, etc. (word-boundary; avoids touching unrelated tokens).
 */
function expandNycStreetTypeAliasesBidirectional(line: string): string[] {
  const acc: string[] = [];
  pushUniqueOrdered(acc, line);
  for (const [longForm, shortForm] of NYC_STREET_TYPE_PAIRS) {
    const snapshot = [...acc];
    for (const cur of snapshot) {
      if (new RegExp(`\\b${longForm}\\b`, "i").test(cur)) {
        pushUniqueOrdered(acc, collapseSpaces(cur.replace(new RegExp(`\\b${longForm}\\b`, "gi"), shortForm)));
      }
      if (new RegExp(`\\b${shortForm}\\b`, "i").test(cur)) {
        pushUniqueOrdered(acc, collapseSpaces(cur.replace(new RegExp(`\\b${shortForm}\\b`, "gi"), longForm)));
      }
    }
  }
  return acc;
}

/** Street-type token after a grid number (house + W/E/N/S + street number). */
const NYC_STREET_TYPE_AFTER_GRID = String.raw`(?:STREET|ST|AVE|AVENUE|RD|ROAD|BLVD|BOULEVARD|PL|PLACE|PKWY|PARKWAY|LN|LANE|DR|DRIVE|CT|COURT)`;

/**
 * "86TH" → "86", "63RD" → "63" (BigQuery sometimes stores plain numbers before street type).
 */
function stripStreetOrdinalSuffixTokens(line: string): string {
  return collapseSpaces(line.replace(/\b(\d{1,3})(ST|ND|RD|TH)\b/gi, "$1"));
}

/**
 * "40 W 86 STREET" → "40 W 86TH STREET" (default TH when adding; matches gold tables that use ordinals).
 */
function addDefaultOrdinalAfterGridDirection(line: string): string | null {
  const re = new RegExp(
    String.raw`\b(W|E|N|S)\s+(\d{1,3})\s+(?=` + NYC_STREET_TYPE_AFTER_GRID + String.raw`\b)`,
    "gi"
  );
  const next = line.replace(re, (_full, dir: string, n: string) => `${dir} ${n}TH `);
  return next === line ? null : collapseSpaces(next);
}

/**
 * After all other address-only variants: append ordinal-stripped and default-TH forms (deterministic; originals first).
 */
function appendOrdinalVariants(addressOnly: readonly string[]): string[] {
  const out: string[] = [];
  for (const line of addressOnly) {
    pushUniqueOrdered(out, line);
  }
  for (const line of addressOnly) {
    const stripped = stripStreetOrdinalSuffixTokens(line);
    if (stripped !== line) {
      pushUniqueOrdered(out, stripped);
    }
    const withTh = addDefaultOrdinalAfterGridDirection(line);
    if (withTh != null && withTh !== line) {
      pushUniqueOrdered(out, withTh);
    }
    if (stripped !== line) {
      const withThFromStripped = addDefaultOrdinalAfterGridDirection(stripped);
      if (withThFromStripped != null && withThFromStripped !== stripped && withThFromStripped !== line) {
        pushUniqueOrdered(out, withThFromStripped);
      }
    }
  }
  return out;
}

/**
 * Priority (lookup order):
 * 1) Address-only: base core + building line, then directional grid aliases, then street-type aliases.
 * 2) Ordinal variants (plain number ↔ ordinal) for grid streets.
 * 3) Same lines with ", NEW YORK, NY", then ", NEW YORK, NY {ZIP}" when ZIP known, then ", USA".
 */
function buildOrderedAddressOnlyCandidates(core: string, buildingOnly: string | null): string[] {
  const ordered: string[] = [];
  const seeds: string[] = [];
  pushUniqueOrdered(seeds, core);
  if (buildingOnly && buildingOnly !== core) pushUniqueOrdered(seeds, buildingOnly);

  for (const seed of seeds) {
    pushUniqueOrdered(ordered, seed);
    for (const dir of expandNycGridDirectionalAliasesBothWays(seed)) {
      pushUniqueOrdered(ordered, dir);
    }
  }

  const withStreetTypes: string[] = [];
  for (const line of ordered) {
    for (const st of expandNycStreetTypeAliasesBidirectional(line)) {
      pushUniqueOrdered(withStreetTypes, st);
    }
  }
  return appendOrdinalVariants(withStreetTypes);
}

/**
 * Lookup priority (BigQuery tries in order):
 * 1) All address-only strings (exact + directional + street-type variants).
 * 2) Same strings + ", NEW YORK, NY".
 * 3) Same strings + ", NEW YORK, NY {ZIP}" when ZIP known.
 * 4) Same strings + ", USA".
 * When `preferredBorough` is set (user typed e.g. Queens), emit `, QUEENS, NY` before generic New York variants.
 */
function appendGeoSuffixVariants(
  streetOnlyLines: readonly string[],
  zip: string | null,
  preferredBorough: string | null
): string[] {
  const out: string[] = [];
  for (const line of streetOnlyLines) {
    pushUniqueOrdered(out, line);
  }
  for (const line of streetOnlyLines) {
    if (preferredBorough) {
      pushUniqueOrdered(out, `${line}, ${preferredBorough}, NY`);
      if (zip) {
        pushUniqueOrdered(out, `${line}, ${preferredBorough}, NY ${zip}`);
      }
    }
    pushUniqueOrdered(out, `${line}, NEW YORK, NY`);
  }
  if (zip) {
    for (const line of streetOnlyLines) {
      pushUniqueOrdered(out, `${line}, NEW YORK, NY ${zip}`);
    }
  }
  for (const line of streetOnlyLines) {
    pushUniqueOrdered(out, `${line}, USA`);
  }
  return out;
}

export type NycTruthNormalizationDebug = {
  /** Normalized line including unit suffix when present. */
  normalized_full_address: string;
  /** Building line (unit stripped); same as full when no unit. */
  normalized_building_address: string;
  /** ZIP extracted from input for ", NEW YORK, NY {ZIP}" candidates. */
  zip_from_input: string | null;
  /** Bump when normalization rules change; verify running build includes this version. */
  candidate_generator_version: number;
  /** Candidates passed to BigQuery in deterministic priority order. */
  candidates: string[];
  /** Borough token from user input (e.g. QUEENS) when present — used for geo suffixes only. */
  preferred_borough_from_input: string | null;
};

/**
 * Computes normalized labels + candidate list (single source of truth).
 */
export function buildNycTruthLookupNormalizationDebug(rawInput: string): NycTruthNormalizationDebug | null {
  const upper = collapseSpaces(preserveQueensInAddressLineIfUserTypedQueens(rawInput).toUpperCase());
  const commaParts = upper.split(",").map((p) => p.trim()).filter(Boolean);

  let coreRaw: string;
  if (commaParts.length <= 1) {
    coreRaw = commaParts[0] ?? upper;
  } else {
    const parts = [...preferBoroughOverConflictingNeighborhoodLabels(commaParts)];
    while (parts.length > 0 && isGeographicOnlyPart(parts[parts.length - 1]!)) {
      parts.pop();
    }
    coreRaw = collapseSpaces(parts.join(" "));
  }

  let core = expandStreetAbbreviations(collapseSpaces(coreRaw));
  core = stripTrailingGeoFromUncommaLine(core);
  core = collapseSpaces(core);

  if (!core) return null;

  const buildingOnly = buildingLineWithoutUnit(core);
  const normalized_full_address = core;
  const normalized_building_address = buildingOnly ?? core;
  const zip = extractNycZipFromRawInput(rawInput);
  const preferredBorough = extractPreferredNycBoroughFromUserInput(rawInput);

  const addressOnly = buildOrderedAddressOnlyCandidates(core, buildingOnly);
  const candidates = appendGeoSuffixVariants(addressOnly, zip, preferredBorough);

  return {
    normalized_full_address,
    normalized_building_address,
    zip_from_input: zip,
    candidate_generator_version: NYC_CANDIDATE_GENERATOR_VERSION,
    candidates,
    preferred_borough_from_input: preferredBorough,
  };
}

/**
 * Ordered list of strings to try against `full_address` (exact equality per attempt).
 */
export function buildNycTruthLookupCandidates(rawInput: string): string[] {
  return buildNycTruthLookupNormalizationDebug(rawInput)?.candidates ?? [];
}
