/**
 * Deterministic NYC address normalization for matching `us_nyc_api_truth` (US only).
 * Not fuzzy — fixed rules only.
 */

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

function collapseSpaces(s: string): string {
  return s.replace(/\s+/g, " ").trim();
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

export type NycTruthNormalizationDebug = {
  /** Normalized line including unit suffix when present. */
  normalized_full_address: string;
  /** Building line (unit stripped); same as full when no unit. */
  normalized_building_address: string;
  /** Candidates passed to BigQuery in order. */
  candidates: string[];
};

/**
 * Computes normalized labels + candidate list (single source of truth).
 */
export function buildNycTruthLookupNormalizationDebug(rawInput: string): NycTruthNormalizationDebug | null {
  const upper = collapseSpaces(rawInput.toUpperCase());
  const commaParts = upper.split(",").map((p) => p.trim()).filter(Boolean);

  let coreRaw: string;
  if (commaParts.length <= 1) {
    coreRaw = commaParts[0] ?? upper;
  } else {
    const parts = [...commaParts];
    while (parts.length > 0 && isGeographicOnlyPart(parts[parts.length - 1]!)) {
      parts.pop();
    }
    coreRaw = collapseSpaces(parts.join(" "));
  }

  // Expand ST → STREET (etc.) before stripping trailing "state" tokens — otherwise "… ROYCE ST" loses ST as a false "state".
  let core = expandStreetAbbreviations(collapseSpaces(coreRaw));
  core = stripTrailingGeoFromUncommaLine(core);
  core = collapseSpaces(core);

  if (!core) return null;

  const buildingOnly = buildingLineWithoutUnit(core);
  const normalized_full_address = core;
  const normalized_building_address = buildingOnly ?? core;

  const out: string[] = [];
  if (buildingOnly && buildingOnly !== core) {
    out.push(core, buildingOnly);
  } else {
    out.push(core);
  }
  const candidates = [...new Set(out)];

  return {
    normalized_full_address,
    normalized_building_address,
    candidates,
  };
}

/**
 * Ordered list of strings to try against pluto_address / sales_address (exact equality).
 * 1) Full normalized line (includes unit if present)
 * 2) Building-only line if unit was present and differs
 */
export function buildNycTruthLookupCandidates(rawInput: string): string[] {
  return buildNycTruthLookupNormalizationDebug(rawInput)?.candidates ?? [];
}
