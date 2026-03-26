/**
 * DOB-only street name normalization for NYC Open Data job filings (US only).
 * The DOB dataset uses abbreviated suffixes (ST, AVE, …); expanded forms yield no matches.
 * Not used for ACRIS, BigQuery, or UI.
 */

/** Longer tokens first; whole-word replacement after uppercase. */
const DOB_STREET_SUFFIX_REPLACEMENTS: ReadonlyArray<[RegExp, string]> = [
  [/\bBOULEVARD\b/g, "BLVD"],
  [/\bPARKWAY\b/g, "PKWY"],
  [/\bTERRACE\b/g, "TER"],
  [/\bSTREET\b/g, "ST"],
  [/\bAVENUE\b/g, "AVE"],
  [/\bPLACE\b/g, "PL"],
  [/\bDRIVE\b/g, "DR"],
  [/\bCOURT\b/g, "CT"],
  [/\bLANE\b/g, "LN"],
  [/\bROAD\b/g, "RD"],
];

/**
 * Uppercase, collapse internal whitespace, expand-to-abbrev street suffixes for DOB SoQL.
 */
export function normalizeStreetNameForDobQuery(streetName: string): string {
  let s = streetName.trim().replace(/\s+/g, " ").toUpperCase();
  for (const [pattern, replacement] of DOB_STREET_SUFFIX_REPLACEMENTS) {
    s = s.replace(pattern, replacement);
  }
  return s.trim();
}
