/**
 * DOB-only street name normalization for NYC Open Data job filings (US only).
 * The DOB dataset uses abbreviated suffixes (ST, AVE, …); expanded forms yield no matches.
 * Ordinals on numbered streets (42ND, 1ST, …) are stripped to match DOB `street_name` (e.g. W 42 ST).
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
 * Numbered-street ordinals (42ND, 1ST, …) → plain number tokens so DOB `street_name` matches
 * dataset form (e.g. W 42 ST). Same rule as street-pricing / truth ordinal strip: only contiguous
 * digit+suffix tokens; does not remove `ST` when it is a separate word after the number.
 */
function stripOrdinalStreetNumberSuffixes(upper: string): string {
  return upper.replace(/\b(\d+)(ST|ND|RD|TH)\b/gi, "$1");
}

/**
 * Uppercase, collapse internal whitespace, expand-to-abbrev street suffixes, then strip ordinals for DOB SoQL.
 */
export function normalizeStreetNameForDobQuery(streetName: string): string {
  let s = streetName.trim().replace(/\s+/g, " ").toUpperCase();
  for (const [pattern, replacement] of DOB_STREET_SUFFIX_REPLACEMENTS) {
    s = s.replace(pattern, replacement);
  }
  s = stripOrdinalStreetNumberSuffixes(s);
  return s.trim();
}
