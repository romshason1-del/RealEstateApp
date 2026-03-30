/**
 * US address normalization — scaffold only (no parsing heuristics yet).
 */

export type USNormalizedAddressLine = {
  raw_input: string;
  /** Trimmed single-line input; structured parsing added when integrating datasets. */
  line: string | null;
};

/** PRK→PARK (incl. Central Park West) — aligned with normalizeNycAddressMasterV1Line pre-suffix pass. */
function applyPrkToParkAliases(upperLine: string): string {
  let s = upperLine;
  s = s.replace(/\bCENTRAL PRK\b/g, "CENTRAL PARK");
  s = s.replace(/ PRK /g, " PARK ");
  s = s.replace(/ PRK$/g, " PARK");
  return s;
}

export function normalizeUSAddressLine(input: string): USNormalizedAddressLine {
  const trimmed = input.trim();
  if (trimmed.length === 0) {
    return { raw_input: input, line: null };
  }
  const line = applyPrkToParkAliases(trimmed.toUpperCase());
  return {
    raw_input: input,
    line,
  };
}
