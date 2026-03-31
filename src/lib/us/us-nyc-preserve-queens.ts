/**
 * NYC borough preservation (US only) — keep "Queens" when the user typed it; do not let Google
 * neighborhood labels (e.g. Long Island City) replace it in resolved addresses or normalization.
 */

/** If `originalUserInput` contains "Queens", strip/replace Google-style LIC labels in the resolved line. */
export function preserveQueensOverGoogleNeighborhood(
  originalUserInput: string | undefined,
  resolvedAddress: string
): string {
  if (!originalUserInput?.trim() || !/\bQueens\b/i.test(originalUserInput)) return resolvedAddress;
  return resolvedAddress
    .replace(/\bLong Island City\b/gi, "Queens")
    .replace(/\b,\s*LIC\s*,/gi, ", Queens,");
}

/** Same rule for a single free-text line (server-side normalization). */
export function preserveQueensInAddressLineIfUserTypedQueens(rawLine: string): string {
  if (!/\bQueens\b/i.test(rawLine)) return rawLine;
  return rawLine
    .replace(/\bLong Island City\b/gi, "Queens")
    .replace(/\b,\s*LIC\s*,/gi, ", Queens,");
}
