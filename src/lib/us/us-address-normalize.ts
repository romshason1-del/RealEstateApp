/**
 * US address normalization — scaffold only (no parsing heuristics yet).
 */

export type USNormalizedAddressLine = {
  raw_input: string;
  /** Trimmed single-line input; structured parsing added when integrating datasets. */
  line: string | null;
};

export function normalizeUSAddressLine(input: string): USNormalizedAddressLine {
  const trimmed = input.trim();
  return {
    raw_input: input,
    line: trimmed.length > 0 ? trimmed : null,
  };
}
