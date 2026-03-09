/**
 * Display utilities for English-only UI output.
 * Masks Hebrew and other non-Latin script to enforce English-only frontend.
 */

import { hebrewToAscii } from "./address-canonical";

const HEBREW_REGEX = /[\u0590-\u05FF]/;

/** Returns display-safe string: Hebrew is transliterated to ASCII, otherwise unchanged. */
export function toEnglishDisplay(val: string | undefined | null): string {
  if (val == null || typeof val !== "string") return "";
  const s = String(val).trim();
  if (!s) return "";
  return HEBREW_REGEX.test(s) ? hebrewToAscii(s) : s;
}

/** Returns [Hebrew] for strings containing Hebrew (for compact debug display). */
export function maskHebrew(val: string | undefined | null): string {
  if (val == null || typeof val !== "string") return "";
  const s = String(val).trim();
  if (!s) return "";
  return HEBREW_REGEX.test(s) ? "[Hebrew]" : s;
}

/** Recursively sanitize object for display: replace Hebrew strings with [Hebrew]. */
export function sanitizeForDisplay(obj: unknown): unknown {
  if (obj == null) return obj;
  if (typeof obj === "string") return maskHebrew(obj);
  if (Array.isArray(obj)) return obj.map(sanitizeForDisplay);
  if (typeof obj === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = sanitizeForDisplay(v);
    }
    return out;
  }
  return obj;
}
