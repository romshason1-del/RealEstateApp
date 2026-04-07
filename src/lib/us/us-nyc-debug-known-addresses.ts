/**
 * LEGACY — not used by the current NYC production path (`/api/us/nyc-app-output` → v4).
 * NYC-only debug: known Manhattan test addresses (pre-v4 precomputed card rules). Not used for France.
 */

import { computeNycNeedsUnitPrompt, parseFirstUsdAmountFromText } from "./us-nyc-precomputed-card";

/** Uppercase collapsed keys for exact user input lines (commas + spacing normalized). */
const NYC_DEBUG_FORCE_UNIT_PROMPT_KEYS = new Set([
  "40 W 86TH ST, NEW YORK, NY 10024",
  "245 E 63RD ST, NEW YORK, NY 10065",
  "234 CENTRAL PARK W, NEW YORK, NY 10024",
]);

function normalizeNycDebugAddressKey(raw: string): string {
  return raw.replace(/\s+/g, " ").trim().toUpperCase();
}

function toNumberOrNull(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "bigint") {
    const x = Number(v);
    return Number.isFinite(x) ? x : null;
  }
  const x = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(x) ? x : null;
}

function normalizeBuildingTypeKey(raw: string): string {
  return raw.replace(/[-\s]+/g, "_").toLowerCase().trim();
}

/**
 * If normal rules already prompt, do not override.
 * Otherwise, for these three inputs only: matched row is not single/two-family, value + parsed sale are empty,
 * and the row still looks multi-unit (inventory type, multifamily, unit_count, or badges).
 */
export function shouldApplyNycDebugKnownAddressUnitPromptOverride(
  originalInput: string,
  row: Record<string, unknown>,
  opts: { addressLine: string; candidatesCount: number }
): boolean {
  if (!NYC_DEBUG_FORCE_UNIT_PROMPT_KEYS.has(normalizeNycDebugAddressKey(originalInput))) return false;

  if (computeNycNeedsUnitPrompt(row, opts)) return false;

  const bt = normalizeBuildingTypeKey(String(row.building_type ?? ""));
  if (bt === "single_family" || bt === "two_family") return false;

  const ev = toNumberOrNull(row.estimated_value);
  const tx = row.final_last_transaction_text;
  const sale = parseFirstUsdAmountFromText(tx != null ? String(tx) : "");
  const emptyEv = ev == null || ev <= 0;
  const emptySale = sale == null || sale <= 0;
  if (!emptyEv || !emptySale) return false;

  const uc = toNumberOrNull(row.unit_count);
  if (uc != null && uc > 1) return true;

  const inv = new Set(["condo", "co_op", "coop", "apartment"]);
  if (inv.has(bt)) return true;

  const mf = new Set(["large_multifamily", "small_multifamily", "mixed_use"]);
  if (mf.has(bt)) return true;

  const parts = [row.badge_1, row.badge_2, row.badge_3, row.badge_4].map((x) => String(x ?? ""));
  if (parts.some((p) => /\b(CONDO|CO-OP|COOP|MULTI|APARTMENT|RENTAL|UNIT|COOPERATIVE)\b/i.test(p))) return true;

  return false;
}
