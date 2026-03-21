/**
 * Defensive unwrap for France API / BigQuery JSON that sometimes encodes scalars as `{ value: ... }`.
 * Prevents React child error #31 (objects are not valid as a React child).
 */

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

/** If `raw` is `{ value: x }`, return `x`; otherwise return `raw`. */
export function unwrapScalar(raw: unknown): unknown {
  if (!isPlainObject(raw)) return raw;
  if (!("value" in raw)) return raw;
  const v = raw.value;
  return v !== undefined ? v : raw;
}

/**
 * Reduce France API / BigQuery `last_sale_date` / `transactionDate` payloads to a single ISO date
 * string (`YYYY-MM-DD`) or null. Handles nested `{ value }`, `{ date }`, `{ text }`, and simple
 * `{ year, month, day }` shapes. Never returns a non-primitive.
 */
export function coerceFranceDisplayDateString(raw: unknown): string | null {
  let v: unknown = raw;
  for (let depth = 0; depth < 8; depth++) {
    v = unwrapScalar(v);
    if (v === null || v === undefined) return null;
    if (typeof v === "string") {
      const t = v.trim();
      return t.length ? t : null;
    }
    if (typeof v === "number" && Number.isFinite(v)) {
      const ms = v > 1e12 ? v : v > 1e9 ? v * 1000 : NaN;
      const d = new Date(ms);
      return Number.isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
    }
    if (v instanceof Date) {
      return Number.isNaN(v.getTime()) ? null : v.toISOString().slice(0, 10);
    }
    if (!isPlainObject(v)) return null;
    const o = v as Record<string, unknown>;
    if ("date" in o && o.date != null) {
      v = o.date;
      continue;
    }
    if ("text" in o && o.text != null) {
      v = o.text;
      continue;
    }
    if ("day" in o && "month" in o && "year" in o) {
      const y = Number(o.year);
      const mo = Number(o.month);
      const day = Number(o.day);
      if ([y, mo, day].every((n) => Number.isFinite(n))) {
        return `${Math.trunc(y)}-${String(Math.trunc(mo)).padStart(2, "0")}-${String(Math.trunc(day)).padStart(2, "0")}`;
      }
    }
    if ("value" in o) {
      v = o.value;
      continue;
    }
    return null;
  }
  return null;
}

/** Human-readable date for France UI (e.g. `13 Dec 2024`), or null if missing / unparseable. */
export function formatFranceDateLabelFromUnknown(raw: unknown): string | null {
  const iso = coerceFranceDisplayDateString(raw);
  if (!iso) return null;
  const d = new Date(iso);
  if (!Number.isNaN(d.getTime())) {
    return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
  }
  return iso.trim().length ? iso.trim() : null;
}

/** Coerce to a finite number, or null. Handles `{ value: n | "n" }`. */
export function coerceFiniteNumber(raw: unknown): number | null {
  const u = unwrapScalar(raw);
  if (u === null || u === undefined) return null;
  if (typeof u === "number" && Number.isFinite(u)) return u;
  if (typeof u === "string") {
    const n = Number(String(u).replace(/\s/g, "").replace(",", "."));
    return Number.isFinite(n) ? n : null;
  }
  if (typeof u === "boolean") return u ? 1 : 0;
  return null;
}

/** Coerce to a positive finite number, or null. */
export function coercePositiveNumber(raw: unknown): number | null {
  const n = coerceFiniteNumber(raw);
  return n != null && n > 0 ? n : null;
}

/**
 * Safe string for JSX text nodes. Never returns a non-primitive.
 * If the payload is `{ value: "x" }`, uses `value`.
 */
export function coerceDisplayString(raw: unknown, fallback = "—"): string {
  const u = unwrapScalar(raw);
  if (u === null || u === undefined) return fallback;
  if (typeof u === "string") return u;
  if (typeof u === "number" && Number.isFinite(u)) return String(u);
  if (typeof u === "boolean") return u ? "true" : "false";
  return fallback;
}

/** `string | null` for optional API text fields (never returns an object). */
export function coerceNullableString(raw: unknown): string | null {
  const u = unwrapScalar(raw);
  if (u === null || u === undefined) return null;
  if (typeof u === "string") return u;
  if (typeof u === "number" && Number.isFinite(u)) return String(u);
  if (typeof u === "boolean") return String(u);
  return null;
}

/** Confidence / enum-like labels for display (title-case segments). */
export function coerceConfidenceLabel(raw: unknown): string | null {
  const u = unwrapScalar(raw);
  if (u === null || u === undefined) return null;
  const s = String(u).replace(/_/g, "-").trim();
  if (!s) return null;
  return s
    .split("-")
    .map((p) => (p ? p[0].toUpperCase() + p.slice(1).toLowerCase() : p))
    .join("-");
}

export type FrancePropertyResultLike = {
  exact_value: number | null;
  exact_value_message: string | null;
  value_level: "property-level" | "building-level" | "street-level" | "area-level" | "no_match";
  last_transaction: { amount: number; date: string | null; message?: string };
  street_average: number | null;
  street_average_message: string | null;
  livability_rating: "POOR" | "FAIR" | "GOOD" | "VERY GOOD" | "EXCELLENT";
};

/** Sanitize `property_result` for France so no nested `{ value }` reaches JSX. */
export function sanitizeFrancePropertyResultForDisplay(pr: FrancePropertyResultLike): FrancePropertyResultLike {
  const ev = coercePositiveNumber(pr.exact_value as unknown);
  const sa = coercePositiveNumber(pr.street_average as unknown);
  const amt = coerceFiniteNumber(pr.last_transaction?.amount as unknown);
  const msg = coerceNullableString(pr.last_transaction?.message);
  const dateRaw = unwrapScalar(pr.last_transaction?.date as unknown);
  const dateStr =
    dateRaw === null || dateRaw === undefined
      ? null
      : String(dateRaw).trim() === ""
        ? null
        : String(dateRaw);

  const livRaw = unwrapScalar(pr.livability_rating as unknown);
  const livStr = typeof livRaw === "string" ? livRaw.trim() : String(livRaw ?? "").trim();
  const livAllowed = new Set(["POOR", "FAIR", "GOOD", "VERY GOOD", "EXCELLENT"]);
  const livability_rating = (livAllowed.has(livStr) ? livStr : "FAIR") as FrancePropertyResultLike["livability_rating"];

  return {
    ...pr,
    exact_value: ev ?? (typeof pr.exact_value === "number" && Number.isFinite(pr.exact_value) && pr.exact_value > 0 ? pr.exact_value : null),
    street_average: sa ?? (typeof pr.street_average === "number" && Number.isFinite(pr.street_average) && pr.street_average > 0 ? pr.street_average : null),
    exact_value_message: coerceNullableString(pr.exact_value_message),
    street_average_message: coerceNullableString(pr.street_average_message),
    livability_rating,
    last_transaction: {
      amount:
        amt != null && amt > 0
          ? amt
          : typeof pr.last_transaction?.amount === "number" && Number.isFinite(pr.last_transaction.amount)
            ? pr.last_transaction.amount
            : 0,
      date: dateStr,
      message: msg ?? undefined,
    },
  };
}

/** Number grouping for France UI (commas); stable across user OS locales. */
export const FRANCE_UI_NUMBER_LOCALE = "en-US";

/**
 * Display-only: some payloads historically sent €/m² as centimes/m²; scale when absurdly high.
 * Does not affect valuation — formatting path only.
 */
export function normalizeFrancePricePerSqmForDisplay(raw: number): number {
  if (!Number.isFinite(raw) || raw <= 0) return raw;
  if (raw > 100_000) return raw / 100;
  return raw;
}

/** Whole-euro amounts: `173886` → `173,886 €`. */
export function formatFranceEuroTotal(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return `${Math.round(value).toLocaleString(FRANCE_UI_NUMBER_LOCALE, { maximumFractionDigits: 0, minimumFractionDigits: 0 })} €`;
}

/** €/m² with grouping; optional `(median)` suffix for headlines. */
export function formatFranceEuroPerSqm(value: number, options?: { medianSuffix?: boolean }): string {
  if (!Number.isFinite(value) || value <= 0) return "— €/m²";
  const n = Math.round(normalizeFrancePricePerSqmForDisplay(value));
  const base = `${n.toLocaleString(FRANCE_UI_NUMBER_LOCALE, { maximumFractionDigits: 0, minimumFractionDigits: 0 })} €/m²`;
  return options?.medianSuffix ? `${base} (median)` : base;
}

/** €/m² from API/BigQuery raw (may be `{ value: n }`). */
export function formatFranceEuroPerSqmFromUnknown(raw: unknown, options?: { medianSuffix?: boolean }): string {
  const coerced = coerceFiniteNumber(raw);
  if (coerced == null || coerced <= 0) return "— €/m²";
  return formatFranceEuroPerSqm(coerced, options);
}
