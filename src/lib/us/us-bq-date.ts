/**
 * BigQuery DATE / TIMESTAMP wrappers often arrive as { value: "YYYY-MM-DD" } in Node.
 * Avoid String(object) → "[object Object]" in API JSON and UI.
 */

export function coerceBigQueryDateToYyyyMmDd(v: unknown): string | null {
  if (v == null || v === "") return null;
  if (v instanceof Date && !Number.isNaN(v.getTime())) {
    return v.toISOString().slice(0, 10);
  }
  if (typeof v === "string") {
    const t = v.trim();
    if (/^\d{4}-\d{2}-\d{2}/.test(t)) return t.slice(0, 10);
    return t.length > 0 ? t : null;
  }
  if (typeof v === "number" && Number.isFinite(v)) {
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
  }
  if (typeof v === "object" && v !== null) {
    const o = v as Record<string, unknown>;
    if (typeof o.value === "string") {
      const t = o.value.trim();
      if (/^\d{4}-\d{2}-\d{2}/.test(t)) return t.slice(0, 10);
    }
  }
  return null;
}
