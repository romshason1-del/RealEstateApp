/**
 * NYC API responses may include `us_nyc_debug` for engineering. Omit in production JSON only.
 * US-only — not used for France.
 */

export function shouldIncludeUsNycDebugInApiResponse(): boolean {
  return process.env.NODE_ENV !== "production";
}

export function omitUsNycDebugFromPayload<T extends Record<string, unknown>>(payload: T): T {
  if (shouldIncludeUsNycDebugInApiResponse()) return payload;
  if (payload == null || typeof payload !== "object" || Array.isArray(payload)) return payload;
  const { us_nyc_debug: _a, us_nyc_app_output_debug: _b, ...rest } = payload;
  return rest as T;
}
