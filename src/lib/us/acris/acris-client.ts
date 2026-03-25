/**
 * Minimal Socrata JSON fetch for NYC ACRIS resources. US-only; no shared France code.
 */

import {
  ACRIS_DEFAULT_LIMIT,
  ACRIS_SOCRATA_ORIGIN,
  acrisSocrataAppToken,
} from "./acris-config";

export type AcrisClientSuccess<T> = { ok: true; data: T };
export type AcrisClientFailure = { ok: false; error: string; status?: number };

function buildUrl(path: string, query: Record<string, string | undefined>): string {
  const u = new URL(path.startsWith("http") ? path : `${ACRIS_SOCRATA_ORIGIN}${path}`);
  for (const [k, v] of Object.entries(query)) {
    if (v !== undefined && v !== "") u.searchParams.set(k, v);
  }
  return u.toString();
}

/**
 * GET a Socrata resource (expects JSON array of objects).
 * Pass SoQL via `query.$where`, `query.$limit`, `query.$order`, etc.
 */
export async function acrisSocrataGet<T = unknown>(
  resourcePath: string,
  query: Record<string, string | undefined>,
  init?: { signal?: AbortSignal; timeoutMs?: number }
): Promise<AcrisClientSuccess<T> | AcrisClientFailure> {
  const token = acrisSocrataAppToken();
  const headers: HeadersInit = {
    Accept: "application/json",
    ...(token ? { "X-App-Token": token } : {}),
  };

  let signal = init?.signal;
  let clearTimer: (() => void) | undefined;
  const timeoutMs = init?.timeoutMs ?? 25_000;
  if (timeoutMs > 0 && typeof AbortController !== "undefined") {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    clearTimer = () => clearTimeout(t);
    if (signal) {
      if (signal.aborted) ctrl.abort();
      else
        signal.addEventListener(
          "abort",
          () => {
            ctrl.abort();
          },
          { once: true }
        );
    }
    signal = ctrl.signal;
  }

  try {
    const url = buildUrl(resourcePath, query);
    const res = await fetch(url, { method: "GET", headers, signal });
    const text = await res.text();
    clearTimer?.();

    if (!res.ok) {
      return {
        ok: false,
        error: text?.slice(0, 500) || `HTTP ${res.status}`,
        status: res.status,
      };
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(text) as unknown;
    } catch {
      return { ok: false, error: "Invalid JSON from ACRIS Socrata", status: res.status };
    }

    return { ok: true, data: parsed as T };
  } catch (e) {
    clearTimer?.();
    const msg = e instanceof Error ? e.message : String(e);
    if (msg === "The user aborted a request." || msg.includes("abort")) {
      return { ok: false, error: "Request aborted" };
    }
    return { ok: false, error: msg };
  }
}

/** Escape a string for use inside a SoQL string literal (single quotes doubled). */
export function acrisEscapeSoqlString(value: string): string {
  return value.replace(/'/g, "''");
}

export function acrisDefaultLimitParam(limit?: number): string {
  const n = limit ?? ACRIS_DEFAULT_LIMIT;
  return String(Math.max(1, Math.min(n, 50_000)));
}
