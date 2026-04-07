/**
 * LEGACY URL ALIAS — forwards to `/api/us/nyc-app-output` (BigQuery `real_estate_us.us_nyc_app_output_final_v4`).
 *
 * The retired PLUTO prefetch + precomputed card v5 + `us-nyc-api-truth` HTTP pipeline is not implemented here.
 * Production NYC: `src/lib/property-value-api.ts` and `/api/property-value` (US) → `/api/us/nyc-app-output`.
 *
 * Offline/debug of the old BigQuery join stack: use `scripts/debug-nyc-property-value.ts` / `scripts/nyc-live-e2e-probe.ts`
 * (they import `us-nyc-api-truth` / `us-nyc-main-payload` directly in Node).
 */

import { NextRequest, NextResponse } from "next/server";
import { omitUsNycDebugFromPayload } from "@/lib/us/us-nyc-api-response-debug";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

async function forwardToNycAppOutput(req: NextRequest, searchParams: URLSearchParams) {
  const target = new URL("/api/us/nyc-app-output", req.nextUrl.origin);
  searchParams.forEach((v, k) => {
    target.searchParams.set(k, v);
  });
  const unit = target.searchParams.get("unit");
  const uol = target.searchParams.get("unit_or_lot");
  const combined = (unit?.trim() || uol?.trim()) ?? "";
  if (combined) target.searchParams.set("unit_or_lot", combined);
  target.searchParams.delete("unit");

  const res = await fetch(target.toString(), { cache: "no-store" });
  const text = await res.text();
  let data: Record<string, unknown>;
  try {
    data = JSON.parse(text) as Record<string, unknown>;
  } catch {
    return new NextResponse(text, {
      status: res.status,
      headers: { "content-type": res.headers.get("content-type") ?? "application/json" },
    });
  }
  return NextResponse.json(omitUsNycDebugFromPayload(data), { status: res.status });
}

export async function GET(req: NextRequest) {
  return forwardToNycAppOutput(req, req.nextUrl.searchParams);
}

export async function POST(req: NextRequest) {
  try {
    const body = (await req.json()) as { address?: string; unit_or_lot?: string; unit?: string };
    const p = new URLSearchParams();
    if (typeof body.address === "string") p.set("address", body.address);
    const u =
      (typeof body.unit === "string" ? body.unit : "") ||
      (typeof body.unit_or_lot === "string" ? body.unit_or_lot : "");
    if (u.trim()) p.set("unit_or_lot", u.trim());
    return forwardToNycAppOutput(req, p);
  } catch {
    return NextResponse.json({ success: false, message: "Invalid JSON body", row: null }, { status: 400 });
  }
}
