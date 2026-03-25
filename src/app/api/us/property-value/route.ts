/**
 * US property value API (isolated route).
 * POST/GET /api/us/property-value — scaffold; does not use France pipeline or /api/property-value.
 */

import { NextRequest, NextResponse } from "next/server";
import { normalizeUSAddressLine } from "@/lib/us/us-address-normalize";
import { createEmptyUSPropertyValueResponse } from "@/lib/us/us-response-shape";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  const address = req.nextUrl.searchParams.get("address") ?? "";
  void normalizeUSAddressLine(address);
  return NextResponse.json(createEmptyUSPropertyValueResponse({ message: null }));
}

export async function POST(req: NextRequest) {
  let body: { address?: string } = {};
  try {
    body = (await req.json()) as { address?: string };
  } catch {
    body = {};
  }
  void normalizeUSAddressLine(typeof body.address === "string" ? body.address : "");
  return NextResponse.json(createEmptyUSPropertyValueResponse({ message: null }));
}
