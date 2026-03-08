"use client";

export type IsraelTransaction = {
  price: number;
  date: string | null;
};

export type IsraelRealEstateResponse = {
  transactions: IsraelTransaction[];
  avgPrice: number | null;
  lastSaleDate: string | null;
  lastSalePrice: number | null;
  source: string;
  isCityFallback?: boolean;
  error?: string;
};

const CACHE = new Map<string, { data: IsraelRealEstateResponse; ts: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000;

const FALLBACK_RESPONSE: IsraelRealEstateResponse = {
  transactions: [],
  avgPrice: null,
  lastSaleDate: null,
  lastSalePrice: null,
  source: "data.gov.il",
  error: "Connection failed",
};

export async function fetchIsraelRealEstate(address: string): Promise<IsraelRealEstateResponse> {
  const key = address.trim().toLowerCase();
  const cached = CACHE.get(key);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    console.log("[fetchIsraelRealEstate] Cache hit for:", address.slice(0, 50));
    return cached.data;
  }

  console.log("[fetchIsraelRealEstate] Fetching /api/israel-real-estate for:", address.slice(0, 50));
  try {
    const res = await fetch(
      `/api/israel-real-estate?address=${encodeURIComponent(address)}&limit=50`,
      { signal: AbortSignal.timeout(20000) }
    );
    const data: IsraelRealEstateResponse = await res.json().catch(() => ({
      ...FALLBACK_RESPONSE,
      error: "Invalid response",
    }));

    console.log("[fetchIsraelRealEstate] Response:", {
      ok: res.ok,
      status: res.status,
      hasData: !data.error && (data.avgPrice != null || data.lastSalePrice != null),
      error: data.error,
    });

    if (res.ok && !data.error && (data.avgPrice != null || data.lastSalePrice != null)) {
      CACHE.set(key, { data, ts: Date.now() });
    }
    return data;
  } catch (err) {
    console.error("[fetchIsraelRealEstate] Connection failed:", err);
    return FALLBACK_RESPONSE;
  }
}

export function formatSaleDate(dateStr: string | null): string {
  if (!dateStr) return "";
  try {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return dateStr;
    return d.toLocaleDateString("en-US", { month: "short", year: "numeric" });
  } catch {
    return dateStr;
  }
}
