"use client";

export type IsraelTransaction = {
  price: number;
  date: string | null;
};

export type IsraelRealEstateResponse = {
  transactions: IsraelTransaction[];
  avgPrice: number | null;
  avgPricePerSqm: number | null;
  officialPropertySqm?: number;
  lastSaleDate: string | null;
  lastSalePrice: number | null;
  transactionCount: number;
  source: string;
  isCityFallback?: boolean;
  isNeighborhoodEstimate?: boolean;
  error?: string;
};

const CACHE = new Map<string, { data: IsraelRealEstateResponse; ts: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000;

const FALLBACK_RESPONSE: IsraelRealEstateResponse = {
  transactions: [],
  avgPrice: null,
  avgPricePerSqm: null,
  lastSaleDate: null,
  lastSalePrice: null,
  transactionCount: 0,
  source: "data.gov.il",
  error: "Connection failed",
};

export async function fetchIsraelRealEstate(address: string, propertyAreaSqm?: number): Promise<IsraelRealEstateResponse> {
  const key = address.trim().toLowerCase();
  const cached = CACHE.get(key);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    console.log("[fetchIsraelRealEstate] Cache hit for:", address.slice(0, 50));
    return cached.data;
  }

  console.log("[fetchIsraelRealEstate] Fetching /api/israel-real-estate for:", address.slice(0, 50));
  try {
    const params = new URLSearchParams({ address, limit: "60" });
    if (propertyAreaSqm != null && propertyAreaSqm > 0) params.set("propertyAreaSqm", String(propertyAreaSqm));
    const res = await fetch(
      `/api/israel-real-estate?${params.toString()}`,
      { signal: AbortSignal.timeout(20000) }
    );
    const data: IsraelRealEstateResponse = await res.json().catch(() => ({
      ...FALLBACK_RESPONSE,
      transactionCount: 0,
      error: "Invalid response",
    }));
    if (data.transactionCount == null) data.transactionCount = data.transactions?.length ?? 0;

    console.log("[fetchIsraelRealEstate] Response:", {
      ok: res.ok,
      status: res.status,
      hasData: !data.error && (data.avgPrice != null || data.lastSalePrice != null),
      error: data.error,
    });

    if (res.ok && !data.error && (data.avgPrice != null || data.lastSalePrice != null || data.avgPricePerSqm != null)) {
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

export function formatSaleYear(dateStr: string | null): string {
  if (!dateStr) return "";
  try {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) {
      const m = /\d{4}/.exec(dateStr);
      return m ? m[0] : "";
    }
    return String(d.getFullYear());
  } catch {
    const m = /\d{4}/.exec(dateStr);
    return m ? m[0] : "";
  }
}
