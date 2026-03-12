/**
 * France DVF (Demandes de Valeurs Foncières) Provider
 * Uses official French property transaction data from DGFiP.
 * API: api.cquest.org/dvf (Micro-API DVF, data.gouv.fr reuse)
 * Data: https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
 * Radius: 300m primary, 800m fallback when insufficient data.
 *
 * TEMPORARILY DISABLED: Current DVF source (api.cquest.org) returns 502 Bad Gateway.
 * France is gated in use-property-value-insights (PROVIDER_COUNTRIES) and property-value-card (hasOfficialProvider).
 * Re-enable when a stable DVF source is available.
 */

const DVF_API_BASE = "https://api.cquest.org/dvf";
const RADIUS_PRIMARY_M = 300;
const RADIUS_FALLBACK_M = 800;
const DEFAULT_SQM = 100;
const MIN_TRANSACTIONS_FOR_MEDIAN = 3;

/** Residential types we prefer for valuation (Maison, Appartement) */
const RESIDENTIAL_TYPES = new Set(["Maison", "Appartement", "Local", "Dépendance"]);

export type DVFTransaction = {
  valeur_fonciere?: number;
  valeurfonc?: number;
  date_mutation?: string;
  type_local?: string;
  surface_reelle_bati?: number;
  surface?: number;
  nature_mutation?: string;
  [key: string]: unknown;
};

export type FRDVFResult = {
  latest_transaction: { amount: number; date: string | null; property_type?: string };
  price_per_sqm: number | null;
  street_average: number | null;
  estimated_value: number | null;
  transaction_count: number;
  radius_used_m: number;
};

function parsePrice(val: unknown): number | null {
  if (val == null) return null;
  const n = typeof val === "number" ? val : parseFloat(String(val));
  return Number.isFinite(n) && n > 0 ? n : null;
}

function parseSurface(val: unknown): number | null {
  if (val == null) return null;
  const n = typeof val === "number" ? val : parseFloat(String(val));
  return Number.isFinite(n) && n > 0 ? n : null;
}

function parseDate(val: unknown): string | null {
  if (val == null || typeof val !== "string") return null;
  const s = String(val).trim();
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : s;
}

function isResidential(t: DVFTransaction): boolean {
  const typeLocal = (t.type_local ?? "").toString().trim();
  if (!typeLocal) return true;
  return RESIDENTIAL_TYPES.has(typeLocal);
}

function isVente(t: DVFTransaction): boolean {
  const nature = (t.nature_mutation ?? "").toString().trim().toLowerCase();
  return nature === "vente" || !nature;
}

function extractComparables(rows: DVFTransaction[]): Array<{ price: number; surface: number; pricePerSqm: number; date: string | null; type_local?: string }> {
  const out: Array<{ price: number; surface: number; pricePerSqm: number; date: string | null; type_local?: string }> = [];
  for (const r of rows) {
    if (!isVente(r) || !isResidential(r)) continue;
    const price = parsePrice(r.valeur_fonciere ?? r.valeurfonc);
    const surface = parseSurface(r.surface_reelle_bati ?? r.surface);
    if (price == null || price <= 0) continue;
    const pricePerSqm = surface != null && surface > 0 ? price / surface : null;
    if (pricePerSqm != null && pricePerSqm > 0 && pricePerSqm < 50000) {
      out.push({
        price,
        surface: surface ?? DEFAULT_SQM,
        pricePerSqm,
        date: parseDate(r.date_mutation),
        type_local: (r.type_local as string) ?? undefined,
      });
    } else if (surface == null || surface <= 0) {
      out.push({
        price,
        surface: DEFAULT_SQM,
        pricePerSqm: price / DEFAULT_SQM,
        date: parseDate(r.date_mutation),
        type_local: (r.type_local as string) ?? undefined,
      });
    }
  }
  return out;
}

function median(arr: number[]): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid]! : (sorted[mid - 1]! + sorted[mid]!) / 2;
}

async function fetchDVFByCoords(lat: number, lon: number, radiusM: number): Promise<DVFTransaction[]> {
  const url = `${DVF_API_BASE}?lat=${lat}&lon=${lon}&dist=${radiusM}&nature_mutation=Vente`;
  const res = await fetch(url, {
    signal: AbortSignal.timeout(8000),
    headers: { Accept: "application/json" },
  });
  if (!res.ok) return [];
  const data = await res.json();
  if (Array.isArray(data)) return data as DVFTransaction[];
  if (data?.features && Array.isArray(data.features)) {
    return (data.features as Array<{ properties?: DVFTransaction }>).map((f) => f.properties ?? (f as unknown as DVFTransaction)).filter(Boolean);
  }
  if (data?.results && Array.isArray(data.results)) return data.results as DVFTransaction[];
  return [];
}

/**
 * Fetch DVF transactions and compute valuation metrics.
 * Uses 300m radius first; falls back to 800m if insufficient comparables.
 */
export async function fetchDVFByCoordinates(
  lat: number,
  lon: number,
  _address?: string,
  sqm: number = DEFAULT_SQM
): Promise<FRDVFResult> {
  const empty: FRDVFResult = {
    latest_transaction: { amount: 0, date: null },
    price_per_sqm: null,
    street_average: null,
    estimated_value: null,
    transaction_count: 0,
    radius_used_m: 0,
  };

  let rows = await fetchDVFByCoords(lat, lon, RADIUS_PRIMARY_M);
  let radiusUsed = RADIUS_PRIMARY_M;

  if (rows.length < MIN_TRANSACTIONS_FOR_MEDIAN) {
    rows = await fetchDVFByCoords(lat, lon, RADIUS_FALLBACK_M);
    radiusUsed = RADIUS_FALLBACK_M;
  }

  const comparables = extractComparables(rows);
  if (comparables.length === 0) return { ...empty, radius_used_m: radiusUsed };

  const sortedByDate = [...comparables].sort((a, b) => {
    const da = a.date ? new Date(a.date).getTime() : 0;
    const db = b.date ? new Date(b.date).getTime() : 0;
    return db - da;
  });

  const latest = sortedByDate[0];
  const latestTransaction = {
    amount: latest?.price ?? 0,
    date: latest?.date ?? null,
    property_type: latest?.type_local,
  };

  const pricesPerSqm = comparables.map((c) => c.pricePerSqm).filter((p) => p > 0);
  const pricePerSqm = pricesPerSqm.length >= MIN_TRANSACTIONS_FOR_MEDIAN ? median(pricesPerSqm) : pricesPerSqm.length > 0 ? median(pricesPerSqm) : null;

  const totalPrices = comparables.map((c) => c.price);
  const streetAverage = totalPrices.length > 0 ? Math.round(median(totalPrices)) : null;

  const estimatedValue =
    pricePerSqm != null && pricePerSqm > 0 ? Math.round(pricePerSqm * sqm) : streetAverage != null ? Math.round(streetAverage) : null;

  return {
    latest_transaction: latestTransaction,
    price_per_sqm: pricePerSqm != null ? Math.round(pricePerSqm) : null,
    street_average: streetAverage,
    estimated_value: estimatedValue,
    transaction_count: comparables.length,
    radius_used_m: radiusUsed,
  };
}
