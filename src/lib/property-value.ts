/**
 * Generates realistic mock property values based on coordinates.
 * Different coordinates produce different, deterministic results.
 */
export type PropertyValueResult = {
  /** Formatted price string (e.g. "$1,280,000") */
  formattedValue: string;
  /** Numeric value for calculations */
  valueNumber: number;
  /** Price per square meter */
  pricePerSqm: number;
  /** Year-over-year market trend (e.g. 4.2 for +4.2%) */
  trendYoY: number;
  /** Whether data is from official/verified source */
  isOfficial: boolean;
  /** Assumed property area in sqm for display */
  areaSqm: number;
  /** Currency symbol */
  currencySymbol: string;
};

const BASE_PRICES = [350_000, 420_000, 510_000, 680_000, 890_000, 1_125_000, 1_280_000, 1_410_000];
const BASE_AREAS = [65, 72, 85, 92, 105, 118, 95, 88];
const TREND_RANGE = [-1.2, 2.1, 3.8, 4.2, 5.1, 6.3, 3.2, 4.7];

export function calculatePropertyValue(
  lat: number,
  lng: number,
  currencySymbol = "$"
): PropertyValueResult {
  const seed = Math.abs(
    Math.round(lat * 10000) * 31 +
      Math.round(lng * 10000) * 17 +
      Math.round(lat * 100000) % 7
  );
  const idx = seed % BASE_PRICES.length;
  const basePrice = BASE_PRICES[idx];
  const areaSqm = BASE_AREAS[idx];
  const trendYoY = TREND_RANGE[idx];

  const coordVariance =
    ((Math.abs(Math.round(lat * 100000)) + Math.abs(Math.round(lng * 100000))) % 13 - 6) *
    Math.max(8000, Math.round(basePrice * 0.015));
  const valueNumber = Math.max(100_000, basePrice + coordVariance);

  const pricePerSqm = Math.round(valueNumber / areaSqm);

  return {
    formattedValue: `${currencySymbol}${valueNumber.toLocaleString()}`,
    valueNumber,
    pricePerSqm,
    trendYoY,
    isOfficial: (seed + idx) % 3 !== 0,
    areaSqm,
    currencySymbol,
  };
}
