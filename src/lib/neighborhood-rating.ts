/**
 * Neighborhood rating system.
 * Extensible structure for future factors: income, education, population trend,
 * property values, crime, schools, transport, amenities.
 */

export type NeighborhoodRating =
  | "POOR"
  | "FAIR"
  | "GOOD"
  | "VERY GOOD"
  | "EXCELLENT";

/** Input factors for computing rating. Add new fields as data becomes available. */
export type NeighborhoodScoreInputs = {
  /** Median household income (used when available) */
  median_household_income?: number;
  /** Median home value (used when available) */
  median_home_value?: number;
  /** Population count */
  population?: number;
  /** % with bachelor's or higher */
  pct_bachelors_plus?: number;
  /** Population growth % (year over year) */
  population_growth_percent?: number;
  /** Income growth % (year over year) */
  income_growth_percent?: number;
  /** Area price proxy when Census unavailable (e.g. Land Registry average) */
  livability_proxy_from_area_price?: number;
  // Future: crime_index?, school_rating?, transport_score?, amenities_score?
};

/** Map legacy API values to display scale */
export function toDisplayRating(
  legacy: "BAD" | "ALMOST GOOD" | "GOOD" | "VERY GOOD" | "EXCELLENT"
): NeighborhoodRating {
  switch (legacy) {
    case "BAD":
      return "POOR";
    case "ALMOST GOOD":
      return "FAIR";
    case "GOOD":
    case "VERY GOOD":
    case "EXCELLENT":
      return legacy;
    default:
      return "POOR";
  }
}

/**
 * Compute neighborhood rating from score inputs.
 * Extensible: add new factors to the scoring logic as data becomes available.
 */
export function computeNeighborhoodRating(
  inputs: NeighborhoodScoreInputs | null
): NeighborhoodRating {
  const price =
    inputs?.median_home_value ??
    inputs?.livability_proxy_from_area_price ??
    0;
  if (price <= 0) return "POOR";
  if (price >= 750000) return "EXCELLENT";
  if (price >= 500000) return "VERY GOOD";
  if (price >= 350000) return "GOOD";
  if (price >= 200000) return "FAIR";
  return "POOR";
}
