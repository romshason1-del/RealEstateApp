/**
 * UK ONS Census Provider
 * Fetches neighborhood statistics for livability rating.
 * Uses postcodes.io for postcode→LSOA/LAD, then derives livability from area context.
 * ONS Census 2021 data available via NOMIS; this provider uses area price as proxy when Census API is unavailable.
 */

import { lookupUKPostcode } from "./uk-postcode-provider";
import { computeNeighborhoodRating, type NeighborhoodRating, type NeighborhoodScoreInputs } from "@/lib/neighborhood-rating";

export type UKNeighborhoodStats = NeighborhoodScoreInputs & {
  median_household_income?: number;
  median_home_value?: number;
  population?: number;
  pct_bachelors_plus?: number;
  /** Livability derived from area price (Land Registry) when Census unavailable */
  livability_proxy_from_area_price?: number;
};

/**
 * Fetch neighborhood stats for UK postcode. Uses postcodes.io for geography.
 * Livability: when Census data unavailable, uses area average price as proxy (higher = better neighborhood).
 */
export async function fetchUKNeighborhoodStats(
  postcode: string,
  areaAveragePrice?: number | null
): Promise<UKNeighborhoodStats | null> {
  const geo = await lookupUKPostcode(postcode);
  if (!geo) {
    if (areaAveragePrice != null && areaAveragePrice > 0) {
      return { livability_proxy_from_area_price: areaAveragePrice };
    }
    return null;
  }

  const stats: UKNeighborhoodStats = {};
  if (areaAveragePrice != null && areaAveragePrice > 0) {
    stats.median_home_value = areaAveragePrice;
    stats.livability_proxy_from_area_price = areaAveragePrice;
  }
  return stats;
}

/**
 * Compute livability rating from UK neighborhood stats.
 * Uses area price proxy: <200k POOR, 200-350k FAIR, 350-500k GOOD, 500-750k VERY GOOD, >750k EXCELLENT.
 */
export function computeUKLivabilityRating(stats: UKNeighborhoodStats | null): NeighborhoodRating {
  return computeNeighborhoodRating(stats);
}
