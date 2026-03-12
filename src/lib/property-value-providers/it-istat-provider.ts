/**
 * Italy ISTAT Provider
 * Fetches area statistics for neighborhood rating.
 * Uses OMI area price as livability proxy when ISTAT API data is unavailable.
 * ISTAT SDMX API: https://www.istat.it/en/classifications-and-tools/sdmx-web-services/
 */

import type { NeighborhoodScoreInputs } from "@/lib/neighborhood-rating";

export type ITISTATStats = NeighborhoodScoreInputs & {
  population?: number;
  median_household_income?: number;
};

/**
 * Fetch ISTAT neighborhood stats for an Italian comune.
 * For MVP: uses OMI area price as livability proxy (same pattern as UK with Land Registry).
 * Future: integrate ISTAT SDMX API for population, income, education.
 */
export async function fetchITISTATStats(
  comune: string,
  omiAreaPrice?: number | null
): Promise<ITISTATStats | null> {
  const stats: ITISTATStats = {};

  if (omiAreaPrice != null && omiAreaPrice > 0) {
    stats.median_home_value = omiAreaPrice;
    stats.livability_proxy_from_area_price = omiAreaPrice;
  }

  return Object.keys(stats).length > 0 ? stats : null;
}
