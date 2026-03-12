/**
 * Italy Market Listing Provider
 * Returns listing-based €/m² for market realism layer.
 * Structure ready for integration when a free listing data source is available.
 * Currently returns null (no free public API for Italy listings).
 */

export type ItalyMarketResult = {
  listing_price_per_sqm: number;
  listing_source: string;
  listing_confidence: "low" | "medium" | "high";
};

/**
 * Fetch listing median €/m² around the address.
 * Returns null when no free data source is available.
 */
export async function fetchItalyMarketListings(
  _lat: number,
  _lng: number,
  _city: string,
  _address?: string
): Promise<ItalyMarketResult | null> {
  return null;
}
