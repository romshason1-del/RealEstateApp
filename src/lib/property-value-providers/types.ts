/**
 * Property Value Provider Types
 * Shared types for the provider-based architecture.
 * All output is English-only.
 */

export type PropertyValueInput = {
  city?: string;
  street?: string;
  houseNumber?: string;
  apartmentNumber?: string;
  /** US: 2-letter state code (e.g. FL) */
  state?: string;
  /** US: ZIP code (e.g. 33139) */
  zip?: string;
  latitude?: number;
  longitude?: number;
  resolvedAddress?: { city: string; street: string; houseNumber?: string; apartmentNumber?: string };
  /** Full address string (e.g. for US RentCast: "123 Main St, City, ST 12345") */
  fullAddress?: string;
};

export type LatestTransaction = {
  transaction_date: string;
  transaction_price: number;
  property_size: number;
  price_per_m2: number;
};

export type CurrentEstimatedValue = {
  estimated_value: number;
  estimated_price_per_m2: number;
  estimation_method: string;
} | null;

export type BuildingSummary = {
  transactions_count_last_3_years: number;
  latest_building_transaction_price: number;
  average_apartment_value_today: number;
} | null;

export type MatchQuality = "exact_building" | "exact_property" | "nearby_building" | "no_reliable_match";

export type PropertyValueInsightsDebug = Record<string, unknown>;

export type PropertyValueInsightsSuccess = {
  address: { city: string; street: string; house_number: string };
  match_quality: MatchQuality;
  latest_transaction: LatestTransaction;
  current_estimated_value: CurrentEstimatedValue;
  building_summary_last_3_years: BuildingSummary;
  explanation?: string;
  debug?: PropertyValueInsightsDebug;
  source: string;
};

export type PropertyValueInsightsNoMatch = {
  message: "no transaction found" | "no reliable exact match found";
};

export type PropertyValueInsightsError = {
  message: string;
  error?: string;
};

export type PropertyValueInsightsResult =
  | PropertyValueInsightsSuccess
  | PropertyValueInsightsNoMatch
  | PropertyValueInsightsError;
