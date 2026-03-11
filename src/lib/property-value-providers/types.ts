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
  /** US: ZIP code (e.g. 33139). UK: postcode (e.g. SW1A 2AA) */
  zip?: string;
  /** UK: postcode if available (e.g. SW1A 2AA) */
  postcode?: string;
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
  /** "street_median" = median of nearby sales; use "Estimated Street Value" label */
  value_type?: "sale" | "rent" | "street_median";
} | null;

export type BuildingSummary = {
  transactions_count_last_3_years: number;
  transactions_count_last_5_years?: number;
  latest_building_transaction_price: number;
  average_apartment_value_today: number;
} | null;

/** How market value was derived. For US: only "avm" or "none" - never use last sale. */
export type MarketValueSource =
  | "avm"
  | "exact_transaction"
  | "exact_provider"
  | "price_per_m2_x_size"
  | "street_median"
  | "none";

/** US: Last sale record (never used as market value) */
export type LastSaleRecord = {
  price: number;
  date: string;
};

/** Global: Most recent recorded sale for the searched property. Country-agnostic. */
export type LastRecordedSale = {
  price: number;
  date: string;
  source?: string;
};

/** US: Property details from provider */
export type PropertyDetails = {
  beds?: number;
  baths?: number;
  sqft?: number;
  year_built?: number;
  property_type?: string;
};

/** US: Sales history entry */
export type SalesHistoryEntry = {
  date: string;
  price: number;
};

/** US: Nearby comps summary */
export type NearbyCompsSummary = {
  avg_price: number;
  avg_price_per_sqft: number;
  count: number;
};

/** Data quality / fallback level */
export type FallbackLevel =
  | "exact_building"
  | "exact_property"
  | "building_fallback"
  | "street_fallback"
  | "none";

export type MatchQuality = "exact_building" | "exact_property" | "nearby_building" | "no_reliable_match";

export type PropertyValueInsightsDebug = Record<string, unknown>;

export type PropertyValueInsightsSuccess = {
  address: { city: string; street: string; house_number: string };
  match_quality: MatchQuality;
  latest_transaction: LatestTransaction;
  current_estimated_value: CurrentEstimatedValue;
  building_summary_last_3_years: BuildingSummary;
  market_value_source?: MarketValueSource;
  fallback_level?: FallbackLevel;
  explanation?: string;
  debug?: PropertyValueInsightsDebug;
  source: string;
  /** US: AVM market value only - never last sale */
  avm_value?: number;
  /** US: AVM rent estimate */
  avm_rent?: number;
  /** US: Last sale (never as market value) */
  last_sale?: LastSaleRecord;
  /** US: Sales history from property */
  sales_history?: SalesHistoryEntry[];
  /** US: Nearby comparable sales summary */
  nearby_comps?: NearbyCompsSummary;
  /** US: Property details */
  property_details?: PropertyDetails;
  /** US: Building requires unit number for condo/multi-unit */
  unit_required?: boolean;
  /** US: Extended market data (Zillow/Redfin fallback) */
  estimated_area_price?: number | null;
  median_sale_price?: number | null;
  median_price_per_sqft?: number | null;
  market_trend?: { change_1y_percent: number } | null;
  inventory_signal?: number | null;
  days_on_market?: number | null;
  data_sources?: ("RentCast" | "Zillow" | "Redfin" | "Census")[];
  /** US: high = RentCast + market data, medium = Zillow+Redfin agreement, low = single-source regional */
  us_match_confidence?: "high" | "medium" | "low";
  /** Global: Most recent recorded sale for the searched property when available */
  last_recorded_sale?: LastRecordedSale;
  /** UK: Land Registry Price Paid Data */
  uk_land_registry?: {
    /** Average price on same street (Land Registry) */
    street_average_price?: number | null;
    /** Average sale price for the exact building (last 5 years). Null if insufficient data. */
    building_average_price: number | null;
    /** Number of valid transactions in the building (last 5 years). */
    transactions_in_building: number;
    /** Most recent sale in the building. Null if none. */
    latest_building_transaction: { price: number; date: string; property_type?: string } | null;
    /** Most recent sale in postcode/area (when no building match). Null if building match or no area data. */
    latest_nearby_transaction?: { price: number; date: string; property_type?: string } | null;
    /** True when exact or fuzzy building match exists. False when showing area-level fallback. */
    has_building_match: boolean;
    /** Area average price (postcode or fallback level). Null if no area data. */
    average_area_price: number | null;
    /** Area median price (Land Registry only; HPI reports average). Null when from HPI. */
    median_area_price?: number | null;
    /** Price trend (YoY change). From HPI or derived from transactions. */
    price_trend?: { change_1y_percent: number; ref_month?: string } | null;
    /** Data source for area metrics: "land_registry" | "HPI" */
    area_data_source?: "land_registry" | "HPI";
    /** Number of transactions used for area average (last 5 years). */
    area_transaction_count: number;
    /** Fallback level when postcode returned 0: street | locality | outward_postcode | postcode_area | postcode | none */
    area_fallback_level: "postcode" | "outward_postcode" | "postcode_area" | "street" | "locality" | "none";
    /** Display level used: building | postcode | locality | area */
    fallback_level_used?: "building" | "postcode" | "locality" | "area";
    /** Confidence label: high = building match, medium = postcode/locality fallback, low = area-only fallback */
    match_confidence?: "high" | "medium" | "low";
  };
}

export type PropertyValueInsightsNoMatch = {
  message: "no transaction found" | "no reliable exact match found";
  debug?: PropertyValueInsightsDebug;
};

export type PropertyValueInsightsError = {
  message: string;
  error?: string;
  debug?: PropertyValueInsightsDebug;
};

export type PropertyValueInsightsResult =
  | PropertyValueInsightsSuccess
  | PropertyValueInsightsNoMatch
  | PropertyValueInsightsError;
