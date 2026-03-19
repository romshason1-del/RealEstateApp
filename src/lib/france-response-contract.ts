export type FranceResultType =
  | "exact_apartment"
  | "similar_apartment_same_building"
  | "building_level"
  | "nearby_comparable"
  | "no_reliable_data"
  // Backward-compat: older naming used by earlier iterations
  | "building_fallback"
  | "comparables_only"
  | "no_result";

export type FranceConfidence = "high" | "medium_high" | "medium" | "low_medium" | "low";

export type FranceNormalizedProperty = {
  transactionDate?: string | null;
  transactionValue?: number | null;
  pricePerSqm?: number | null;
  surfaceArea?: number | null;
  rooms?: number | null;
  propertyType?: string | null;
  building?: string | null;
  postalCode?: string | null;
  commune?: string | null;
};

export type FranceBuildingStats = {
  transactionCount?: number;
  avgPricePerSqm?: number | null;
  avgTransactionValue?: number | null;
};

export type FranceComparable = {
  date: string | null;
  type: string;
  price: number;
  surface: number | null;
  lot_number?: string | null;
};

export type FranceDebug = {
  searchedAddress?: string;
  normalizedAddress?: string;
  requestedLot?: string | null;
  normalizedLot?: string | null;
  matchedLot?: string | null;
  fallbackSource?: "exact" | "similar_same_building" | "building_level" | "nearby_comparable" | "none";
  fallbackReason?: string | null;
  comparableAddress?: string | null;
  comparableDistanceMeters?: number | null;
  matchExplanation?: string | null;
  similarityScore?: number | null;
  candidateCountSameBuilding?: number;
  candidateCountNearby?: number;
  comparableScope?: "same_street" | "same_postcode_commune" | "same_commune";
  selectedNearbyStrategy?: "street_postcode" | "postcode_commune" | "commune" | null;
  nearbyStageCounts?: { same_street: number; same_postcode_commune: number; same_commune: number };
  nearbyFilterStats?: { raw: number; missingSurface: number; missingDate: number; tooOld: number; trustworthy: number };
  suspiciousPricePerSqm?: boolean;
  suspiciousPricePerSqmValue?: number | null;
  suspiciousPolicy?: "suppressed" | "warning";
  buildingRejectedAsWeak?: boolean;
  exactLotRowCount?: number;
  buildingRowCount?: number;
  comparableRowCount?: number;
  selectedResultType?: string;
  usedFallback?: boolean;
  failureReason?: string | null;
  queryDurationMs?: number | null;
};

export type FrancePropertyResponse = {
  success: boolean;
  country: "fr";
  resultType: FranceResultType;
  confidence: FranceConfidence;
  matchedAddress: string | null;
  normalizedAddress: string | null;
  requestedLot: string | null;
  normalizedLot: string | null;
  matchedLot?: string | null;
  fallbackSource?: FranceDebug["fallbackSource"];
  fallbackReason?: string | null;
  comparableAddress?: string | null;
  comparableDistanceMeters?: number | null;
  comparableScope?: FranceDebug["comparableScope"];
  selectedNearbyStrategy?: FranceDebug["selectedNearbyStrategy"];
  matchExplanation?: string | null;
  property: FranceNormalizedProperty | null;
  buildingStats: FranceBuildingStats | null;
  comparables: FranceComparable[];
  debug: FranceDebug;
};

export function emptyFranceResponse(overrides?: Partial<FrancePropertyResponse>): FrancePropertyResponse {
  return {
    success: false,
    country: "fr",
    resultType: "no_result",
    confidence: "low",
    matchedAddress: null,
    normalizedAddress: null,
    requestedLot: null,
    normalizedLot: null,
    property: null,
    buildingStats: null,
    comparables: [],
    debug: { failureReason: null, queryDurationMs: null },
    ...overrides,
  };
}

