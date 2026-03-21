"use client";

import * as React from "react";
import { Calendar, Database, Star, X } from "lucide-react";
import { createPortal } from "react-dom";
import { usePropertyValueInsights } from "@/hooks/use-property-value-insights";
import type { FrancePropertyResponse } from "@/lib/france-response-contract";
import {
  coerceConfidenceLabel,
  coerceDisplayString,
  coerceFiniteNumber,
  coerceNullableString,
  coercePositiveNumber,
  formatFranceEuroPerSqmFromUnknown,
  formatFranceEuroTotal,
  normalizeFrancePricePerSqmForDisplay,
  unwrapScalar,
} from "@/lib/fr-display-safe";

type FranceSheetProps = {
  address: string;
  position: { lat: number; lng: number };
  postcode?: string;
  typedAddressForFrance?: string;
  rawInputAddressForFrance?: string;
  currencySymbol?: string;
  onClose: () => void;
};

/** API `fr_valuation_display` envelope (France property-value route). */
export type FranceValuationDisplay = {
  winning_source_label?: string | null;
  winning_step?: string | null;
  confidence?: string | null;
  estimated_value?: number | null;
  price_per_m2?: number | null;
  display_value?: number | null;
  display_value_type?: string | null;
  has_display_value?: boolean;
  /** Back-compat alias; API may send both. */
  source_label?: string | null;
};

export function FranceApartmentSheet({
  address,
  position,
  postcode,
  typedAddressForFrance,
  rawInputAddressForFrance,
  currencySymbol: _currencySymbol = "€",
  onClose,
}: FranceSheetProps) {
  const isDev = process.env.NODE_ENV !== "production";
  const addressForApi = (typedAddressForFrance?.trim() || address?.trim() || "").trim();
  const rawForApi = (rawInputAddressForFrance?.trim() || typedAddressForFrance?.trim() || addressForApi || address?.trim() || "").trim();

  const [lotInput, setLotInput] = React.useState("");
  const [requestedLot, setRequestedLot] = React.useState<string | undefined>(undefined);
  const [trigger, setTrigger] = React.useState(0);
  const [isLotFocused, setIsLotFocused] = React.useState(false);
  const [keyboardInsetPx, setKeyboardInsetPx] = React.useState(0);
  const [hasSubmittedLotSearch, setHasSubmittedLotSearch] = React.useState(false);
  const [isResultCardOpen, setIsResultCardOpen] = React.useState(false);
  const [isMoreDetailsOpen, setIsMoreDetailsOpen] = React.useState(false);
  const [resolvedForDisplay, setResolvedForDisplay] = React.useState<{
    fr: FrancePropertyResponse | null;
    legacy: { averageBuildingValue: number | null; livabilityRating?: string | null } | null;
    fr_runtime_debug?: Record<string, unknown> | null;
  } | null>(null);
  const [isExpanded, setIsExpanded] = React.useState(false);
  const [showOptionalAptInput, setShowOptionalAptInput] = React.useState(false);
  // TEMP: default-open in non-production to make this investigation obvious.
  const [isFranceDebugOpen, setIsFranceDebugOpen] = React.useState(isDev);
  const dragStartYRef = React.useRef<number | null>(null);
  const dragToggledRef = React.useRef(false);

  const { data, isLoading, refetch } = usePropertyValueInsights(addressForApi || rawForApi, "FR", {
    latitude: position.lat,
    longitude: position.lng,
    aptNumber: requestedLot ?? (hasSubmittedLotSearch ? lotInput : undefined),
    postcode,
    refetchTrigger: trigger,
    countryCode: "FR",
    rawInputAddress: rawForApi || undefined,
  });

  React.useEffect(() => {
    if (!isDev) return;
    const d: any = data;
    console.log("[FR_LOT_UI] response tag received", d?.fr?.resultType ?? d?.result_level ?? d?.message ?? null);
    if (d?.data_source === "properties_france") {
      const fr = d?.fr as FrancePropertyResponse | undefined;
      console.log("[FranceSheet] response", {
        result_level: d?.result_level,
        apartment_not_matched: d?.apartment_not_matched,
        exact_lot_row_count: d?.exact_lot_row_count,
        fr_resultType: fr?.resultType,
        fr_confidence: fr?.confidence,
        fr_debug: fr?.debug,
      });
    }
  }, [data, isDev]);

  /** Temporary France exact-path price trace (raw → API → formatter); dev-only. */
  React.useEffect(() => {
    if (!isDev || data == null || typeof data !== "object") return;
    const d = data as Record<string, unknown>;
    const rd = d.fr_runtime_debug as Record<string, unknown> | undefined;
    const ws = String(rd?.winning_step ?? "");
    if (ws !== "exact_unit" && ws !== "exact_address" && ws !== "exact_house") return;
    const fv = d.fr_valuation_display as Record<string, unknown> | undefined;
    const fr = d.fr as { property?: { transactionValue?: unknown } } | undefined;
    const uiInput =
      coercePositiveNumber(fr?.property?.transactionValue as unknown) ??
      coercePositiveNumber(fv?.last_sale_price as unknown);
    console.log("[FR_PRICE] ui_input_last_sale_price=" + String(uiInput ?? "(null)"));
    if (uiInput != null && uiInput > 0) {
      console.log("[FR_PRICE] final_display_last_sale_price=" + formatFranceEuroTotal(uiInput));
    }
  }, [data, isDev]);

  // Once we have a building payload for this address, never render a no-data France state.
  const lastBuildingPayloadRef = React.useRef<typeof data>(null);
  const addressKey = React.useMemo(() => `${addressForApi.trim().toLowerCase()}|pc:${(postcode ?? "").trim().toLowerCase()}`, [addressForApi, postcode]);
  React.useEffect(() => {
    lastBuildingPayloadRef.current = null;
    setRequestedLot(undefined);
    setLotInput("");
    setHasSubmittedLotSearch(false);
    setIsResultCardOpen(false);
    setIsMoreDetailsOpen(false);
    setIsFranceDebugOpen(isDev);
    setShowOptionalAptInput(false);
    setResolvedForDisplay(null);
    setIsExpanded(false);
  }, [addressKey]);

  // Prevent any "house-direct" result-card auto-open until we have resolved the initial
  // address detection request for the current address selection.
  const [frAddressFetchStarted, setFrAddressFetchStarted] = React.useState(false);
  const [frAddressFetchDone, setFrAddressFetchDone] = React.useState(false);
  React.useEffect(() => {
    setFrAddressFetchStarted(false);
    setFrAddressFetchDone(false);
  }, [addressKey]);
  React.useEffect(() => {
    if (isLoading) setFrAddressFetchStarted(true);
  }, [isLoading]);
  React.useEffect(() => {
    if (frAddressFetchStarted && !isLoading) setFrAddressFetchDone(true);
  }, [frAddressFetchStarted, isLoading]);

  const isFranceBuildingPayload = React.useCallback((d: typeof data) => {
    if (!d || typeof d !== "object") return false;
    const r = d as {
      data_source?: string;
      result_level?: string;
      multiple_units?: boolean;
      available_lots?: unknown[];
      building_sales?: unknown[];
      average_building_value?: number;
      property_result?: { value_level?: string; exact_value?: number | null; street_average?: number | null };
    };
    if (r.data_source !== "properties_france") return false;
    if (r.multiple_units === true) return true;
    if (r.result_level === "building") return true;
    if (Array.isArray(r.available_lots) && r.available_lots.length > 0) return true;
    if (Array.isArray(r.building_sales) && r.building_sales.length > 0) return true;
    if ((r.average_building_value ?? 0) > 0) return true;
    if (r.property_result?.value_level === "building-level" && ((r.property_result.exact_value ?? 0) > 0 || (r.property_result.street_average ?? 0) > 0)) return true;
    return false;
  }, []);

  React.useEffect(() => {
    if (!isLoading && isFranceBuildingPayload(data)) {
      lastBuildingPayloadRef.current = data;
    }
  }, [isLoading, data, isFranceBuildingPayload]);

  const isExactApartment = (d: typeof data) => {
    if (!d || typeof d !== "object") return false;
    const r = d as any;
    if (r.data_source !== "properties_france") return false;
    const rt = r.fr?.resultType as string | undefined;
    if (rt === "exact_apartment" || rt === "exact_address" || rt === "exact_house") return true;
    return r.result_level === "exact_property" && r.property_result?.value_level === "property-level";
  };

  const isFranceBuildingSimilarWin = React.useCallback((d: typeof data) => {
    if (!d || typeof d !== "object") return false;
    const r = d as any;
    return r.data_source === "properties_france" && r.fr?.resultType === "building_similar_unit";
  }, []);

  const franceResultPriority = React.useCallback((d: unknown): number => {
    const rt = (d as any)?.fr?.resultType as string | undefined;
    switch (rt) {
      case "exact_apartment":
      case "exact_address":
      case "exact_house":
        return 5;
      case "similar_apartment_same_building":
        return 4;
      case "building_similar_unit":
      case "building_level":
      case "building_fallback":
        return 3;
      case "nearby_comparable":
      case "comparables_only":
        return 2;
      case "no_result":
      default:
        return 1;
    }
  }, []);

  const effectiveData = React.useMemo(() => {
    // Priority order in UI layer: exact > similar > building > nearby > no_result.
    // Never let older/weaker sticky building payload override a newer/stronger result.
    if (data && lastBuildingPayloadRef.current) {
      const a = franceResultPriority(data);
      const b = franceResultPriority(lastBuildingPayloadRef.current);
      return a >= b ? data : lastBuildingPayloadRef.current;
    }
    return data ?? lastBuildingPayloadRef.current ?? null;
  }, [data, franceResultPriority]);

  const parsed = effectiveData as any;
  const normalized = (parsed?.fr ?? null) as FrancePropertyResponse | null;
  const availableLots: string[] = Array.isArray(parsed?.available_lots) ? parsed.available_lots : [];
  const buildingSales = Array.isArray(parsed?.building_sales) ? parsed.building_sales : [];
  const averageBuildingValue = typeof parsed?.average_building_value === "number" ? parsed.average_building_value : null;
  const resultLevel = parsed?.result_level as string | undefined;
  const pr = parsed?.property_result as any | undefined;
  const legacyLivability = (pr?.livability_rating ?? null) as string | null;

  // Infer whether this FR property is a single-unit house ("maison") or a multi-unit building ("appartement"-like).
  // Used only for the France input UX (hide apartment/lot input for houses).
  const buildingTypeStrings: string[] = buildingSales
    .map((s: any) => String(s?.type ?? "").trim().toLowerCase())
    .filter((v: string) => Boolean(v));
  const hasMaisonType = buildingTypeStrings.some((t: string) => t.includes("maison"));
  const hasAppartementType = buildingTypeStrings.some((t: string) => t.includes("appartement"));
  const hasNoLots = availableLots.length === 0;
  const lowMultiUnitHeuristic = buildingSales.length <= 1;
  // UI-only inference:
  // If we have no lot inventory, no explicit "appartement" type, and the address doesn't look like
  // a dense multi-unit pattern (low result count / single transaction), treat it as a private house.
  const frDetect = typeof parsed?.fr_detect === "string" ? (parsed?.fr_detect as string) : undefined;
  const isHouseInferredByHeuristic = frDetect === "house" && hasNoLots && !hasAppartementType && lowMultiUnitHeuristic;

  const isHouseDetected =
    (frDetect === "house" && parsed?.multiple_units === false && availableLots.length === 0) ||
    (hasMaisonType && !hasAppartementType) ||
    isHouseInferredByHeuristic;
  const isApartmentLikely = parsed?.multiple_units === true || availableLots.length > 0 || hasAppartementType;
  const isPropertyTypeUnknown = !isHouseDetected && !isApartmentLikely;
  const propertyTypeHint = String((normalized?.property?.propertyType ?? pr?.property_type ?? "")).toLowerCase();
  const isApartmentTypeHint = propertyTypeHint.includes("appart");
  const backendMultiUnitFlag = parsed?.multiple_units === true || parsed?.prompt_for_apartment === true;
  const hasMultiUnitEvidence =
    availableLots.length > 0 ||
    hasAppartementType ||
    backendMultiUnitFlag ||
    isApartmentTypeHint;

  // UI-only override: some "house-like" addresses can be classified as multi-unit due to sparse DVF type info.
  // If the payload looks like a small inventory (low counts), treat as house-like for copy only.
  const isHouseLikeOverride =
    frDetect === "house" && buildingSales.length <= 5 && availableLots.length < 20 && !hasMultiUnitEvidence;
  const isApartmentLikeForLotFirst = hasMultiUnitEvidence;
  const isHouseLikeUI = isHouseDetected || (isHouseLikeOverride && !isApartmentLikeForLotFirst);
  const shouldForceLotFirstFlow = isApartmentLikeForLotFirst && !isHouseLikeUI;
  const shouldShowApartmentInput = !isHouseLikeUI && hasMultiUnitEvidence;

  // Effective detect class as seen by the UI (uses the same fallback logic as the debug marker).
  const effectiveDetectClass: "apartment" | "house" | "unclear" =
    backendMultiUnitFlag
      ? "apartment"
      : frDetect === "apartment" || frDetect === "house" || frDetect === "unclear"
        ? frDetect
        : shouldForceLotFirstFlow
          ? "apartment"
          : isHouseLikeUI
            ? "house"
            : "unclear";
  React.useEffect(() => {
    if (!isDev) return;
    console.log("[FR_UI] apartment_vs_house_decision", {
      isHouseDetected,
      isHouseLikeUI,
      shouldForceLotFirstFlow,
      isApartmentLikely,
      isApartmentLikeForLotFirst,
      hasMultiUnitEvidence,
      backendMultiUnitFlag,
      multipleUnits: parsed?.multiple_units === true,
      availableLots: availableLots.length,
      hasAppartementType,
      isApartmentTypeHint,
    });
  }, [isDev, isHouseDetected, isHouseLikeUI, shouldForceLotFirstFlow, isApartmentLikely, isApartmentLikeForLotFirst, hasMultiUnitEvidence, backendMultiUnitFlag, parsed?.multiple_units, availableLots.length, hasAppartementType, isApartmentTypeHint]);

  const uiState = isResultCardOpen
    ? "result-visible"
    : shouldShowApartmentInput
      ? "apartment-input"
      : isHouseLikeUI
        ? "house-direct"
        : "house-direct";

  React.useEffect(() => {
    if (!isDev) return;
    console.log("[FR_UI_STATE]", { uiState });
  }, [isDev, uiState]);

  const resultCardBlocked = effectiveDetectClass === "apartment" && !(requestedLot ?? "").trim();

  // Strict UI gate for apartment-first flow:
  // if apartment is detected and user hasn't submitted a lot yet,
  // keep the result card closed and never allow "searched" phases to render.
  React.useEffect(() => {
    if (effectiveDetectClass !== "apartment") return;
    if (!resultCardBlocked) return;

    if (isDev) {
      console.log("[FR_UI_DEBUG]", {
        detectClass: effectiveDetectClass,
        hasSubmittedLot: hasSubmittedLotSearch,
        resultCardBlocked: true,
      });
    }

    if (hasSubmittedLotSearch) setHasSubmittedLotSearch(false);
    if (isResultCardOpen) setIsResultCardOpen(false);
    if (resolvedForDisplay != null) setResolvedForDisplay(null);
    if (isMoreDetailsOpen) setIsMoreDetailsOpen(false);
    if (isExpanded) setIsExpanded(false);
  }, [effectiveDetectClass, resultCardBlocked, hasSubmittedLotSearch, isResultCardOpen, resolvedForDisplay, isMoreDetailsOpen, isExpanded, isDev]);

  const displayConfidence = React.useMemo(() => {
    return coerceConfidenceLabel(normalized?.confidence as unknown);
  }, [normalized?.confidence]);

  const sourceLabel = React.useMemo(() => {
    const rt = normalized?.resultType;
    if (rt === "exact_apartment") return isHouseLikeUI ? "Exact property match" : "Exact lot match";
    if (rt === "exact_address") return "Exact address match";
    if (rt === "exact_house") return "Exact house match";
    if (rt === "similar_apartment_same_building")
      return isHouseLikeUI ? "Similar house in this building" : "Similar apartment in this building";
    if (rt === "building_similar_unit") return "Similar apartments in this building";
    if (rt === "building_level" || rt === "building_fallback")
      return "Similar properties in this building";
    if (rt === "nearby_comparable" || rt === "comparables_only") {
      const scope = normalized?.comparableScope;
      if (isHouseLikeUI) return "Nearby comparable";
      if (scope === "same_street") return "Same-street comparable";
      if (scope === "same_postcode_commune") return "Same-postcode comparable";
      if (scope === "same_commune") return "Same-commune comparable";
      return "Nearby comparable";
    }
    return null;
  }, [normalized?.resultType, normalized?.comparableScope, isHouseLikeUI]);

  const valueLabel = React.useMemo(() => {
    const rt = normalized?.resultType;
    if (rt === "exact_apartment") return "Last recorded transaction";
    if (rt === "exact_address") return "Address-level transaction";
    if (rt === "exact_house") return "Last recorded transaction";
    if (rt === "similar_apartment_same_building") return "Similar apartment transaction";
    if (rt === "building_similar_unit") return "Building apartment estimate";
    if (rt === "building_level" || rt === "building_fallback") return "Building average";
    if (rt === "nearby_comparable" || rt === "comparables_only") return "Best available nearby comparable";
    return null;
  }, [normalized?.resultType]);

  const matchDetails = React.useMemo(() => {
    const requested = coerceNullableString(normalized?.requestedLot as unknown);
    const matched = coerceNullableString(normalized?.matchedLot as unknown);
    if (!requested && !matched) return null;
    if (requested && matched && requested !== matched) {
      return { requested, matched, differs: true };
    }
    return { requested: requested ?? null, matched: matched ?? null, differs: false };
  }, [normalized?.requestedLot, normalized?.matchedLot]);

  const hasUsefulBuildingData =
    (Array.isArray(buildingSales) && buildingSales.length > 0) ||
    ((averageBuildingValue ?? 0) > 0) ||
    (Array.isArray(availableLots) && availableLots.length > 0) ||
    (parsed?.multiple_units === true) ||
    (isFranceBuildingSimilarWin(effectiveData) &&
      (((parsed?.property_result?.exact_value as number) ?? 0) > 0 ||
        (coercePositiveNumber(normalized?.property?.pricePerSqm as unknown) ?? 0) > 0));

  type Phase =
    | "initial_building_state"
    | "exact_apartment_match_state"
    | "building_similar_match_state"
    | "searched_no_exact_match_but_building_exists_state"
    | "no_result_state";

  const apartmentLotGateActive = effectiveDetectClass === "apartment" && !(requestedLot ?? "").trim();

  const phase: Phase = React.useMemo(() => {
    if (apartmentLotGateActive) return "initial_building_state";
    if (isExactApartment(effectiveData)) return "exact_apartment_match_state";
    if (isFranceBuildingSimilarWin(effectiveData)) return "building_similar_match_state";
    if (!hasSubmittedLotSearch) {
      return hasUsefulBuildingData ? "initial_building_state" : "no_result_state";
    }
    return hasUsefulBuildingData ? "searched_no_exact_match_but_building_exists_state" : "no_result_state";
  }, [effectiveData, hasSubmittedLotSearch, hasUsefulBuildingData, apartmentLotGateActive, isFranceBuildingSimilarWin]);

  // Houses/single-unit properties should not require apartment/lot input.
  // As soon as we have enough data to infer "house", open the results directly from the address.
  React.useEffect(() => {
    // Fallback: if apartment input is not shown, ensure the user always gets a usable next step
    // by opening the results (house-direct/implicit resolved state) rather than leaving the sheet inactive.
    // If backend indicates a multi-unit building (apartment/lot-first), never auto-open results.
    // This guarantees we don't show a final result/no-data card before the user submits a lot.
    if (parsed?.multiple_units === true || parsed?.prompt_for_apartment === true) return;
    if (!frAddressFetchDone) return;
    if (effectiveDetectClass === "apartment") return;
    if (shouldShowApartmentInput) return;
    if (hasSubmittedLotSearch) return;
    if (isLoading) return;
    if (isDev) console.log("[FR_UI] direct_house_flow_chosen");
    setHasSubmittedLotSearch(true);
    setIsResultCardOpen(true);
  }, [shouldShowApartmentInput, hasSubmittedLotSearch, isLoading, isDev, frDetect, frAddressFetchDone, effectiveDetectClass, parsed?.multiple_units, parsed?.prompt_for_apartment]);

  React.useEffect(() => {
    if (!isDev) return;
    if (shouldForceLotFirstFlow) {
      console.log("[FR_UI] lot_first_flow_triggered");
    }
  }, [isDev, shouldForceLotFirstFlow]);

  const badge = React.useMemo(() => {
    if (phase === "exact_apartment_match_state") {
      if (normalized?.resultType === "exact_house") {
        return { label: "Exact house match", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
      }
      if (normalized?.resultType === "exact_address") {
        return { label: "Exact address match", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
      }
      return isHouseLikeUI
        ? { label: "Exact house match", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" }
        : { label: "Exact apartment match", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
    }
    if (phase === "building_similar_match_state") {
      return {
        label: "Similar apartments in this building",
        tone: "bg-teal-500/10 border-teal-500/25 text-teal-200",
      };
    }
    if (phase === "searched_no_exact_match_but_building_exists_state") {
      const rt = normalized?.resultType;
      if (rt === "similar_apartment_same_building") {
        return isHouseLikeUI
          ? { label: "Similar house in this building", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" }
          : { label: "Similar apartment in this building", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
      }
      if (rt === "nearby_comparable") {
        return isHouseLikeUI
          ? { label: "Nearby comparable houses", tone: "bg-sky-500/10 border-sky-500/25 text-sky-200" }
          : { label: "Nearby comparable", tone: "bg-sky-500/10 border-sky-500/25 text-sky-200" };
      }
      return { label: "Result", tone: "bg-amber-500/10 border-amber-500/25 text-amber-200" };
    }
    return null;
  }, [phase, normalized?.resultType, isHouseLikeUI]);

  const subtitle = React.useMemo(() => {
    if (isHouseLikeUI) return "Showing the best available value for this single-unit property.";
    if (isPropertyTypeUnknown && phase === "initial_building_state") {
      return "Building data found. Enter a property / lot number (if applicable) for an exact match.";
    }
    if (phase === "initial_building_state") {
      return "Building data found. Enter a property / lot number for an exact match.";
    }
    if (phase === "exact_apartment_match_state") {
      const lot = (requestedLot ?? lotInput).trim();
      if (normalized?.resultType === "exact_address") {
        return lot
          ? `Matched address for lot ${lot}; unit not confirmed in government data — showing address-level record.`
          : "Matched address; unit not confirmed in government data.";
      }
      if (isHouseLikeUI) return lot ? `Matched house / lot ${lot}` : "Matched house / lot";
      return lot ? `Matched apartment / lot ${lot}` : "Matched apartment / lot";
    }
    if (phase === "building_similar_match_state") {
      const lot = (requestedLot ?? "").trim();
      return lot
        ? `No exact unit in registry for lot ${lot}. Estimate from similar apartments at this address.`
        : "No exact unit in registry. Estimate from similar apartments at this address.";
    }
    if (phase === "searched_no_exact_match_but_building_exists_state") {
      const lot = (requestedLot ?? "").trim();
      const rt = normalized?.resultType;
      const ws = String((parsed as any)?.fr_runtime_debug?.winning_step ?? "").trim();
      if (ws === "street_fallback") {
        return lot
          ? `No exact match for lot ${lot}. Estimates use similar properties on the same street.`
          : "No exact lot match. Estimates use similar properties on the same street.";
      }
      if (ws === "commune_fallback") {
        return lot
          ? `No exact match for lot ${lot}. Estimates use similar properties in the same commune.`
          : "No exact lot match. Estimates use similar properties in the same commune.";
      }
      if (ws === "building_level" || ws === "building_fallback") {
        return lot
          ? `No exact match for lot ${lot}. Estimates use similar properties in this building.`
          : "No exact lot match. Estimates use similar properties in this building.";
      }
      if (isHouseLikeUI) {
        if (rt === "similar_apartment_same_building" || rt === "nearby_comparable") {
          return lot
            ? `No exact property match for lot ${lot}. Based on nearby property transactions.`
            : "No exact property match. Based on nearby property transactions.";
        }
        return lot
          ? `No exact property match for lot ${lot} — showing building transaction data.`
          : "No exact property match — showing building transaction data.";
      }
      if (rt === "similar_apartment_same_building") {
        return lot
          ? `No exact apartment match for lot ${lot} — showing the closest apartment in this building.`
          : "No exact apartment match — showing the closest apartment in this building.";
      }
      if (rt === "nearby_comparable") {
        return lot
          ? `No exact apartment match for lot ${lot} — showing a similar nearby apartment.`
          : "No exact apartment match — showing a similar nearby apartment.";
      }
      return lot
        ? `No exact apartment match for lot ${lot} — showing building transaction data.`
        : "No exact apartment match — showing building transaction data.";
    }
    return isHouseLikeUI ? "No matching property or building transaction data found." : "No matching apartment or building transaction data found.";
  }, [phase, requestedLot, lotInput, normalized?.resultType, isHouseLikeUI, isPropertyTypeUnknown, parsed?.fr_runtime_debug]);

  const submit = React.useCallback((source: "enter" | "button") => {
    const lot = lotInput.trim();
    const finalAptNumberSent = lot || undefined;
    console.log("[FR_LOT_UI] before_submit");
    console.log("[FR_LOT_UI] lotInput", lotInput);
    console.log("[FR_LOT_UI] requestedLot", requestedLot);
    console.log("[FR_LOT_UI] hasSubmittedLotSearch", hasSubmittedLotSearch);
    console.log("[FR_LOT_UI] final aptNumber sent", finalAptNumberSent ?? null);
    if (isDev) {
      console.log("[FR_DEBUG_UI] submit_pressed", {
        lot_input_ui_value: lotInput,
        lot_value_sent_in_request: lot || null,
        effectiveDetectClass,
        requestedLot_current: requestedLot,
        hasSubmittedLotSearch,
        isLoading,
        source,
      });
    }
    // Hard block: apartment-first flow requires a lot before any final card can render.
    if (effectiveDetectClass === "apartment" && !lot) {
      if (isDev)
        console.log("[FR_UI_DEBUG]", {
          detectClass: effectiveDetectClass,
          lotSubmitted: false,
          blockedBeforeLot: true,
          franceRequestStarted: false,
          franceRequestFinished: false,
          franceRequestFailed: false,
          loadingStateBefore: isLoading,
          loadingStateAfter: isLoading,
          responseStatus: null,
          responsePayloadTag: (normalized as any)?.resultType ?? null,
          source,
        });
      return;
    }
    if (effectiveDetectClass === "apartment" && isLoading) {
      if (isDev)
        console.log("[FR_UI_DEBUG]", {
          detectClass: effectiveDetectClass,
          lotSubmitted: !!lot,
          blockedBeforeLot: false,
          franceRequestStarted: false,
          franceRequestFinished: false,
          franceRequestFailed: false,
          loadingStateBefore: true,
          loadingStateAfter: true,
          responseStatus: null,
          responsePayloadTag: (normalized as any)?.resultType ?? null,
          source,
        });
      return;
    }
    setRequestedLot(lot || undefined);
    // Hard guarantee: never show any previous result while a new lot search is in flight.
    // We only render a final result once the latest request resolves and matches the requested lot.
    setResolvedForDisplay(null);
    setHasSubmittedLotSearch(true);
    setTrigger((t) => t + 1);
    setIsResultCardOpen(true);
    console.log("[FR_LOT_UI] new request started");

    if (isDev) {
      console.log("[FR_UI_DEBUG]", {
        detectClass: effectiveDetectClass,
        lotSubmitted: !!lot,
        blockedBeforeLot: false,
        franceRequestStarted: true,
        loadingStateBefore: isLoading,
        responseStatus: null,
        responsePayloadTag: null,
        source,
      });
    }
  }, [lotInput, requestedLot, hasSubmittedLotSearch, effectiveDetectClass, isDev, isLoading, normalized]);

  const prevIsLoadingRef = React.useRef<boolean>(false);
  const lastLoggedFranceKeyRef = React.useRef<string | null>(null);
  React.useEffect(() => {
    if (!hasSubmittedLotSearch) {
      prevIsLoadingRef.current = isLoading;
      return;
    }
    // Detect transition: lot-search request was in-flight -> now resolved.
    if (prevIsLoadingRef.current && !isLoading) {
      const lotSubmitted = !!(requestedLot ?? "").trim();
      const resultCardBlocked = effectiveDetectClass === "apartment" && !lotSubmitted;
      const responsePayloadTag = (normalized as any)?.resultType ?? (data as any)?.message ?? null;
      const responseStatus = (normalized as any)?.success ?? null;

      const requestFailed = !normalized;
      const finalStreetAvgMessage = (normalized as any)?.property_result?.street_average_message ?? null;
      const winningSourceLabel =
        finalStreetAvgMessage === "Similar properties on same street"
          ? "Similar properties on same street"
          : finalStreetAvgMessage === "Similar properties in same commune"
            ? "Similar properties in same commune"
            : (normalized as any)?.resultType === "exact_apartment"
              ? "Exact apartment"
              : (normalized as any)?.resultType === "exact_address"
                ? "Exact address match"
                : (normalized as any)?.resultType === "exact_house"
                  ? "Exact house match"
              : (normalized as any)?.resultType === "building_level"
                ? "Similar properties in this building"
                : finalStreetAvgMessage ?? "No reliable data found";

      const valuationStepReached =
        (normalized as any)?.resultType === "exact_apartment"
          ? "exact_unit"
          : (normalized as any)?.resultType === "exact_address"
            ? "exact_address"
            : (normalized as any)?.resultType === "exact_house"
              ? "exact_house"
          : finalStreetAvgMessage === "Similar properties on same street"
            ? "street_fallback"
            : finalStreetAvgMessage === "Similar properties in same commune"
              ? "commune_fallback"
              : "no_data";

      if (isDev) {
        console.log("[FR_UI_DEBUG]", {
          detectClass: effectiveDetectClass,
          lotSubmitted,
          blockedBeforeLot: resultCardBlocked,
          franceRequestFinished: !requestFailed,
          franceRequestFailed: requestFailed,
          loadingStateBefore: true,
          loadingStateAfter: false,
          responseStatus,
          responsePayloadTag,
          valuationStepReached,
          winningSourceLabel,
        });
      }
    }
    prevIsLoadingRef.current = isLoading;
  }, [hasSubmittedLotSearch, isLoading, requestedLot, effectiveDetectClass, normalized, isDev, data]);

  // Capture ONLY the final resolved result for the active lot request.
  // This prevents intermediate flashes from stale building-only payloads.
  React.useEffect(() => {
    if (!isResultCardOpen) return;
    if (!hasSubmittedLotSearch) return;
    if (isLoading) return;
    const fr = normalized;
    if (!fr) return;
    const reqLot = (requestedLot ?? "").trim();
    const frReqLot = (fr.requestedLot ?? "").trim();
    if (reqLot !== frReqLot) return;
    const runtimeDebug = (parsed as any)?.fr_runtime_debug ?? (data as any)?.fr_runtime_debug ?? null;
    setResolvedForDisplay({
      fr,
      legacy: { averageBuildingValue, livabilityRating: legacyLivability },
      fr_runtime_debug: runtimeDebug,
    });

    if (isDev) {
      console.log("[FR_DEBUG_UI] lot_value_received_in_api", {
        lot_value_received_in_api: (runtimeDebug as any)?.submitted_lot ?? null,
        normalized_submitted_lot: (runtimeDebug as any)?.submitted_lot ?? null,
        requestedLot_from_fr: (fr as any)?.requestedLot ?? null,
      });
    }

    // TEMP: Investigation logging once per resolved request (France only).
    const showFranceDebug = true;
    if (showFranceDebug) {
      const resultKey = `${addressKey}|lot:${reqLot}|result:${String(fr?.resultType ?? "")}|t:${trigger}`;
      if (lastLoggedFranceKeyRef.current !== resultKey) {
        lastLoggedFranceKeyRef.current = resultKey;
        // eslint-disable-next-line no-console
        console.log("[FR_UI_DEBUG] full_fr_api_response", data);
      }
    }

    if (isDev) {
      const finalSourceLabel = (fr as any)?.property_result?.street_average_message ?? null;
      const valuationStepReached =
        finalSourceLabel === "Similar properties on same street"
          ? "street_fallback"
          : finalSourceLabel === "Similar properties in same commune"
            ? "commune_fallback"
            : fr.resultType === "exact_apartment"
              ? "exact_unit"
              : fr.resultType === "exact_address"
                ? "exact_address"
                : fr.resultType === "exact_house"
                  ? "exact_house"
              : fr.resultType === "building_level"
                ? "building_level"
                : fr.resultType === "nearby_comparable"
                  ? "street_fallback"
                  : "no_data";
      console.log("[FR_UI_DEBUG] final_value_produced", {
        detectClass: effectiveDetectClass,
        hasSubmittedLot: hasSubmittedLotSearch,
        valuationStepReached,
        sourceLabel: finalSourceLabel,
        resultType: fr.resultType,
        winningSourceLabel: finalSourceLabel,
      });
    }
    if (isDev) {
      console.log("[FR_UI] final_response_type", {
        resultType: fr.resultType,
        success: fr.success,
        confidence: fr.confidence,
      });
    }
  }, [
    isResultCardOpen,
    hasSubmittedLotSearch,
    isLoading,
    normalized,
    requestedLot,
    averageBuildingValue,
    legacyLivability,
    isDev,
    parsed,
    data,
    addressKey,
    trigger,
  ]);

  const isMobileViewport = React.useMemo(() => {
    if (typeof window === "undefined") return false;
    return window.matchMedia?.("(max-width: 640px)")?.matches ?? false;
  }, []);

  React.useEffect(() => {
    if (!isLotFocused) {
      setKeyboardInsetPx(0);
      return;
    }
    const vv = window.visualViewport;
    const update = () => {
      if (vv) {
        const inset = Math.max(0, Math.round(window.innerHeight - vv.height - vv.offsetTop));
        setKeyboardInsetPx(inset);
      } else {
        setKeyboardInsetPx(280);
      }
    };
    update();
    vv?.addEventListener("resize", update);
    vv?.addEventListener("scroll", update);
    window.addEventListener("resize", update);
    return () => {
      vv?.removeEventListener("resize", update);
      vv?.removeEventListener("scroll", update);
      window.removeEventListener("resize", update);
    };
  }, [isLotFocused]);

  const COMPACT_MAX_VH = 35;
  const EXPANDED_MAX_VH = 70;
  const panelMaxHeightVh = isExpanded ? EXPANDED_MAX_VH : COMPACT_MAX_VH;

  // NOTE: do NOT memoize this portal node. We need immediate re-render on UI-only state
  // changes like "More details" toggling (no waiting for a refetch).
  const resultCardNode = (() => {
    if (typeof document === "undefined") return null;
    if (!isResultCardOpen) return null;
    if (!hasSubmittedLotSearch) {
      if (effectiveDetectClass === "apartment" && isDev) {
        console.log("[FR_UI_DEBUG] result_card_blocked", {
          detectClass: effectiveDetectClass,
          hasSubmittedLot: hasSubmittedLotSearch,
          blocked: true,
        });
      }
      return null;
    }
    // Extra safety: apartment-first should never render without a submitted lot value.
    if (effectiveDetectClass === "apartment" && !(requestedLot ?? "").trim()) {
      if (isDev) {
        console.log("[FR_UI_DEBUG] result_card_blocked_missing_lot_value", {
          detectClass: effectiveDetectClass,
          hasSubmittedLot: hasSubmittedLotSearch,
          blocked: true,
          requestedLot,
        });
      }
      return null;
    }

    const isLoadingNow = isLoading;
    const fr = resolvedForDisplay?.fr ?? normalized ?? null;
    const legacy = resolvedForDisplay?.legacy ?? { averageBuildingValue, livabilityRating: legacyLivability };
    const frRuntimeDebug =
      resolvedForDisplay?.fr_runtime_debug ?? (data as any)?.fr_runtime_debug ?? (parsed as any)?.fr_runtime_debug ?? null;
    const rd: any = frRuntimeDebug ?? null;
    const toDebugStr = (v: any) => {
      if (v === null || v === undefined) return "—";
      if (typeof v === "string") return v;
      if (typeof v === "number" || typeof v === "boolean") return String(v);
      try {
        return JSON.stringify(v);
      } catch {
        return String(v);
      }
    };

    /**
     * Text between "•" and "reference sale" on the France result card. Only primitives: unwrap
     * value / text / label / date (never `String(object)` → "[object Object]").
     */
    function referenceSaleDateLabelFromRaw(raw: unknown): string | null {
      const formatEnGb = (d: Date): string | null => {
        if (Number.isNaN(d.getTime())) return null;
        const lab = d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
        return typeof lab === "string" && lab.trim() ? lab.trim() : null;
      };

      const step = (v: unknown, depth: number): string | null => {
        if (depth > 14) return null;
        if (v === null || v === undefined) return null;
        if (typeof v === "string") {
          const t = v.trim();
          return t.length ? t : null;
        }
        if (typeof v === "number" && Number.isFinite(v)) return String(v);
        if (typeof v !== "object" || Array.isArray(v)) return null;
        const o = v as Record<string, unknown>;
        if ("value" in o && o.value !== undefined) return step(o.value, depth + 1);
        if ("text" in o && o.text !== undefined) return step(o.text, depth + 1);
        if ("label" in o && o.label !== undefined) return step(o.label, depth + 1);
        if ("date" in o && o.date !== undefined) return step(o.date, depth + 1);
        if ("year" in o && "month" in o && "day" in o) {
          const y = step(o.year, depth + 1);
          const mo = step(o.month, depth + 1);
          const day = step(o.day, depth + 1);
          if (y != null && mo != null && day != null) {
            const ys = Number(y);
            const ms = Number(mo);
            const ds = Number(day);
            if ([ys, ms, ds].every((n) => Number.isFinite(n))) {
              return `${Math.trunc(ys)}-${String(Math.trunc(ms)).padStart(2, "0")}-${String(Math.trunc(ds)).padStart(2, "0")}`;
            }
          }
        }
        return null;
      };

      const prim = step(raw, 0);
      if (!prim) return null;
      const d = new Date(prim);
      const label = formatEnGb(d);
      if (label) return label;
      const n = Number(prim.replace(/\s/g, ""));
      if (Number.isFinite(n) && n > 1_000_000_000) {
        const ms = n > 1e12 ? n : n * 1000;
        const d2 = new Date(ms);
        return formatEnGb(d2);
      }
      return null;
    }

    const referenceSaleValue: string | null = isLoadingNow
      ? null
      : referenceSaleDateLabelFromRaw(fr?.property?.transactionDate as unknown);

    if (!isLoadingNow && fr?.resultType === "building_similar_unit") {
      console.log("[FR_UI_REF_SALE]", referenceSaleValue, typeof referenceSaleValue);
    }

    const title = isLoadingNow
      ? "Searching DVF…"
      : badge?.label ??
        (!fr
          ? "No result"
          : fr?.success === false
            ? "No result"
            : "Result");
    const explanation = isLoadingNow
      ? ((requestedLot ?? "").trim() ? `Searching lot ${(requestedLot ?? "").trim()} for this address` : "Searching building data for this address")
      : (subtitle || fr?.matchExplanation || null);

    const fv = (parsed as any)?.fr_valuation_display as FranceValuationDisplay | undefined;
    const topEstimated = coercePositiveNumber((parsed as any)?.estimated_value);
    const topPricePerM2 = coercePositiveNumber((parsed as any)?.price_per_m2);
    const topDisplayValue = coercePositiveNumber((parsed as any)?.display_value);
    const topDisplayValueType = coerceNullableString((parsed as any)?.display_value_type);
    const fvEst = coercePositiveNumber(fv?.estimated_value as unknown);
    const fvPpm = coercePositiveNumber(fv?.price_per_m2 as unknown);
    const fvDisp = coercePositiveNumber(fv?.display_value as unknown);
    const fvDispType = coerceNullableString(fv?.display_value_type as unknown);

    const txFromProp = coercePositiveNumber(fr?.property?.transactionValue as unknown);
    const rawValue =
      fr?.resultType === "building_similar_unit"
        ? fvEst ??
          topEstimated ??
          coercePositiveNumber(pr?.exact_value) ??
          txFromProp ??
          null
        : txFromProp ??
          fvEst ??
          topEstimated ??
          coercePositiveNumber(pr?.exact_value) ??
          (fr?.resultType === "building_level"
            ? coercePositiveNumber(fr?.buildingStats?.avgTransactionValue as unknown) ??
              coercePositiveNumber(legacy?.averageBuildingValue as unknown)
            : null);
    const hasValue = typeof rawValue === "number" && rawValue > 0;
    const ppm2FromApi =
      coercePositiveNumber(fr?.property?.pricePerSqm as unknown) ??
      fvPpm ??
      topPricePerM2 ??
      coercePositiveNumber(pr?.street_average) ??
      (fvDispType === "price_per_m2" && fvDisp != null && fvDisp > 0 ? fvDisp : null) ??
      (topDisplayValueType === "price_per_m2" && topDisplayValue != null && topDisplayValue > 0 ? topDisplayValue : null) ??
      coercePositiveNumber(rd?.winning_median_price_per_m2);
    const ppm2Display =
      ppm2FromApi != null ? normalizeFrancePricePerSqmForDisplay(ppm2FromApi) : null;
    const isNoResult =
      !isLoadingNow &&
      (!fr || fr?.resultType === "no_result" || fr?.resultType === "no_reliable_data" || fr?.success === false);

    const isSuspiciousFallbackRaw =
      !isLoadingNow &&
      (fr?.resultType === "similar_apartment_same_building" || fr?.resultType === "nearby_comparable") &&
      (fr?.debug?.suspiciousPolicy === "warning" || fr?.debug?.suspiciousPricePerSqm === true);
    // API contract: explicit €/m² display from valuation_response must still render.
    const isSuspiciousFallback =
      isSuspiciousFallbackRaw &&
      !(
        fvDispType === "price_per_m2" &&
        fv?.has_display_value === true &&
        (fvPpm != null || fvDisp != null)
      );

    const winningStepStr = coerceDisplayString(rd?.winning_step, "").trim();
    const winningParsed = coerceDisplayString((parsed as any)?.winning_source_label, "").trim();
    const winningFv = coerceDisplayString(fv?.winning_source_label as unknown, "").trim();
    const winningRd = coerceDisplayString(rd?.winning_source_label, "").trim();
    const winningSourceFromApi = winningParsed || winningFv || winningRd || "";
    const hasFranceValuationWin =
      fr != null &&
      fr.success !== false &&
      winningStepStr.length > 0 &&
      winningStepStr !== "no_data" &&
      !isNoResult;

    const basedOnExplainer = (() => {
      if (isLoadingNow || isNoResult) return null;
      const ws = winningStepStr;
      if (ws === "exact_unit") return "Based on exact property match";
      if (ws === "exact_address") return "Based on exact address match";
      if (ws === "exact_house") return "Based on exact house match";
      if (ws === "building_level" || ws === "building_fallback")
        return "Based on similar properties in this building";
      if (ws === "building_similar_unit") return "Based on similar apartments in this building";
      if (ws === "street_fallback") return "Based on similar properties on the same street";
      if (ws === "commune_fallback") return "Based on similar properties in the same commune";
      if (fr?.resultType === "exact_apartment") return "Based on exact property match";
      if (fr?.resultType === "exact_address") return "Based on exact address match";
      if (fr?.resultType === "exact_house") return "Based on exact house match";
      if (fr?.resultType === "building_level" || fr?.resultType === "building_fallback")
        return "Based on similar properties in this building";
      if (fr?.resultType === "building_similar_unit") return "Based on similar apartments in this building";
      if (fr?.resultType === "nearby_comparable")
        return isHouseLikeUI
          ? "Based on nearby comparable houses"
          : "Based on nearby comparable apartments";
      if (fr?.resultType === "similar_apartment_same_building")
        return isHouseLikeUI
          ? "Based on nearby property transactions"
          : "Based on similar apartments in the same building";
      return null;
    })();

    // Rule: estimated_value null + price_per_m2 (or display_value) → show €/m² in headline, not "—".
    const hasPricePerM2Headline =
      !hasValue &&
      ppm2FromApi != null &&
      ppm2Display != null &&
      !isNoResult &&
      !isLoadingNow &&
      !isSuspiciousFallback;

    const transactionCountRaw =
      !isLoadingNow ? coerceFiniteNumber(fr?.buildingStats?.transactionCount as unknown) : null;
    const transactionCount =
      transactionCountRaw != null && transactionCountRaw > 0 ? Math.round(transactionCountRaw) : null;

    const valueRange = (() => {
      if (isLoadingNow) return null;
      if (isNoResult) return null;
      if (isSuspiciousFallback) return null;
      // Only show a range when we have enough building comparables to support it.
      if (fr?.resultType !== "building_level" && fr?.resultType !== "similar_apartment_same_building") return null;
      const displayedSurfaceRaw = coercePositiveNumber(fr?.property?.surfaceArea as unknown);
      const displayedSurface = displayedSurfaceRaw;
      const displayedPricePerSqmRaw = coercePositiveNumber(fr?.property?.pricePerSqm as unknown);
      const displayedPricePerSqm =
        displayedPricePerSqmRaw != null ? normalizeFrancePricePerSqmForDisplay(displayedPricePerSqmRaw) : null;

      const rangeCandidates = (fr?.comparables ?? [])
        .map((c) => {
          const price = typeof c?.price === "number" ? c.price : 0;
          const surface = typeof c?.surface === "number" ? c.surface : null;
          const ppm2 = surface != null && surface > 0 ? price / surface : null;
          return { price, surface, ppm2 };
        })
        .filter((c) => Number.isFinite(c.price) && c.price > 0);

      // Range cohort filtering only (does not affect any other UI or result logic):
      // 1) Surface filter ±25% of displayed surface (when both exist)
      // 2) Price/m² filter ±30% of displayed €/m² (when both exist)
      const filteredForRange = rangeCandidates.filter((c) => {
        if (displayedSurface != null) {
          if (!(c.surface != null && c.surface > 0)) return false;
          const lo = displayedSurface * 0.75;
          const hi = displayedSurface * 1.25;
          if (c.surface < lo || c.surface > hi) return false;
        }
        if (displayedPricePerSqm != null) {
          if (!(c.ppm2 != null && c.ppm2 > 0)) return false;
          const lo = displayedPricePerSqm * 0.7;
          const hi = displayedPricePerSqm * 1.3;
          if (c.ppm2 < lo || c.ppm2 > hi) return false;
        }
        return true;
      });

      const prices = filteredForRange.map((c) => c.price);
      if (prices.length < 3) return null;
      const sorted = [...prices].sort((a, b) => a - b);
      const quantile = (q: number): number | null => {
        if (!(q >= 0 && q <= 1)) return null;
        const n = sorted.length;
        if (n < 3) return null;
        const pos = (n - 1) * q;
        const lo = Math.floor(pos);
        const hi = Math.ceil(pos);
        const a = sorted[lo];
        const b = sorted[hi];
        if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
        if (lo === hi) return a;
        const t = pos - lo;
        return a + (b - a) * t;
      };
      const p20 = quantile(0.2);
      const p80 = quantile(0.8);
      if (p20 == null || p80 == null) return null;
      if (!Number.isFinite(p20) || !Number.isFinite(p80) || p20 <= 0 || p80 <= 0) return null;
      if (p80 <= p20) return null;
      return { min: p20, max: p80 };
    })();

    // €/m²-only (estimated total absent, no min–max range): hide "Estimated value" — keep market value + confidence + source.
    const isPricePerM2OnlyHeadline =
      hasPricePerM2Headline && !hasValue && valueRange == null;
    const showEstimatedValueSubLabel = !isPricePerM2OnlyHeadline && (hasValue || valueRange != null);

    const lastTxValue =
      !isLoadingNow && !isNoResult ? coercePositiveNumber(fr?.property?.transactionValue as unknown) : null;
    const estimatedEqualsLastTx =
      !isLoadingNow &&
      !isNoResult &&
      !isSuspiciousFallback &&
      valueRange == null &&
      typeof rawValue === "number" &&
      rawValue > 0 &&
      lastTxValue != null &&
      rawValue === lastTxValue;

    const displayPricePerSqm =
      !isLoadingNow && !isNoResult && !isSuspiciousFallback && ppm2Display != null ? ppm2Display : null;

    const mainValue = isLoadingNow
      ? "Searching…"
      : isNoResult
        ? frDetect === "unclear"
          ? "No reliable France property data found for this address in the current dataset."
          : "No reliable data available"
        : isSuspiciousFallback
          ? "No reliable price available"
          : hasValue
            ? formatFranceEuroTotal(rawValue)
            : hasPricePerM2Headline && ppm2FromApi != null
              ? formatFranceEuroPerSqmFromUnknown(ppm2FromApi, { medianSuffix: true })
            : fr?.resultType === "building_level"
              ? "No reliable building value available"
              : "—";

    const lastTxAmountPositive = !isLoadingNow
      ? coercePositiveNumber(fr?.property?.transactionValue as unknown)
      : null;

    const dateText = isLoadingNow ? "—" : referenceSaleValue ?? "—";

    const lastTransactionSummaryLine = (() => {
      if (isLoadingNow) return "—";
      const amt = lastTxAmountPositive;
      if (
        (fr?.resultType === "exact_apartment" || fr?.resultType === "exact_address" || fr?.resultType === "exact_house") &&
        !isNoResult &&
        amt != null &&
        referenceSaleValue != null
      ) {
        return `${formatFranceEuroTotal(amt)} • ${referenceSaleValue}`;
      }
      if (fr?.resultType === "building_similar_unit" && !isNoResult && amt != null) {
        return referenceSaleValue != null
          ? `${formatFranceEuroTotal(amt)} • ${referenceSaleValue} reference sale`
          : `${formatFranceEuroTotal(amt)} • reference sale`;
      }
      return "No exact recent transaction available";
    })();
    const streetAvgMsg = coerceDisplayString(pr?.street_average_message as unknown, "").trim();
    const sourceText = isLoadingNow
      ? "—"
      : hasFranceValuationWin && winningSourceFromApi
        ? winningSourceFromApi
        : streetAvgMsg && !/no reliable data found/i.test(streetAvgMsg)
          ? streetAvgMsg
          : sourceLabel || (isNoResult ? "No reliable data found" : "—");
    const confidenceText = isLoadingNow ? "—" : (displayConfidence ? displayConfidence : "—");
    const livabilityText = isLoadingNow ? "—" : coerceDisplayString(legacy?.livabilityRating as unknown, "—");
    const flowMarker = shouldForceLotFirstFlow ? "FR Flow: apartment-first" : isHouseLikeUI ? "FR Flow: house-direct" : "FR Flow: fallback";
    const detectMarker = `FR Detect: ${String(parsed?.fr_detect ?? (shouldForceLotFirstFlow ? "apartment" : isHouseLikeUI ? "house" : "unclear"))}`;
    // Reuse the exact gold token used by the Search button and active Explore icon.
    const goldTextClass = "text-amber-400";
    const frConfNorm = String(unwrapScalar(fr?.confidence as unknown) ?? "")
      .toLowerCase()
      .replace(/_/g, "-");
    const confidenceTone =
      isLoadingNow
        ? "border-white/10 bg-white/5 text-zinc-200"
        : frConfNorm === "high" || frConfNorm === "medium-high" || frConfNorm === "medium_high"
          ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-200"
          : frConfNorm === "medium" || frConfNorm === "low-medium" || frConfNorm === "low_medium"
            ? "border-amber-400/20 bg-amber-400/10 text-amber-200"
            : "border-rose-400/20 bg-rose-400/10 text-rose-200";

    // Position above the input sheet, like the old floating property card.
    const bottomOffset = isMobileViewport ? `calc(6rem + ${keyboardInsetPx}px + 1rem)` : "10.5rem";
    const side = "0.5rem";

    return createPortal(
      <div
        className="pointer-events-none fixed inset-0 z-30"
        aria-live="polite"
        aria-label="France DVF result"
      >
        <div
          className="pointer-events-auto"
          style={{
            position: "fixed",
            right: side,
            left: "auto",
            bottom: bottomOffset,
          }}
        >
          <div
            className="pointer-events-auto shrink-0 rounded-[10px] border border-white/10 bg-[#0b0d10] shadow-md"
            style={{
              width: 300,
              maxWidth: 300,
              padding: 8,
            }}
          >
            {/* Row 1 – Header */}
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0 flex-1">
                <div className="mt-0.5 truncate text-sm font-semibold leading-tight text-white">{address}</div>
              </div>
              <div className="flex shrink-0 items-center gap-1.5">
                <button
                  type="button"
                  onClick={() => setIsResultCardOpen(false)}
                  aria-label="Close result"
                  className="rounded-full border border-white/10 p-1.5 text-zinc-400 hover:text-white"
                >
                  <X className="size-4" />
                </button>
              </div>
            </div>

            {/* Row 1b – Result title + optional subtitle */}
            <div className="mt-1.5">
              <div className="flex items-center gap-2">
                <div className="text-xs font-semibold text-white/90">{title}</div>
                {isSuspiciousFallback ? (
                  <div className="truncate rounded-[10px] border border-amber-400/25 bg-amber-400/10 px-2 py-0.5 text-[10px] font-semibold text-amber-200 max-w-[170px]">
                    Caution: unusually high price per m². Treat this result with care.
                  </div>
                ) : null}
              </div>
            </div>

            {/* Core summary – 4 primary sections */}
            <div className="mt-1.5 space-y-[6px]">
              {/* 1) Estimated market value */}
              <div className="rounded-[10px] border border-white/10 bg-black/20 p-2">
                <div className="text-[11px] font-medium uppercase tracking-[0.14em] leading-tight text-zinc-400/70">
                  Estimated market value
                </div>
                <div
                  className={
                    isLoadingNow
                      ? `mt-1 flex items-center gap-2 whitespace-nowrap text-[20px] font-medium leading-tight ${goldTextClass}`
                      : isNoResult
                        ? "mt-1 text-sm font-semibold whitespace-nowrap leading-tight text-zinc-100"
                        : `mt-1 text-[26px] sm:text-[28px] whitespace-nowrap font-bold leading-none ${goldTextClass}`
                  }
                >
                  {isLoadingNow ? (
                    <>
                      <span className={`inline-flex size-3.5 animate-spin rounded-full border-2 border-white/15 border-t-amber-200`} aria-hidden="true" />
                      <span>Searching...</span>
                    </>
                  ) : (
                    (isNoResult
                      ? "No exact data found — showing limited insights"
                      : valueRange
                        ? `${formatFranceEuroTotal(valueRange.min)} – ${formatFranceEuroTotal(valueRange.max)}`
                        : mainValue)
                  )}
                </div>

                <div className="mt-1 flex items-center justify-start gap-2">
                  {showEstimatedValueSubLabel ? (
                    <div className="text-[11px] font-medium uppercase tracking-wider leading-tight text-zinc-400/70">
                      Estimated value
                    </div>
                  ) : null}
                  {!isLoadingNow ? (
                    <div className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${confidenceTone}`}>
                      Confidence: {confidenceText}
                    </div>
                  ) : null}
                </div>

                {!isLoadingNow ? (
                  <div className="mt-1 text-xs font-medium leading-tight text-zinc-300/75">
                    {frDetect === "unclear" && isNoResult
                      ? "Try another address with confirmed France DVF coverage."
                      : basedOnExplainer}
                  </div>
                ) : null}
              </div>

              {/* 2) Last transaction */}
              <div className="rounded-[10px] border border-white/10 bg-black/20 px-2.5 py-2">
                <div className="text-[11px] font-medium uppercase tracking-[0.14em] leading-tight text-zinc-400/70">
                  Last transaction
                </div>
                <div className="mt-1 text-[14px] font-semibold leading-tight text-white">
                  {lastTransactionSummaryLine}
                </div>
              </div>

              {/* 3) Based on */}
              <div className="rounded-[10px] border border-white/10 bg-black/20 px-2.5 py-2">
                <div className="text-[11px] font-medium uppercase tracking-[0.14em] leading-tight text-zinc-400/70">Source</div>
                <div className="mt-1 text-[14px] font-semibold leading-tight text-white">{sourceText}</div>
              </div>

              {/* 4) Livability */}
              <div className="rounded-[10px] border border-white/10 bg-black/20 px-2.5 py-2">
                <div className="text-[11px] font-medium uppercase tracking-[0.14em] leading-tight text-zinc-400/70">
                  Livability
                </div>
                <div className="mt-1 text-[14px] font-semibold leading-tight text-white">{livabilityText}</div>
              </div>

              {/* TEMP: France runtime diagnostics (investigation only) */}
              <div className="mt-1">
                <button
                  type="button"
                  onClick={() => setIsFranceDebugOpen((v) => !v)}
                  className="flex w-full items-center justify-between rounded-[10px] border border-white/10 bg-white/5 px-2.5 py-2 text-left"
                  aria-expanded={isFranceDebugOpen}
                >
                  <span className="text-xs font-semibold text-white/85">France Debug</span>
                  <span className="text-xs font-semibold text-white/60">{isFranceDebugOpen ? "Hide" : "Show"}</span>
                </button>
                {isFranceDebugOpen ? (
                  <div className="mt-1 rounded-[10px] border border-white/10 bg-black/15 px-2.5 py-2 overflow-hidden">
                    <div className="text-[10px] uppercase tracking-wider text-zinc-400/90 font-medium">fr_runtime_debug</div>
                    <div className="mt-2 space-y-0.5 text-[10px] leading-tight text-zinc-400/90">
                      <div className="font-mono">{flowMarker}</div>
                      <div className="font-mono">{detectMarker}</div>
                    </div>
                    <div className="mt-2 font-mono text-[10px] leading-tight text-zinc-200/80 space-y-0.5">
                      <div>ban_city: {toDebugStr(rd?.ban_city)}</div>
                      <div>ban_postcode: {toDebugStr(rd?.ban_postcode)}</div>
                      <div>ban_street: {toDebugStr(rd?.ban_street)}</div>
                      <div>ban_house_number: {toDebugStr(rd?.ban_house_number)}</div>
                      <div>submitted_lot: {toDebugStr(rd?.submitted_lot)}</div>
                      <div>detect_class: {toDebugStr(rd?.detect_class)}</div>
                      <div>exact_rows_count: {toDebugStr(rd?.exact_rows_count)}</div>
                      <div>exact_usable_rows_count: {toDebugStr(rd?.exact_usable_rows_count)}</div>
                      <div>exact_level: {toDebugStr(rd?.exact_level)}</div>
                      <div>exact_unit_row_count: {toDebugStr(rd?.exact_unit_row_count)}</div>
                      <div>exact_address_row_count: {toDebugStr(rd?.exact_address_row_count)}</div>
                      <div>building_rows_count: {toDebugStr(rd?.building_rows_count)}</div>
                      <div>building_usable_rows_count: {toDebugStr(rd?.building_usable_rows_count)}</div>
                      <div>building_similar_unit_candidates_count: {toDebugStr(rd?.building_similar_unit_candidates_count)}</div>
                      <div>building_similar_unit_after_filters_count: {toDebugStr(rd?.building_similar_unit_after_filters_count)}</div>
                      <div>building_similar_unit_reject_reason: {toDebugStr(rd?.building_similar_unit_reject_reason)}</div>
                      <div>street_rows_count: {toDebugStr(rd?.street_rows_count)}</div>
                      <div>street_usable_rows_count: {toDebugStr(rd?.street_usable_rows_count)}</div>
                      <div>commune_rows_count: {toDebugStr(rd?.commune_rows_count)}</div>
                      <div>commune_usable_rows_count: {toDebugStr(rd?.commune_usable_rows_count)}</div>
                      <div>winning_step: {toDebugStr(rd?.winning_step)}</div>
                      <div>winning_source_label: {toDebugStr(rd?.winning_source_label)}</div>
                      <div>has_surface_for_estimate: {toDebugStr(rd?.has_surface_for_estimate)}</div>
                      <div>chosen_surface_value: {toDebugStr(rd?.chosen_surface_value)}</div>
                      <div>no_data_reason: {toDebugStr(rd?.no_data_reason)}</div>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>

            {/* More details (collapsed by default) */}
            {(
              transactionCount ||
              matchDetails ||
              fr?.property?.pricePerSqm ||
              fr?.property?.surfaceArea ||
              fr?.property?.propertyType
            ) ? (
              <div className="mt-1.5" style={{ marginBottom: 0 }}>
                <button
                  type="button"
                  onClick={() => setIsMoreDetailsOpen((v) => !v)}
                  className="flex w-full items-center justify-between rounded-[10px] border border-white/10 bg-white/5 px-2.5 py-2 text-left"
                  aria-expanded={isMoreDetailsOpen}
                >
                  <span className="text-xs font-semibold text-white/85">More details</span>
                  <span className="text-xs font-semibold text-white/60">{isMoreDetailsOpen ? "Hide" : "Show"}</span>
                </button>

                {isMoreDetailsOpen ? (
                  <div className="mt-1" />
                ) : null}

                <div
                  className={[
                    "grid transition-[grid-template-rows,opacity] duration-200 ease-out",
                    isMoreDetailsOpen ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0",
                  ].join(" ")}
                >
                  <div className="overflow-hidden">
                    <div className="mt-[6px] rounded-[10px] border border-zinc-500/15 bg-black/15 p-2">
                      <div className="text-[10px] uppercase tracking-wider text-zinc-400/90">Additional details</div>
                      <div className="mt-1 space-y-0.5 text-[10px] leading-tight text-zinc-200/80">
                        {transactionCount ? (
                          <div>Transactions: <span className="font-medium text-white">{transactionCount}</span></div>
                        ) : null}
                        {matchDetails?.requested ? (
                          <div>Requested lot: <span className="font-medium text-white">{matchDetails.requested}</span></div>
                        ) : null}
                        {matchDetails?.matched ? (
                          <div>Data taken from lot: <span className="font-medium text-white">{matchDetails.matched}</span></div>
                        ) : null}
                        {coercePositiveNumber(fr?.property?.surfaceArea as unknown) != null ? (
                          <div>
                            Surface:{" "}
                            <span className="font-medium text-white">
                              {coercePositiveNumber(fr?.property?.surfaceArea as unknown)} m²
                            </span>
                          </div>
                        ) : null}
                        {!isSuspiciousFallback && coercePositiveNumber(fr?.property?.pricePerSqm as unknown) != null ? (
                          <div>
                            Price/m²:{" "}
                            <span className="font-medium text-white">
                              {formatFranceEuroPerSqmFromUnknown(fr?.property?.pricePerSqm, { medianSuffix: false })}
                            </span>
                          </div>
                        ) : null}
                        {isSuspiciousFallback ? (
                          <div>Price/m²: <span className="font-medium text-white">Suppressed (suspicious)</span></div>
                        ) : null}
                        {coerceDisplayString(fr?.property?.propertyType as unknown, "").trim() ? (
                          <div>
                            Property type:{" "}
                            <span className="font-medium text-white">
                              {coerceDisplayString(fr?.property?.propertyType as unknown, "—")}
                            </span>
                          </div>
                        ) : null}
                        {/* Secondary rows hidden by default */}
                        <div>Date: <span className="font-medium text-white">{dateText}</span></div>
                        <div>Confidence: <span className="font-medium text-white">{confidenceText}</span></div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </div>,
      document.body
    );
  })();

  return (
    <div
      className={[
        "pointer-events-none z-20 flex flex-col justify-end transition-all duration-300 ease-out",
        // Keep it lightweight and map-first. On mobile, use fixed inset-x so it doesn't overflow.
        isMobileViewport ? "fixed inset-x-2" : "absolute right-4 left-auto",
      ].join(" ")}
      style={{
        bottom: `calc(${isMobileViewport ? "6rem" : "1.5rem"} + ${keyboardInsetPx}px)`,
      }}
    >
      <div
        className={[
          "pointer-events-auto flex shrink-0 min-h-0 flex-col overflow-hidden rounded-2xl border border-amber-400/20 bg-black/90 shadow-2xl shadow-black/60 backdrop-blur-xl",
          isMobileViewport ? "w-full" : "w-[360px] max-w-[92vw] sm:w-[380px] sm:max-w-[420px]",
        ].join(" ")}
        style={{
          maxHeight: `calc(${panelMaxHeightVh}vh + env(safe-area-inset-bottom))`,
          paddingBottom: `calc(env(safe-area-inset-bottom) + ${keyboardInsetPx}px)`,
        }}
      >
        {resultCardNode}
        {/* Grabber / expand handle */}
        <div
          className="pointer-events-auto flex justify-center pt-2"
          onPointerDown={(e) => {
            dragStartYRef.current = e.clientY;
            dragToggledRef.current = false;
            try {
              (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
            } catch {}
          }}
          onPointerMove={(e) => {
            const startY = dragStartYRef.current;
            if (startY == null || dragToggledRef.current) return;
            const dy = e.clientY - startY;
            if (dy < -28) {
              dragToggledRef.current = true;
              setIsExpanded(true);
            } else if (dy > 28) {
              dragToggledRef.current = true;
              setIsExpanded(false);
            }
          }}
          onPointerUp={() => {
            dragStartYRef.current = null;
            dragToggledRef.current = false;
          }}
          onPointerCancel={() => {
            dragStartYRef.current = null;
            dragToggledRef.current = false;
          }}
          onClick={() => setIsExpanded((v) => !v)}
          role="button"
          tabIndex={0}
          aria-label={isExpanded ? "Collapse sheet" : "Expand sheet"}
        >
          <div className="h-1 w-10 rounded-full bg-white/15" />
        </div>

        {/* Fixed header area: always visible */}
        <div className="shrink-0 border-b border-amber-400/15 px-4 py-3">
          <div className="flex items-start justify-between gap-2">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <div className="text-[11px] font-semibold tracking-wide text-amber-300">France DVF</div>
              </div>
              <div className="mt-1 break-words text-[12px] font-medium text-white/90">{address}</div>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="shrink-0 rounded-full border border-white/10 p-2 text-zinc-300 hover:text-white"
              aria-label="Close"
            >
              <X className="size-4" />
            </button>
          </div>

          {!isHouseLikeUI && phase === "initial_building_state" && availableLots.length > 0 && (isApartmentLikely || showOptionalAptInput) ? (
            <div className="mt-3">
              <div className="text-[10px] font-medium text-zinc-400">Try one of these lot numbers</div>
              <div className="mt-1.5 flex flex-wrap gap-1.5">
                {availableLots.slice(0, 18).map((l) => (
                  <button
                    key={l}
                    type="button"
                    onClick={() => setLotInput(String(l))}
                    className="rounded-full border border-amber-400/20 bg-black/40 px-2.5 py-1 text-[11px] font-medium text-amber-200/90 hover:border-amber-400/35 hover:bg-black/55"
                  >
                    {l}
                  </button>
                ))}
                {availableLots.length > 18 ? (
                  <span className="self-center text-[11px] text-zinc-500">+{availableLots.length - 18} more</span>
                ) : null}
              </div>
            </div>
          ) : null}

          {!isHouseLikeUI && hasMultiUnitEvidence ? (
            <div className="mt-3 rounded-xl border border-zinc-500/20 bg-black/35 px-3 py-2.5">
              <div className="text-[10px] font-medium text-zinc-400">Apartment / lot number</div>
              <div className="mt-2 flex gap-2">
                <input
                  type="text"
                  inputMode="text"
                  value={lotInput}
                  onChange={(e) => setLotInput(e.target.value)}
                  onFocus={() => setIsLotFocused(true)}
                  onBlur={() => setIsLotFocused(false)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      e.stopPropagation();
                      submit("enter");
                    }
                  }}
                  className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-950/70 px-3 py-2 text-[16px] text-white placeholder:text-zinc-600 outline-none focus:border-amber-400/40"
                  placeholder="e.g. 9"
                />
                <button
                  type="button"
                  onClick={() => submit("button")}
                  disabled={isLoading}
                  className="shrink-0 rounded-lg border border-amber-400/35 bg-amber-400/15 px-3.5 py-2 text-[13px] font-semibold text-amber-200 hover:bg-amber-400/20 disabled:opacity-50"
                >
                  {isLoading ? "Searching…" : "Search"}
                </button>
              </div>
            </div>
          ) : null}
        </div>

        {/* Scrollable body: results only */}
        <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-4 py-3">
          <div className="rounded-xl border border-white/10 bg-black/25 p-3">
            {!isHouseLikeUI ? (
              <div className="text-[11px] text-zinc-300">
                Search results open in a separate floating card above the map.
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}

