"use client";

import * as React from "react";
import { Calendar, Database, Star, X } from "lucide-react";
import { createPortal } from "react-dom";
import { usePropertyValueInsights } from "@/hooks/use-property-value-insights";
import type { FrancePropertyResponse } from "@/lib/france-response-contract";
import {
  coerceConfidenceLabel,
  coerceDisplayString,
  coerceFranceDisplayDateString,
  coerceNullableString,
  coercePositiveNumber,
  formatFranceEuroPerSqmFromUnknown,
  formatFranceEuroTotal,
  normalizeFrancePricePerSqmForDisplay,
  unwrapScalar,
} from "@/lib/fr-display-safe";

/** Map API livability enum to area-demand copy (wording only; logic unchanged). */
function frAreaDemandLabelFromLivability(rating: string | null | undefined): string {
  const u = String(rating ?? "").trim().toUpperCase();
  if (u === "EXCELLENT" || u === "VERY GOOD" || u === "GOOD") return "High-demand area";
  if (u === "FAIR") return "Medium-demand area";
  if (u === "POOR") return "Low-demand area";
  return "—";
}

function frDataFreshnessYearFromPayload(parsed: unknown, fv: unknown, pr: unknown, fr: unknown): number | null {
  const p = parsed as Record<string, unknown> | null | undefined;
  const fvRec = fv as Record<string, unknown> | null | undefined;
  const prRec = pr as Record<string, unknown> | null | undefined;
  const frRec = fr as Record<string, unknown> | null | undefined;
  const lt = prRec?.last_transaction as Record<string, unknown> | undefined;
  const iso =
    coerceFranceDisplayDateString(p?.date_mutation) ??
    coerceFranceDisplayDateString(fvRec?.last_sale_date) ??
    coerceFranceDisplayDateString(lt?.date) ??
    coerceFranceDisplayDateString((frRec?.property as Record<string, unknown> | undefined)?.transactionDate);
  if (iso && /^\d{4}/.test(iso)) return Number.parseInt(iso.slice(0, 4), 10);
  return null;
}

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
  /** When false, do not treat transactionValue/last_sale as current estimate. */
  has_current_valuation?: boolean | null;
  /** Back-compat alias; API may send both. */
  source_label?: string | null;
  /** valuation_response path: last recorded sale amount (from pr.last_transaction.amount). */
  last_sale_price?: number | null;
  /** valuation_response path: last recorded sale date (from pr.last_transaction.date). */
  last_sale_date?: string | null;
  /** Truthful disclosure: exact | same_building_similar_unit | same_street_similar_house | nearby_similar_house | area_fallback */
  last_transaction_match_type?: string | null;
  /** Source address for comparable/area transactions (e.g. "8 Rue X, 06400 Cannes"). */
  last_transaction_source_address?: string | null;
  /** Human-readable disclosure text. */
  last_transaction_disclosure?: string | null;
  /** Surface source for total estimate: "exact property" | "building median" | "street similar" | "nearby (n=N)" */
  surface_source_label?: string | null;
  /** Surface m² used when displaying total estimated value */
  surface_m2_used?: number | null;
  /** Source unit for displayed result, e.g. "Apartment 25" or null for building-level */
  source_unit_display?: string | null;
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
  const addressForApi = (typedAddressForFrance?.trim() || address?.trim() || "").trim();
  const rawForApi = (rawInputAddressForFrance?.trim() || typedAddressForFrance?.trim() || addressForApi || address?.trim() || "").trim();

  const [lotInput, setLotInput] = React.useState("");
  const [requestedLot, setRequestedLot] = React.useState<string | undefined>(undefined);
  const [trigger, setTrigger] = React.useState(0);
  const [isLotFocused, setIsLotFocused] = React.useState(false);
  const [keyboardInsetPx, setKeyboardInsetPx] = React.useState(0);
  const [hasSubmittedLotSearch, setHasSubmittedLotSearch] = React.useState(false);
  const [isResultCardOpen, setIsResultCardOpen] = React.useState(false);
  const [resolvedForDisplay, setResolvedForDisplay] = React.useState<{
    fr: FrancePropertyResponse | null;
    legacy: { averageBuildingValue: number | null; livabilityRating?: string | null } | null;
    fr_runtime_debug?: Record<string, unknown> | null;
  } | null>(null);
  const [isExpanded, setIsExpanded] = React.useState(false);
  const [showOptionalAptInput, setShowOptionalAptInput] = React.useState(false);
  const dragStartYRef = React.useRef<number | null>(null);
  const dragToggledRef = React.useRef(false);

  const { data, isLoading, refetch } = usePropertyValueInsights(addressForApi || rawForApi, "FR", {
    latitude: position.lat,
    longitude: position.lng,
    aptNumber: (() => {
      const t = (requestedLot ?? (hasSubmittedLotSearch ? lotInput : "")).trim();
      return t || undefined;
    })(),
    postcode,
    refetchTrigger: trigger,
    countryCode: "FR",
    rawInputAddress: rawForApi || undefined,
  });

  // Once we have a building payload for this address, never render a no-data France state.
  const lastBuildingPayloadRef = React.useRef<typeof data>(null);
  const addressKey = React.useMemo(() => `${addressForApi.trim().toLowerCase()}|pc:${(postcode ?? "").trim().toLowerCase()}`, [addressForApi, postcode]);
  React.useEffect(() => {
    lastBuildingPayloadRef.current = null;
    setRequestedLot(undefined);
    setLotInput("");
    setHasSubmittedLotSearch(false);
    setIsResultCardOpen(false);
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

  // Show result card immediately when loading starts (no intermediate popup).
  React.useEffect(() => {
    if (isLoading) setIsResultCardOpen(true);
  }, [isLoading]);

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

  // property_type_final: backend official-data-first decision. UI uses ONLY this.
  // house => never ask for apartment/unit; apartment => ask when needed; unknown => fallback decides.
  const propertyTypeFinal = (parsed?.property_type_final ?? parsed?.fr_detect ?? "unknown") as string;
  const propertyTypeSource = (parsed?.property_type_source ?? null) as string | null;
  const propertyTypeConfidence = (parsed?.property_type_confidence ?? null) as string | null;
  const propertyTypeFinalNorm =
    propertyTypeFinal === "house" ? "house"
      : propertyTypeFinal === "apartment" ? "apartment"
      : "unknown";
  const detectClassIsHouseFromApi = propertyTypeFinalNorm === "house";
  const frDetectFinalClass: "apartment" | "house" | "unclear" =
    propertyTypeFinal === "house" ? "house"
      : propertyTypeFinal === "unknown" || propertyTypeFinal === "unclear" ? "unclear"
      : "apartment";
  const isHouseLikeUI = detectClassIsHouseFromApi;

  // Single source of truth: backend fr_should_prompt_lot from BigQuery unit flags. House blocks only when backend does not ask for a lot.
  const rdForLot = (parsed as any)?.fr_runtime_debug ?? (data as any)?.fr_runtime_debug ?? null;
  const frDisplayContext = (parsed?.fr_display_context ?? rdForLot?.fr_display_context ?? null) as
    | "exact_unit"
    | "exact_address"
    | "building_level"
    | "street_level"
    | "area_level"
    | "unknown"
    | null;

  const normalizeLotTokenForContext = React.useCallback((s: string) => {
    return s.trim().toUpperCase().replace(/^0+(?=[0-9])/, "") || s.trim().toUpperCase();
  }, []);

  /** Second line only for exact_address (headline is already "Official record — address level"). */
  const standardContextExplanation = React.useMemo(() => {
    const dc = frDisplayContext;
    const fvDisp = (parsed as { fr_valuation_display?: { source_unit_display?: string | null } } | null)
      ?.fr_valuation_display;
    const sourceUnitLine = String(fvDisp?.source_unit_display ?? "").trim();
    const mSrc =
      /(?:APARTMENT|APPART|APT|LOT|UNIT|N°|Nº|#)\s*([A-Z0-9]+)/i.exec(sourceUnitLine)?.[1] ??
      /\b(\d{1,4})\s*$/i.exec(sourceUnitLine)?.[1] ??
      "";
    const reqLot = String(normalized?.requestedLot ?? requestedLot ?? "").trim();
    const reqTok = reqLot ? normalizeLotTokenForContext(reqLot) : "";
    const srcTok = mSrc ? normalizeLotTokenForContext(mSrc) : "";
    const sourceDiffersFromRequested =
      reqTok.length > 0 && srcTok.length > 0 && reqTok !== srcTok;
    const noSourceUnitForUser = sourceUnitLine.length === 0;
    const isAddressLevelOfficialRecord =
      dc === "exact_address" ||
      ((dc == null || dc === "unknown") && normalized?.resultType === "exact_address");
    const displayContextBlocksAddrExplanation =
      dc === "exact_unit" ||
      dc === "building_level" ||
      dc === "street_level" ||
      dc === "area_level";

    if (
      isAddressLevelOfficialRecord &&
      !displayContextBlocksAddrExplanation &&
      (noSourceUnitForUser || sourceDiffersFromRequested)
    ) {
      return "This is an address-level official record and may reflect multiple units.";
    }
    return null;
  }, [
    frDisplayContext,
    normalizeLotTokenForContext,
    normalized?.resultType,
    normalized?.requestedLot,
    parsed,
    requestedLot,
  ]);

  const backendShouldPromptLot = rdForLot?.fr_should_prompt_lot === true;
  const backendLotPromptVisible = rdForLot?.fr_lot_prompt_visible === true;
  const backendWantsLotUi =
    (backendLotPromptVisible || backendShouldPromptLot) &&
    (!detectClassIsHouseFromApi || backendShouldPromptLot);
  const shouldShowApartmentInput = backendWantsLotUi;
  const shouldForceLotFirstFlow = shouldShowApartmentInput;
  const isPropertyTypeUnknown = frDetectFinalClass === "unclear";
  const effectiveDetectClass = frDetectFinalClass;
  const frFlowSourceOfTruth = frDetectFinalClass;
  const frDetect = propertyTypeFinal;
  const detectClassIsHouse = detectClassIsHouseFromApi;
  const lotPromptGenuinelyRequired =
    backendShouldPromptLot && !(requestedLot ?? "").trim();

  // Apartment block active: backend wants lot + no lot + not yet submitted. Triggers lot prompt UI.
  const apartmentBlockActive = lotPromptGenuinelyRequired && !hasSubmittedLotSearch;

  const lotPromptVisible = backendWantsLotUi;

  const frLotPromptVisibleReason =
    detectClassIsHouse && !backendShouldPromptLot
      ? "house_blocks_lot_input"
      : backendShouldPromptLot
        ? "fr_should_prompt_lot_true"
        : "fr_should_prompt_lot_false_or_unset";

  // House-direct flow: skip lot UI unless BigQuery-backed prompt says otherwise.
  const isHouseDirectFlow =
    (frDetectFinalClass === "house" && !backendShouldPromptLot) ||
    (frDetectFinalClass === "unclear" && !shouldShowApartmentInput) ||
    (normalized?.resultType === "exact_house" && frAddressFetchDone && !backendShouldPromptLot);

  // Close result card ONLY when apartment block is active. Never close for house/direct flows.
  React.useEffect(() => {
    if (isHouseDirectFlow) return;
    if (!apartmentBlockActive) return;
    setIsResultCardOpen(false);
  }, [isHouseDirectFlow, apartmentBlockActive]);

  const uiState = isResultCardOpen
    ? "result-visible"
    : shouldShowApartmentInput
      ? "apartment-input"
      : isHouseLikeUI
        ? "house-direct"
        : "house-direct";

  const resultCardBlocked = apartmentBlockActive;

  // Strict UI gate for lot-prompt flow: when block active, reset to lot prompt state.
  React.useEffect(() => {
    if (isHouseDirectFlow) return;
    if (!backendShouldPromptLot || detectClassIsHouse) return;
    if (!resultCardBlocked) return;

    if (hasSubmittedLotSearch) setHasSubmittedLotSearch(false);
    if (isResultCardOpen) setIsResultCardOpen(false);
    if (resolvedForDisplay != null) setResolvedForDisplay(null);
    if (isExpanded) setIsExpanded(false);
  }, [isHouseDirectFlow, backendShouldPromptLot, detectClassIsHouse, resultCardBlocked, hasSubmittedLotSearch, isResultCardOpen, resolvedForDisplay, isExpanded]);

  const displayConfidence = React.useMemo(() => {
    return coerceConfidenceLabel(normalized?.confidence as unknown);
  }, [normalized?.confidence]);

  const sourceLabel = React.useMemo(() => {
    if (frDisplayContext === "exact_unit") return isHouseLikeUI ? "Exact property match" : "Exact lot match";
    if (frDisplayContext === "exact_address") return "Exact address match";
    if (frDisplayContext === "building_level") return "Building-level data";
    if (frDisplayContext === "street_level") return "Street-level data";
    if (frDisplayContext === "area_level") return "Wider area data";
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
  }, [frDisplayContext, normalized?.resultType, normalized?.comparableScope, isHouseLikeUI]);

  const valueLabel = React.useMemo(() => {
    if (frDisplayContext === "exact_unit") return "Last recorded transaction (this unit)";
    if (frDisplayContext === "exact_address") return "Address-level official transaction";
    if (frDisplayContext === "building_level") return "Building-level estimate";
    if (frDisplayContext === "street_level") return "Street-level estimate";
    if (frDisplayContext === "area_level") return "Area-level estimate";
    const rt = normalized?.resultType;
    if (rt === "exact_apartment") return "Last recorded transaction";
    if (rt === "exact_address") return "Address-level transaction";
    if (rt === "exact_house") return "Last recorded transaction";
    if (rt === "similar_apartment_same_building") return "Similar apartment transaction";
    if (rt === "building_similar_unit") return "Building apartment estimate";
    if (rt === "building_level" || rt === "building_fallback") return "Building average";
    if (rt === "nearby_comparable" || rt === "comparables_only") return "Best available nearby comparable";
    return null;
  }, [frDisplayContext, normalized?.resultType]);

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

  const apartmentLotGateActive = lotPromptGenuinelyRequired;

  const phase: Phase = React.useMemo(() => {
    if (apartmentLotGateActive) return "initial_building_state";
    if (isExactApartment(effectiveData)) return "exact_apartment_match_state";
    if (isFranceBuildingSimilarWin(effectiveData)) return "building_similar_match_state";
    if (!hasSubmittedLotSearch) {
      return hasUsefulBuildingData ? "initial_building_state" : "no_result_state";
    }
    return hasUsefulBuildingData ? "searched_no_exact_match_but_building_exists_state" : "no_result_state";
  }, [effectiveData, hasSubmittedLotSearch, hasUsefulBuildingData, apartmentLotGateActive, isFranceBuildingSimilarWin]);

  // Houses/single-unit: open results when NOT apartment and not waiting for lot.
  React.useEffect(() => {
    if (frDetectFinalClass === "apartment") return;
    if (lotPromptGenuinelyRequired) return;
    if (!frAddressFetchDone) return;
    if (shouldShowApartmentInput) return;
    if (hasSubmittedLotSearch) return;
    if (isLoading) return;
    if (!effectiveData) return;
    setHasSubmittedLotSearch(true);
    setIsResultCardOpen(true);
  }, [frDetectFinalClass, lotPromptGenuinelyRequired, shouldShowApartmentInput, hasSubmittedLotSearch, isLoading, frAddressFetchDone, effectiveData, backendShouldPromptLot, detectClassIsHouse]);

  const badge = React.useMemo(() => {
    // building / street / area: primary context is a single headline string (no separate tier badge — avoids mixing labels).
    if (frDisplayContext && phase !== "initial_building_state" && phase !== "no_result_state") {
      if (frDisplayContext === "exact_unit") {
        return { label: "Official record — exact property", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
      }
      if (frDisplayContext === "exact_address") {
        return { label: "Official record — address level", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
      }
      if (
        frDisplayContext === "building_level" ||
        frDisplayContext === "street_level" ||
        frDisplayContext === "area_level"
      ) {
        return null;
      }
    }
    if (phase === "exact_apartment_match_state") {
      if (normalized?.resultType === "exact_house") {
        return { label: "Official record — this property", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
      }
      if (normalized?.resultType === "exact_address") {
        return { label: "Official record — this address", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
      }
      return isHouseLikeUI
        ? { label: "Official record — this property", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" }
        : { label: "Official record — exact property", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
    }
    if (phase === "building_similar_match_state") {
      return {
        label: "Estimate from this building",
        tone: "bg-teal-500/10 border-teal-500/25 text-teal-200",
      };
    }
    if (phase === "searched_no_exact_match_but_building_exists_state") {
      const rt = normalized?.resultType;
      if (rt === "similar_apartment_same_building") {
        return isHouseLikeUI
          ? { label: "Comparable sale in this building", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" }
          : { label: "Comparable sale in this building", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
      }
      if (rt === "nearby_comparable") {
        return isHouseLikeUI
          ? { label: "Based on nearby official sales", tone: "bg-sky-500/10 border-sky-500/25 text-sky-200" }
          : { label: "Based on nearby official sales", tone: "bg-sky-500/10 border-sky-500/25 text-sky-200" };
      }
      return { label: "Valuation ready", tone: "bg-amber-500/10 border-amber-500/25 text-amber-200" };
    }
    return null;
  }, [phase, frDisplayContext, normalized?.resultType, isHouseLikeUI]);

  const subtitle = React.useMemo(() => {
    if (isHouseLikeUI) return "Showing the strongest official value we can tie to this address.";
    if (isPropertyTypeUnknown && phase === "initial_building_state") {
      return "We found this building. Add your unit number if you have one.";
    }
    if (phase === "initial_building_state") {
      return "We found this building. Add your unit number for a tighter match.";
    }
    if (
      phase !== "no_result_state" &&
      (frDisplayContext === "building_level" ||
        frDisplayContext === "street_level" ||
        frDisplayContext === "area_level")
    ) {
      return "";
    }
    if (phase === "exact_apartment_match_state") {
      const lot = (requestedLot ?? lotInput).trim();
      if (frDisplayContext === "exact_unit") {
        return "";
      }
      if (frDisplayContext === "exact_address" || normalized?.resultType === "exact_address") {
        return "";
      }
      if (isHouseLikeUI) return lot ? `Matched to your property (lot ${lot}).` : "Matched to your property.";
      return lot ? `Matched to your unit (${lot}).` : "Matched to your unit.";
    }
    if (phase === "building_similar_match_state") {
      const lot = (requestedLot ?? "").trim();
      return lot
        ? `No separate registry line for unit ${lot}. Estimate uses similar sales in this building.`
        : "No separate registry line for this unit. Estimate uses similar sales in this building.";
    }
    if (phase === "searched_no_exact_match_but_building_exists_state") {
      const lot = (requestedLot ?? "").trim();
      const rt = normalized?.resultType;
      const ws = String((parsed as any)?.fr_runtime_debug?.winning_step ?? "").trim();
      if (ws === "street_fallback") {
        return lot
          ? `No unit-specific record for ${lot}. Estimate uses the same street.`
          : "No unit-specific record. Estimate uses the same street.";
      }
      if (ws === "commune_fallback") {
        return lot
          ? `No unit-specific record for ${lot}. Estimate uses the wider area.`
          : "No unit-specific record. Estimate uses the wider area.";
      }
      if (ws === "building_level" || ws === "building_fallback") {
        return lot
          ? `No unit-specific record for ${lot}. Estimate uses this building.`
          : "No unit-specific record. Estimate uses this building.";
      }
      if (isHouseLikeUI) {
        if (rt === "similar_apartment_same_building" || rt === "nearby_comparable") {
          return lot
            ? `No exact match for lot ${lot}. Based on nearby official sales.`
            : "No exact match. Based on nearby official sales.";
        }
        return lot
          ? `No exact match for lot ${lot}. Showing building-level official data.`
          : "No exact match. Showing building-level official data.";
      }
      if (rt === "similar_apartment_same_building") {
        return lot
          ? `No exact unit for ${lot}. Showing the closest comparable in this building.`
          : "No exact unit on file. Showing the closest comparable in this building.";
      }
      if (rt === "nearby_comparable") {
        return lot
          ? `No exact unit for ${lot}. Showing a nearby comparable.`
          : "No exact unit on file. Showing a nearby comparable.";
      }
      return lot
        ? `No exact unit for ${lot}. Showing building-level official data.`
        : "No exact unit on file. Showing building-level official data.";
    }
    return isHouseLikeUI ? "No official transaction tied to this address yet." : "No official transaction tied to this unit yet.";
  }, [phase, frDisplayContext, requestedLot, lotInput, normalized?.resultType, isHouseLikeUI, isPropertyTypeUnknown, parsed?.fr_runtime_debug]);

  const submit = React.useCallback((source: "enter" | "button") => {
    const lot = lotInput.trim();
    // Hard block: when backend requires lot, block until lot is provided.
    if (lotPromptGenuinelyRequired && !lot) return;
    // Do not block lot search while the initial building-level request is still in flight:
    // otherwise Enter / refetch never runs with apt_number and the API stays at submitted_lot=null.
    setRequestedLot(lot || undefined);
    // Hard guarantee: never show any previous result while a new lot search is in flight.
    // We only render a final result once the latest request resolves and matches the requested lot.
    setResolvedForDisplay(null);
    setHasSubmittedLotSearch(true);
    setTrigger((t) => t + 1);
    setIsResultCardOpen(true);
  }, [lotInput, lotPromptGenuinelyRequired]);

  const prevIsLoadingRef = React.useRef<boolean>(false);
  React.useEffect(() => {
    if (!hasSubmittedLotSearch) {
      prevIsLoadingRef.current = isLoading;
      return;
    }
    // Detect transition: lot-search request was in-flight -> now resolved.
    if (prevIsLoadingRef.current && !isLoading) {
      const lotSubmitted = !!(requestedLot ?? "").trim();
      const resultCardBlocked = lotPromptGenuinelyRequired && !lotSubmitted;
      const responsePayloadTag = (normalized as any)?.resultType ?? (data as any)?.message ?? null;
      const responseStatus = (normalized as any)?.success ?? null;

      const requestFailed = !normalized;
      const finalStreetAvgMessage = (normalized as any)?.property_result?.street_average_message ?? null;
      const winningSourceLabel =
        finalStreetAvgMessage === "Based on recent sales on this street"
          ? "Based on recent sales on this street"
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
          : finalStreetAvgMessage === "Based on recent sales on this street"
            ? "street_fallback"
            : finalStreetAvgMessage === "Similar properties in same commune"
              ? "commune_fallback"
              : "no_data";

    }
    prevIsLoadingRef.current = isLoading;
  }, [hasSubmittedLotSearch, isLoading, requestedLot, lotPromptGenuinelyRequired, normalized, data]);

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
  }, [
    isResultCardOpen,
    hasSubmittedLotSearch,
    isLoading,
    normalized,
    requestedLot,
    averageBuildingValue,
    legacyLivability,
    parsed,
    data,
    addressKey,
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

  /** Same-address only: clear unit/lot UI state; do not bump `trigger` or alter address. */
  const resetToApartmentPrompt = React.useCallback(() => {
    setRequestedLot(undefined);
    setLotInput("");
    setHasSubmittedLotSearch(false);
    setIsResultCardOpen(false);
    setResolvedForDisplay(null);
    setIsExpanded(false);
  }, []);

  // Do not tie to `shouldShowApartmentInput`: after a lot is submitted the API sets
  // `fr_should_prompt_lot` false, so that flag would hide this button exactly when results show.
  const showCheckAnotherApartmentButton =
    hasSubmittedLotSearch &&
    !isHouseLikeUI &&
    !isHouseDirectFlow &&
    (frDetectFinalClass === "apartment" || frDetectFinalClass === "unclear");

  // NOTE: do NOT memoize this portal node. We need immediate re-render on UI-only state
  // changes (no waiting for a refetch).
  const resultCardNode = (() => {
    if (typeof document === "undefined") return null;
    if (!isResultCardOpen) return null;

    const houseFlowActive = isHouseDirectFlow;
    const apartmentBlockActiveNow = apartmentBlockActive;

    if (apartmentBlockActiveNow) return null;

    const showForHouseOrDirect = houseFlowActive && (frAddressFetchDone || isLoading);
    const showForApartment = hasSubmittedLotSearch || (isLoading && frDetectFinalClass === "apartment");
    const showForNonLotFlow = frAddressFetchDone && effectiveData && frDetectFinalClass !== "apartment";

    if (!showForHouseOrDirect && !showForApartment && !showForNonLotFlow) {
      return null;
    }

    const isLoadingNow = isLoading;
    const fr = resolvedForDisplay?.fr ?? normalized ?? null;
    const legacy = resolvedForDisplay?.legacy ?? { averageBuildingValue, livabilityRating: legacyLivability };
    const frRuntimeDebug =
      resolvedForDisplay?.fr_runtime_debug ?? (data as any)?.fr_runtime_debug ?? (parsed as any)?.fr_runtime_debug ?? null;
    const rd: any = frRuntimeDebug ?? null;
    // Lot-related debug must match the source driving lot visibility (rdForLot) so UI and debug always agree.
    /**
     * Text between "•" and "reference sale" on the France result card. Only primitives: unwrap
     * value / text / label / date (never `String(object)` → "[object Object]").
     * Handles: Date instance, ISO string, timestamp, BigQuery { value }, { date }, { iso }, { year, month, day }.
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
          if (t === "[object Object]") return null;
          return t.length ? t : null;
        }
        if (typeof v === "number" && Number.isFinite(v)) return String(v);
        if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString().slice(0, 10);
        if (typeof v !== "object" || Array.isArray(v)) return null;
        const o = v as Record<string, unknown>;
        if ("value" in o && o.value !== undefined) return step(o.value, depth + 1);
        if ("text" in o && o.text !== undefined) return step(o.text, depth + 1);
        if ("label" in o && o.label !== undefined) return step(o.label, depth + 1);
        if ("date" in o && o.date !== undefined) return step(o.date, depth + 1);
        if ("iso" in o && o.iso !== undefined) return step(o.iso, depth + 1);
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

    const fvForDate = (parsed as any)?.fr_valuation_display;
    const referenceSaleValue: string | null = isLoadingNow
      ? null
      : referenceSaleDateLabelFromRaw(fr?.property?.transactionDate as unknown) ??
        referenceSaleDateLabelFromRaw((pr as any)?.last_transaction?.date as unknown) ??
        referenceSaleDateLabelFromRaw(fvForDate?.last_sale_date as unknown);

    const contextHeadline =
      frDisplayContext === "building_level"
        ? "This estimate is based on recent sales within this building."
        : frDisplayContext === "street_level"
          ? "This estimate is based on recent sales on this street."
          : frDisplayContext === "area_level"
            ? "This estimate is based on similar properties in the surrounding area."
            : frDisplayContext === "exact_unit"
              ? "Official record — exact property"
              : frDisplayContext === "exact_address"
                ? "Official record — address level"
                : (frDisplayContext == null || frDisplayContext === "unknown") && normalized?.resultType === "exact_address"
                  ? "Official record — address level"
                  : null;

    const title = isLoadingNow
      ? "Fetching official records…"
      : contextHeadline ??
        badge?.label ??
        (!fr
          ? "No result"
          : fr?.success === false
            ? "No result"
            : "Valuation ready");
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
    const prExact = coercePositiveNumber(pr?.exact_value);
    const hasCurrentValuation = fv?.has_current_valuation !== false;
    const estimatedSources = fvEst ?? topEstimated ?? prExact;
    const buildingLevelValue =
      fr?.resultType === "building_level"
        ? coercePositiveNumber(fr?.buildingStats?.avgTransactionValue as unknown) ??
          coercePositiveNumber(legacy?.averageBuildingValue as unknown)
        : null;
    const rawValue =
      fr?.resultType === "building_similar_unit"
        ? estimatedSources ?? txFromProp ?? null
        : hasCurrentValuation
          ? txFromProp ?? estimatedSources ?? buildingLevelValue
          : estimatedSources ?? buildingLevelValue;
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
    const hasDisplayFromApi = (parsed as any)?.fr_valuation_display?.has_display_value === true || (parsed as any)?.fr_runtime_debug?.fr_final_has_display_value === true;
    const fallbackWinStep = /^(street_fallback|commune_fallback|nearby_fallback|commune_emergency|building_profile)$/.test(String(rd?.winning_step ?? ""));
    const hasFallbackDisplayData = hasDisplayFromApi || (fallbackWinStep && (ppm2FromApi != null || coercePositiveNumber((parsed as any)?.display_value) != null || coercePositiveNumber((parsed as any)?.estimated_value) != null));
    const isNoResult =
      !isLoadingNow &&
      (!fr || fr?.resultType === "no_result" || fr?.resultType === "no_reliable_data" || fr?.success === false) &&
      !hasFallbackDisplayData;

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
      if (
        frDisplayContext === "exact_unit" ||
        frDisplayContext === "exact_address" ||
        frDisplayContext === "building_level" ||
        frDisplayContext === "street_level" ||
        frDisplayContext === "area_level"
      ) {
        return null;
      }
      const ws = winningStepStr;
      if (ws === "exact_unit" || ws === "exact_address" || ws === "exact_house") return "Based on this property's recorded sale";
      if (ws === "exact_approximate" || ws === "building_level" || ws === "building_fallback" || ws === "building_similar_unit")
        return "Based on recent sales in this building";
      if (ws === "street_fallback") return "Based on recent sales on this street";
      if (ws === "commune_fallback" || ws === "nearby_fallback")
        return "Based on similar properties in the area";
      if (fr?.resultType === "similar_apartment_same_building")
        return isHouseLikeUI ? "Based on recent sales in the area" : "Based on recent sales in this building";
      if (fr?.resultType === "nearby_comparable") return "Based on similar properties in the area";
      if (ws === "no_data" && winningSourceFromApi) return winningSourceFromApi;
      return "Based on recent sales data";
    })();

    // Rule: estimated_value null + price_per_m2 (or display_value) → show €/m² in headline, not "—".
    const hasPricePerM2Headline =
      !hasValue &&
      ppm2FromApi != null &&
      ppm2Display != null &&
      !isNoResult &&
      !isLoadingNow &&
      !isSuspiciousFallback;

    const apiValueRange = (parsed as any)?.value_range ?? (data as any)?.value_range;
    const valueRange = (() => {
      if (isLoadingNow) return null;
      if (isNoResult) return null;
      if (isSuspiciousFallback) return null;
      // API-provided range (France valuation_response).
      if (apiValueRange?.low_estimate != null && apiValueRange?.high_estimate != null) {
        const lo = coercePositiveNumber(apiValueRange.low_estimate);
        const hi = coercePositiveNumber(apiValueRange.high_estimate);
        if (lo != null && hi != null && hi >= lo) return { min: lo, max: hi };
      }
      // Fallback: range from building comparables when enough data.
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
      ? coercePositiveNumber(fr?.property?.transactionValue as unknown) ??
        coercePositiveNumber((pr as any)?.last_transaction?.amount as unknown) ??
        coercePositiveNumber(fv?.last_sale_price as unknown)
      : null;

    const txMatchType = (fv?.last_transaction_match_type ?? (pr as any)?.last_transaction?.match_type ?? "exact") as string;
    const txSourceAddress = fv?.last_transaction_source_address ?? (pr as any)?.last_transaction?.source_address;
    const lastTransactionSectionTitle = "Last official sale";
    const lastTransactionSummaryLine = (() => {
      if (isLoadingNow) return "—";
      if (frDisplayContext === "area_level") {
        return "Area reference — not this unit’s exact recorded sale";
      }
      if (frDisplayContext === "street_level") {
        return "Street-level reference — not necessarily this unit’s exact sale";
      }
      const amt = lastTxAmountPositive;
      const hasUsableAmount = amt != null && amt > 0;
      const hasUsableDate = referenceSaleValue != null && referenceSaleValue.trim().length > 0;
      if (fr?.resultType === "building_similar_unit" && !isNoResult && hasUsableAmount) {
        return hasUsableDate
          ? `Last sold for ${formatFranceEuroTotal(amt)} • ${referenceSaleValue}`
          : `Last sold for ${formatFranceEuroTotal(amt)}`;
      }
      if (hasUsableAmount && hasUsableDate) {
        return `Last sold for ${formatFranceEuroTotal(amt)} • ${referenceSaleValue}`;
      }
      if (hasUsableAmount) {
        return hasUsableDate ? `Last sold for ${formatFranceEuroTotal(amt)} • ${referenceSaleValue}` : `Last sold for ${formatFranceEuroTotal(amt)}`;
      }
      if (hasUsableDate) {
        return referenceSaleValue;
      }
      return "No recent sale on file";
    })();
    const pctSinceLastSaleUi: { text: string; tone: "up" | "down" | "flat" } | null = (() => {
      if (isLoadingNow || isNoResult || isSuspiciousFallback) return null;
      if (!hasValue || typeof rawValue !== "number" || rawValue <= 0) return null;
      const last = lastTxAmountPositive;
      if (last == null || last <= 0) return null;
      const pct = ((rawValue - last) / last) * 100;
      if (!Number.isFinite(pct)) return null;
      const abs = Math.abs(pct);
      const decimals = abs >= 10 ? 0 : 1;
      const rounded = Number(pct.toFixed(decimals));
      if (Math.abs(pct) < 1) {
        return { text: "The price is unchanged since the last transaction", tone: "flat" };
      }
      const magnitude = Math.abs(rounded);
      const pctLabel = decimals === 0 ? String(Math.round(magnitude)) : String(magnitude);
      if (rounded > 0) {
        return { text: `The price has increased since the last transaction by ${pctLabel}%`, tone: "up" };
      }
      if (rounded < 0) {
        return { text: `The price has decreased since the last transaction by ${pctLabel}%`, tone: "down" };
      }
      return { text: "The price is unchanged since the last transaction", tone: "flat" };
    })();
    const dataFreshnessYear = frDataFreshnessYearFromPayload(parsed, fv, pr, fr);
    // Multi-unit note: current finished API response only (never `resolvedForDisplay` / prior fetch).
    const currentResponse = data as Record<string, unknown> | null | undefined;
    const showMultiUnitTransactionNote =
      !isLoadingNow && currentResponse != null && currentResponse.multi_unit_transaction === true;
    const streetAvgMsg = coerceDisplayString(pr?.street_average_message as unknown, "").trim();
    const sourceText = isLoadingNow
      ? "—"
      : frDisplayContext === "building_level"
        ? "This estimate is based on recent sales within this building."
        : frDisplayContext === "street_level"
          ? "This estimate is based on recent sales on this street."
          : frDisplayContext === "area_level"
            ? "This estimate is based on similar properties in the surrounding area."
            : hasFranceValuationWin && winningSourceFromApi
              ? winningSourceFromApi
              : streetAvgMsg && !/no reliable data found/i.test(streetAvgMsg)
                ? streetAvgMsg
                : sourceLabel || (isNoResult ? "No reliable data found" : "—");
    const confidenceText = isLoadingNow ? "—" : (displayConfidence ? displayConfidence : "—");
    const livabilityText = isLoadingNow
      ? "—"
      : frAreaDemandLabelFromLivability(coerceDisplayString(legacy?.livabilityRating as unknown, "") || null);
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
            className="pointer-events-auto max-h-[min(72vh,560px)] shrink-0 overflow-y-auto overflow-x-hidden rounded-[10px] border border-white/10 bg-[#0b0d10] shadow-md"
            style={{
              width: 320,
              maxWidth: 320,
              padding: 10,
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
              {!isLoadingNow &&
              !isNoResult &&
              standardContextExplanation &&
              (frDisplayContext === "exact_address" ||
                ((frDisplayContext == null || frDisplayContext === "unknown") && normalized?.resultType === "exact_address")) ? (
                <div className="mt-1 text-[10px] leading-snug text-zinc-400/90">{standardContextExplanation}</div>
              ) : !isLoadingNow &&
                  subtitle.trim() &&
                  frDisplayContext !== "building_level" &&
                  frDisplayContext !== "street_level" &&
                  frDisplayContext !== "area_level" &&
                  frDisplayContext !== "exact_unit" &&
                  frDisplayContext !== "exact_address" ? (
                <div className="mt-1 text-[10px] leading-snug text-zinc-400/90">{subtitle}</div>
              ) : null}
            </div>

            {/* Core summary – premium layout */}
            <div className="mt-1.5 space-y-[6px]">
              {/* 1) Estimated value – hero */}
              <div className="rounded-[10px] border border-amber-400/15 bg-gradient-to-b from-black/30 to-black/50 p-3 shadow-inner shadow-black/40">
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <div className="text-[10px] font-semibold uppercase tracking-[0.16em] leading-tight text-zinc-400">
                      Estimated True Value
                    </div>
                    <div
                      className={
                        isLoadingNow
                          ? `mt-1.5 flex items-center gap-2 whitespace-nowrap text-[20px] font-medium leading-tight ${goldTextClass}`
                          : isNoResult
                            ? "mt-1.5 text-sm font-semibold whitespace-nowrap leading-tight text-zinc-100"
                            : `mt-1.5 text-[28px] sm:text-[30px] whitespace-nowrap font-bold leading-[1.05] tracking-tight ${goldTextClass}`
                      }
                    >
                      {isLoadingNow ? (
                        <>
                          <span className={`inline-flex size-3.5 animate-spin rounded-full border-2 border-white/15 border-t-amber-200`} aria-hidden="true" />
                          <span>Searching...</span>
                        </>
                      ) : (
                        (isNoResult
                          ? "No exact data found"
                          : mainValue)
                      )}
                    </div>
                    {pctSinceLastSaleUi != null && !isLoadingNow && !isNoResult ? (
                      <div
                        className={
                          pctSinceLastSaleUi.tone === "up"
                            ? "mt-1.5 text-[13px] font-semibold text-emerald-400"
                            : pctSinceLastSaleUi.tone === "down"
                              ? "mt-1.5 text-[13px] font-semibold text-rose-400"
                              : "mt-1.5 text-[13px] font-semibold text-zinc-400"
                        }
                      >
                        {pctSinceLastSaleUi.text}
                      </div>
                    ) : null}
                    <div className="mt-2 text-[11px] font-medium leading-snug text-zinc-400/95">
                      Data source: Official government records
                    </div>
                  </div>
                  {!isLoadingNow && !isNoResult ? (
                    <div className={`shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-semibold ${confidenceTone}`}>
                      {confidenceText}
                    </div>
                  ) : null}
                </div>
                {valueRange != null && !isLoadingNow && !isNoResult ? (
                  <div className="mt-1.5 text-xs font-medium text-zinc-400/90">
                    Market range: {formatFranceEuroTotal(valueRange.min)} – {formatFranceEuroTotal(valueRange.max)}
                  </div>
                ) : null}
                {!isLoadingNow && !isNoResult && hasValue && fv?.surface_source_label && fv?.surface_m2_used != null ? (
                  <div className="mt-1 text-[10px] text-zinc-400/80">
                    Based on ~{Math.round(fv.surface_m2_used)} m² ({fv.surface_source_label})
                  </div>
                ) : null}
                {!isLoadingNow &&
                !isNoResult &&
                (hasValue || ppm2Display != null) &&
                frDisplayContext === "exact_unit" &&
                fv?.source_unit_display?.trim() ? (
                  <div className="mt-1 text-[10px] text-zinc-400/80">Source unit: {fv.source_unit_display.trim()}</div>
                ) : null}
                {dataFreshnessYear != null && !isLoadingNow && !isNoResult ? (
                  <div className="mt-1.5 text-[10px] leading-snug text-zinc-500">
                    Data updated: {dataFreshnessYear}, may be delayed
                  </div>
                ) : null}
              </div>

              {/* 2) Last transaction – human wording */}
              <div className="rounded-[10px] border border-white/10 bg-black/20 px-2.5 py-2">
                <div className="text-[11px] font-medium uppercase tracking-[0.14em] leading-tight text-zinc-400/70">
                  {lastTransactionSectionTitle}
                </div>
                <div className="mt-1 text-[14px] font-semibold leading-tight text-white">
                  {lastTransactionSummaryLine}
                </div>
                {showMultiUnitTransactionNote ? (
                  <div className="mt-1 text-[11px] font-medium text-amber-200/90">
                    Transaction includes multiple units
                  </div>
                ) : null}
                {(frDisplayContext === "exact_unit" || frDisplayContext === "exact_address") &&
                txSourceAddress &&
                txSourceAddress.trim() ? (
                  <div className="mt-0.5 text-[11px] text-zinc-400/90">
                    Recorded at: {txSourceAddress}
                  </div>
                ) : null}
              </div>

              {/* 3) Price per m² (boxed) */}
              {displayPricePerSqm != null && (hasValue || valueRange != null) ? (
                <div className="rounded-[10px] border border-white/10 bg-black/20 px-2.5 py-2">
                  <div className="text-[11px] font-medium uppercase tracking-[0.14em] leading-tight text-zinc-400/70">
                    Price per m²
                  </div>
                  <div className="mt-1 text-[14px] font-semibold leading-tight text-white">
                    {formatFranceEuroPerSqmFromUnknown(displayPricePerSqm, { medianSuffix: false })}
                  </div>
                </div>
              ) : null}

              {/* 4) Explanation */}
              {!isLoadingNow && (basedOnExplainer || (frDetect === "unclear" && isNoResult)) ? (
                <div className="rounded-[10px] border border-white/10 bg-black/20 px-2.5 py-2">
                  <div className="text-xs font-medium leading-tight text-zinc-400/90">
                    {frDetect === "unclear" && isNoResult
                      ? "Try another address with confirmed France DVF coverage."
                      : basedOnExplainer}
                  </div>
                </div>
              ) : null}

              {/* 5) Area demand (wording only) */}
              <div className="rounded-[10px] border border-white/10 bg-black/20 px-2.5 py-2">
                <div className="text-[11px] font-medium uppercase tracking-[0.14em] leading-tight text-zinc-400/70">
                  Area demand
                </div>
                <div className="mt-1 text-[14px] font-semibold leading-tight text-white">{livabilityText}</div>
                {(() => {
                  const priceLevel = rd?.fr_area_price_level as string | null | undefined;
                  const trend = rd?.fr_area_trend as string | null | undefined;
                  const liquidity = rd?.fr_area_liquidity as string | null | undefined;
                  if (!priceLevel && !trend && !liquidity) return null;
                  const parts: string[] = [];
                  if (priceLevel === "premium") parts.push("Premium neighbourhood");
                  else if (priceLevel === "moderate") parts.push("Mid-range neighbourhood");
                  else if (priceLevel === "affordable") parts.push("More affordable neighbourhood");
                  if (trend === "up") parts.push("Prices trending up");
                  else if (trend === "down") parts.push("Prices trending down");
                  else if (trend === "stable") parts.push("Prices stable");
                  if (liquidity === "high") parts.push("Active market");
                  else if (liquidity === "low") parts.push("Fewer transactions");
                  if (parts.length === 0) return null;
                  return (
                    <div className="mt-1.5 text-[11px] font-medium leading-tight text-zinc-400/90">
                      {parts.join(" • ")}
                    </div>
                  );
                })()}
              </div>

              {showCheckAnotherApartmentButton && !isLoadingNow ? (
                <div className="mt-1 border-t border-white/10 pt-2">
                  <button
                    type="button"
                    onClick={resetToApartmentPrompt}
                    className="w-full rounded-lg border border-white/15 bg-white/5 py-2 text-center text-[12px] font-medium text-zinc-200 hover:border-amber-400/30 hover:bg-white/10"
                  >
                    Check another apartment
                  </button>
                </div>
              ) : null}

            </div>
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
                <div className="text-[11px] font-semibold tracking-wide text-amber-300">France · Official records</div>
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

          {lotPromptVisible && phase === "initial_building_state" && availableLots.length > 0 ? (
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

          {lotPromptVisible ? (
            <div className="mt-3 rounded-xl border border-zinc-500/20 bg-black/35 px-3 py-2.5">
              <div className="text-[10px] font-medium text-zinc-400">
                {!hasSubmittedLotSearch ? "What's your apartment number?" : "Apartment / lot number"}
              </div>
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
                  disabled={isLoading && !lotInput.trim()}
                  className="shrink-0 rounded-lg border border-amber-400/35 bg-amber-400/15 px-3.5 py-2 text-[13px] font-semibold text-amber-200 hover:bg-amber-400/20 disabled:opacity-50"
                >
                  {isLoading ? "Searching…" : "Search"}
                </button>
              </div>
            </div>
          ) : null}

        </div>

        {/* Scrollable body: results only (no intermediate popup; result card shows Searching/result directly) */}
        <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-4 py-3" />
      </div>
    </div>
  );
}

