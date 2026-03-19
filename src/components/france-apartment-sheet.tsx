"use client";

import * as React from "react";
import { Calendar, Database, Star, X } from "lucide-react";
import { createPortal } from "react-dom";
import { usePropertyValueInsights } from "@/hooks/use-property-value-insights";
import type { FrancePropertyResponse } from "@/lib/france-response-contract";

type FranceSheetProps = {
  address: string;
  position: { lat: number; lng: number };
  postcode?: string;
  typedAddressForFrance?: string;
  currencySymbol?: string;
  onClose: () => void;
};

function formatCurrency(value: number, symbol: string): string {
  if (!Number.isFinite(value)) return `${symbol}0`;
  return `${symbol}${Math.round(value).toLocaleString("en-US")}`;
}

export function FranceApartmentSheet({
  address,
  position,
  postcode,
  typedAddressForFrance,
  currencySymbol = "€",
  onClose,
}: FranceSheetProps) {
  const isDev = process.env.NODE_ENV !== "production";
  const addressForApi = typedAddressForFrance?.trim() ? typedAddressForFrance.trim() : address;

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
  } | null>(null);
  const [isExpanded, setIsExpanded] = React.useState(false);
  const [showOptionalAptInput, setShowOptionalAptInput] = React.useState(false);
  const dragStartYRef = React.useRef<number | null>(null);
  const dragToggledRef = React.useRef(false);

  const { data, isLoading, refetch } = usePropertyValueInsights(addressForApi, "FR", {
    latitude: position.lat,
    longitude: position.lng,
    aptNumber: requestedLot,
    postcode,
    refetchTrigger: trigger,
    countryCode: "FR",
  });

  React.useEffect(() => {
    if (!isDev) return;
    const d: any = data;
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
    setShowOptionalAptInput(false);
    setResolvedForDisplay(null);
    setIsExpanded(false);
  }, [addressKey]);

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
    const r = d as { data_source?: string; result_level?: string; property_result?: { value_level?: string } };
    return r.data_source === "properties_france" && r.result_level === "exact_property" && r.property_result?.value_level === "property-level";
  };

  const franceResultPriority = React.useCallback((d: unknown): number => {
    const rt = (d as any)?.fr?.resultType as string | undefined;
    switch (rt) {
      case "exact_apartment":
        return 5;
      case "similar_apartment_same_building":
        return 4;
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
  const isHouseInferredByHeuristic = hasNoLots && !hasAppartementType && lowMultiUnitHeuristic;

  const isHouseDetected =
    (parsed?.multiple_units === false && availableLots.length === 0) ||
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
  const isHouseLikeOverride = buildingSales.length <= 5 && availableLots.length < 20 && !hasMultiUnitEvidence;
  const isApartmentLikeForLotFirst = hasMultiUnitEvidence;
  const isHouseLikeUI = isHouseDetected || (isHouseLikeOverride && !isApartmentLikeForLotFirst);
  const shouldForceLotFirstFlow = isApartmentLikeForLotFirst && !isHouseLikeUI;
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

  const displayConfidence = React.useMemo(() => {
    const c = normalized?.confidence;
    if (!c) return null;
    const s = String(c).replace(/_/g, "-");
    // Capitalize
    return s
      .split("-")
      .map((p) => (p ? p[0].toUpperCase() + p.slice(1) : p))
      .join("-");
  }, [normalized?.confidence]);

  const sourceLabel = React.useMemo(() => {
    const rt = normalized?.resultType;
    if (rt === "exact_apartment") return isHouseLikeUI ? "Exact property match" : "Exact lot match";
    if (rt === "similar_apartment_same_building")
      return isHouseLikeUI ? "Similar house in this building" : "Similar apartment in this building";
    if (rt === "building_level" || rt === "building_fallback")
      return "Building-level aggregate";
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
    if (rt === "similar_apartment_same_building") return "Similar apartment transaction";
    if (rt === "building_level" || rt === "building_fallback") return "Building average";
    if (rt === "nearby_comparable" || rt === "comparables_only") return "Best available nearby comparable";
    return null;
  }, [normalized?.resultType]);

  const matchDetails = React.useMemo(() => {
    const requested = normalized?.requestedLot;
    const matched = normalized?.matchedLot;
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
    (parsed?.multiple_units === true);

  type Phase =
    | "initial_building_state"
    | "exact_apartment_match_state"
    | "searched_no_exact_match_but_building_exists_state"
    | "no_result_state";

  const phase: Phase = React.useMemo(() => {
    if (isExactApartment(effectiveData)) return "exact_apartment_match_state";
    if (!hasSubmittedLotSearch) {
      return hasUsefulBuildingData ? "initial_building_state" : "no_result_state";
    }
    return hasUsefulBuildingData ? "searched_no_exact_match_but_building_exists_state" : "no_result_state";
  }, [effectiveData, hasSubmittedLotSearch, hasUsefulBuildingData]);

  // Houses/single-unit properties should not require apartment/lot input.
  // As soon as we have enough data to infer "house", open the results directly from the address.
  React.useEffect(() => {
    if (!isHouseLikeUI) return;
    if (hasSubmittedLotSearch) return;
    if (isLoading) return;
    if (isDev) console.log("[FR_UI] direct_house_flow_chosen");
    setHasSubmittedLotSearch(true);
    setIsResultCardOpen(true);
  }, [isHouseLikeUI, hasSubmittedLotSearch, isLoading, isDev]);

  React.useEffect(() => {
    if (!isDev) return;
    if (shouldForceLotFirstFlow) {
      console.log("[FR_UI] lot_first_flow_triggered");
    }
  }, [isDev, shouldForceLotFirstFlow]);

  const badge = React.useMemo(() => {
    if (phase === "exact_apartment_match_state") {
      return isHouseLikeUI
        ? { label: "Exact house match", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" }
        : { label: "Exact apartment match", tone: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200" };
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
      if (isHouseLikeUI) return lot ? `Matched house / lot ${lot}` : "Matched house / lot";
      return lot ? `Matched apartment / lot ${lot}` : "Matched apartment / lot";
    }
    if (phase === "searched_no_exact_match_but_building_exists_state") {
      const lot = (requestedLot ?? "").trim();
      const rt = normalized?.resultType;
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
  }, [phase, requestedLot, lotInput, normalized?.resultType, isHouseLikeUI, isPropertyTypeUnknown]);

  const submit = React.useCallback((source: "enter" | "button") => {
    const lot = lotInput.trim();
    setRequestedLot(lot || undefined);
    // Hard guarantee: never show any previous result while a new lot search is in flight.
    // We only render a final result once the latest request resolves and matches the requested lot.
    setResolvedForDisplay(null);
    setTrigger((t) => t + 1);
    setHasSubmittedLotSearch(true);
    setIsResultCardOpen(true);
  }, [lotInput]);

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
    setResolvedForDisplay({
      fr,
      legacy: { averageBuildingValue, livabilityRating: legacyLivability },
    });
    if (isDev) {
      console.log("[FR_UI] final_response_type", {
        resultType: fr.resultType,
        success: fr.success,
        confidence: fr.confidence,
      });
    }
  }, [isResultCardOpen, hasSubmittedLotSearch, isLoading, normalized, requestedLot, averageBuildingValue, legacyLivability, isDev]);

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
    if (!hasSubmittedLotSearch) return null;

    const isLoadingNow = isLoading || resolvedForDisplay == null;
    const fr = resolvedForDisplay?.fr ?? null;
    const legacy = resolvedForDisplay?.legacy ?? null;

    const title = isLoadingNow ? "Searching DVF…" : (badge?.label ?? (fr?.success === false ? "No result" : "Result"));
    const explanation = isLoadingNow
      ? ((requestedLot ?? "").trim() ? `Searching lot ${(requestedLot ?? "").trim()} for this address` : "Searching building data for this address")
      : (subtitle || fr?.matchExplanation || null);

    const rawValue =
      fr?.property?.transactionValue ??
      (fr?.resultType === "building_level" ? (fr?.buildingStats?.avgTransactionValue ?? legacy?.averageBuildingValue ?? null) : null);
    const hasValue = typeof rawValue === "number" && rawValue > 0;
    const isNoResult =
      !isLoadingNow &&
      (fr?.resultType === "no_result" || fr?.resultType === "no_reliable_data" || fr?.success === false);

    const isSuspiciousFallback =
      !isLoadingNow &&
      (fr?.resultType === "similar_apartment_same_building" || fr?.resultType === "nearby_comparable") &&
      (fr?.debug?.suspiciousPolicy === "warning" || fr?.debug?.suspiciousPricePerSqm === true);

    const transactionCount =
      !isLoadingNow && typeof fr?.buildingStats?.transactionCount === "number" && fr.buildingStats.transactionCount > 0
        ? fr.buildingStats.transactionCount
        : null;

    const valueRange = (() => {
      if (isLoadingNow) return null;
      if (isNoResult) return null;
      if (isSuspiciousFallback) return null;
      // Only show a range when we have enough building comparables to support it.
      if (fr?.resultType !== "building_level" && fr?.resultType !== "similar_apartment_same_building") return null;
      const displayedSurface =
        typeof fr?.property?.surfaceArea === "number" && fr.property.surfaceArea > 0 ? fr.property.surfaceArea : null;
      const displayedPricePerSqm =
        typeof fr?.property?.pricePerSqm === "number" && fr.property.pricePerSqm > 0 ? fr.property.pricePerSqm : null;

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

    const lastTxValue =
      !isLoadingNow && !isNoResult && typeof fr?.property?.transactionValue === "number" && fr.property.transactionValue > 0
        ? fr.property.transactionValue
        : null;
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
      !isLoadingNow &&
      !isNoResult &&
      !isSuspiciousFallback &&
      typeof fr?.property?.pricePerSqm === "number" &&
      fr.property.pricePerSqm > 0
        ? fr.property.pricePerSqm
        : null;

    const mainValue = isLoadingNow
      ? "Searching…"
      : isNoResult
        ? "No reliable data available"
        : isSuspiciousFallback
          ? "No reliable price available"
          : hasValue
          ? formatCurrency(rawValue, currencySymbol)
          : (fr?.resultType === "building_level"
              ? "No reliable building value available"
              : "—");

    const formatDisplayDate = (raw: string | null | undefined): string => {
      if (!raw) return "—";
      // Expecting ISO-like YYYY-MM-DD; normalize via Date but keep YYYY-MM-DD order.
      const d = new Date(raw);
      if (Number.isNaN(d.getTime())) return raw;
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${y}-${m}-${day}`;
    };

    const dateText = isLoadingNow ? "—" : formatDisplayDate(fr?.property?.transactionDate ?? null);
    const sourceText = isLoadingNow
      ? "—"
      : (sourceLabel ||
          (typeof pr?.street_average_message === "string" && pr.street_average_message.trim() ? pr.street_average_message.trim() : null) ||
          (fr?.success === false ? "No reliable data found" : "Area fallback"));
    const confidenceText = isLoadingNow ? "—" : (displayConfidence ? displayConfidence : "—");
    const livabilityText = isLoadingNow ? "—" : (legacy?.livabilityRating ?? "—");
    // Reuse the exact gold token used by the Search button and active Explore icon.
    const goldTextClass = "text-amber-400";
    const confidenceTone =
      isLoadingNow
        ? "border-white/10 bg-white/5 text-zinc-200"
        : fr?.confidence === "high" || fr?.confidence === "medium_high"
          ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-200"
          : fr?.confidence === "medium" || fr?.confidence === "low_medium"
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
                        ? `${formatCurrency(valueRange.min, currencySymbol)} – ${formatCurrency(valueRange.max, currencySymbol)}`
                        : (mainValue.startsWith(currencySymbol) ? mainValue : mainValue))
                  )}
                </div>

                <div className="mt-1 flex items-center justify-start gap-2">
                  <div className="text-[11px] font-medium uppercase tracking-wider leading-tight text-zinc-400/70">
                    Estimated value
                  </div>
                  {!isLoadingNow ? (
                    <div className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${confidenceTone}`}>
                      Confidence: {confidenceText}
                    </div>
                  ) : null}
                </div>

                {!isLoadingNow ? (
                  <div className="mt-1 text-xs font-medium leading-tight text-zinc-300/75">
                    {fr?.resultType === "exact_apartment"
                      ? "Based on an exact transaction for this property"
                      : fr?.resultType === "similar_apartment_same_building"
                        ? isHouseLikeUI ? "Based on nearby property transactions" : "Based on similar apartments in the same building"
                        : fr?.resultType === "nearby_comparable"
                          ? isHouseLikeUI ? "Based on nearby comparable houses" : "Based on nearby comparable apartments"
                          : fr?.resultType === "building_level"
                            ? isHouseLikeUI ? "Based on recent house transactions in this building" : "Based on recent transactions in this building"
                            : fr?.resultType === "no_reliable_data"
                              ? "Not enough reliable data to estimate value"
                              : null}
                  </div>
                ) : null}
              </div>

              {/* 2) Last transaction */}
              <div className="rounded-[10px] border border-white/10 bg-black/20 px-2.5 py-2">
                <div className="text-[11px] font-medium uppercase tracking-[0.14em] leading-tight text-zinc-400/70">
                  Last transaction
                </div>
                <div className="mt-1 text-[14px] font-semibold leading-tight text-white">
                  {isLoadingNow
                    ? "—"
                    : (!isNoResult && fr?.property?.transactionValue && fr.property.transactionValue > 0 && fr?.property?.transactionDate
                    ? `${formatCurrency(fr.property.transactionValue, currencySymbol)} • ${formatDisplayDate(fr?.property?.transactionDate ?? null)}`
                    : "No recent transaction available")}
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
                        {fr?.property?.surfaceArea ? (
                          <div>Surface: <span className="font-medium text-white">{fr.property.surfaceArea} m²</span></div>
                        ) : null}
                        {!isSuspiciousFallback && fr?.property?.pricePerSqm ? (
                          <div>
                            Price/m²:{" "}
                            <span className="font-medium text-white">
                              {Math.round(fr.property.pricePerSqm).toLocaleString("en-US")} {currencySymbol}
                            </span>
                          </div>
                        ) : null}
                        {isSuspiciousFallback ? (
                          <div>Price/m²: <span className="font-medium text-white">Suppressed (suspicious)</span></div>
                        ) : null}
                        {fr?.property?.propertyType ? (
                          <div>Property type: <span className="font-medium text-white">{fr.property.propertyType}</span></div>
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

          {!isHouseLikeUI ? (
            <div className="mt-3 rounded-xl border border-zinc-500/20 bg-black/35 px-3 py-2.5">
              {isPropertyTypeUnknown && !showOptionalAptInput ? (
                <button
                  type="button"
                  onClick={() => setShowOptionalAptInput(true)}
                  className="w-full rounded-lg border border-white/10 bg-black/20 px-2 py-2 text-left text-[11px] font-medium text-zinc-300 hover:border-white/20"
                >
                  {isHouseLikeUI ? "Add property/lot only if relevant" : "Add apartment/lot only if relevant"}
                </button>
              ) : (
                <>
                  <div className="text-[10px] font-medium text-zinc-400">{isHouseLikeUI ? "Property / lot number" : "Apartment / lot number"}</div>
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
                </>
              )}
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
            {normalized?.success === false ? (
              <button
                type="button"
                onClick={() => refetch()}
                className="mt-2 inline-flex items-center justify-center rounded-lg border border-amber-400/30 bg-amber-400/10 px-3 py-2 text-[12px] font-semibold text-amber-200"
              >
                Retry
              </button>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}

