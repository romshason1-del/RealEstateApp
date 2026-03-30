"use client";

import * as React from "react";
import { X, FileText, Sparkles, Building2, BadgeCheck, Bug, ChevronDown, ChevronUp, Info } from "lucide-react";
import { HeartButton } from "@/components/heart-button";
import { calculatePropertyValue } from "@/lib/property-value";
import { usePropertyValueInsights } from "@/hooks/use-property-value-insights";
import { parseAddressFromFullString, parseUSAddressFromFullString, parseUKAddressFromFullString, parseFRAddressFromFullString } from "@/lib/address-parse";
import { toCanonicalAddress } from "@/lib/address-canonical";
import { toEnglishDisplay, sanitizeForDisplay } from "@/lib/display-utils";
import {
  coercePositiveNumber,
  formatFranceEuroPerSqm,
  formatFranceEuroTotal,
  sanitizeFrancePropertyResultForDisplay,
  type FrancePropertyResultLike,
} from "@/lib/fr-display-safe";
import {
  formatNycCardDisplayAddress,
  UsNycTruthPropertyCard,
  type UsNycTruthCardData,
} from "@/components/us/us-nyc-truth-property-card";

export type PropertyValueCardProps = {
  address: string;
  position: { lat: number; lng: number };
  currencySymbol?: string;
  countryCode?: string;
  /** NYC `/api/property-value` top-level `status` (e.g. requires_unit); usually derived from API JSON. */
  status?: string;
  onClose: () => void;
  isSaved?: boolean;
  onToggleSave?: () => void;
  /** UK only: raw typed input (preserves Flat/Unit) */
  rawInputAddress?: string;
  /** UK only: Google formatted_address from selected suggestion */
  selectedFormattedAddress?: string;
  /** France: exact typed address when user pressed Enter (bypasses Google formatting) */
  typedAddressForFrance?: string;
  /** France: postcode from Google address_components (avoids "Postcode required") */
  postcode?: string;
};

function formatSaleDate(dateStr: string | null): string {
  if (!dateStr) return "";
  try {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return dateStr;
    return d.toLocaleDateString("en-US", { month: "short", year: "numeric" });
  } catch {
    return dateStr;
  }
}

/** Format currency with proper thousands separators (e.g. $1,250,000) */
function formatCurrency(value: number, symbol: string): string {
  if (!Number.isFinite(value)) return `${symbol}0`;
  return `${symbol}${Math.round(value).toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
}

/** Universal first-card label and support text by value level */
function getFirstCardWording(valueLevel: string | undefined): { label: string; supportText: string } {
  const label = "Estimated value for this property";
  switch (valueLevel) {
    case "property-level":
      return { label, supportText: "Based on exact property transaction history" };
    case "building-level":
      return { label, supportText: "Based on building transaction history" };
    case "street-level":
      return { label, supportText: "Based on recent sales on this street" };
    case "area-level":
      return { label, supportText: "Based on area market data" };
    case "no_match":
      return { label, supportText: "Map location found; no property record" };
    default:
      return { label, supportText: "Based on available market data" };
  }
}

/** Heuristic: values in typical monthly rent range (500–25,000) are likely rent, not sale price */
function isLikelyRent(value: number, source?: string): boolean {
  if (!Number.isFinite(value) || value <= 0) return false;
  if (source === "rentcast") {
    return value >= 500 && value <= 25000;
  }
  return value >= 500 && value <= 25000;
}

function CollapsibleSection({
  title,
  children,
  defaultOpen = false,
  count,
}: {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
  count?: number;
}) {
  const [open, setOpen] = React.useState(defaultOpen);
  return (
    <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center justify-between px-2 py-0.5 sm:px-2 sm:py-1 text-left text-[8px] uppercase tracking-wider text-zinc-400/90 hover:bg-zinc-500/10 transition-colors"
      >
        <span>{title}{count != null && count > 0 ? ` (${count})` : ""}</span>
        {open ? <ChevronUp className="size-3 shrink-0" /> : <ChevronDown className="size-3 shrink-0" />}
      </button>
      {open && <div className="border-t border-zinc-500/20 px-2 py-1 sm:px-2 sm:py-1">{children}</div>}
    </div>
  );
}

type ParsedAddress = { city: string; street: string; houseNumber: string; state?: string; zip?: string; country?: string };

type DebugPanelProps = {
  address: string;
  parsed: ParsedAddress;
  canonical: { cityKey: string; streetKey: string; houseKey: string };
  insightsData: {
    debug?: {
      normalized_postcode?: string;
      postcode_query_executed?: string;
      postcode_query_url?: string;
      postcode_query_raw_result_count?: number;
      postcode_results_count?: number;
      exact_building_matches_count?: number;
      fuzzy_building_matches_count?: number;
      address_match_mode?: string;
      raw_input_address?: { city: string; street: string; house_number: string };
      canonical_address?: { city_key: string; street_key: string; house_key: string };
      records_fetched?: number;
      records_after_filter?: number;
      exact_matches_count?: number;
      nearby_matches_count?: number;
      street_matches_found?: string[];
      building_numbers_found?: string[];
      distance_from_requested_m?: number;
      rejection_reason?: string;
      dataset_sample?: Array<{ city: string; street: string; house_number: string; canonical: { city_key: string; street_key: string; house_key: string } }>;
      api_status?: number;
      api_error?: string;
      records_returned?: number;
      raw_dataset_sample?: Record<string, unknown>[];
      dataset_id?: string;
      resource_id_selected?: string;
      datastore_active?: boolean;
      active_provider_id?: string;
      provider_configured?: boolean;
      PROPERTY_PROVIDER_US?: string;
      RENTCAST_API_KEY_present?: boolean;
      request_attempted?: boolean;
      http_status?: number;
      reason?: string;
      response_summary?: string;
      property_found?: boolean;
      avm_value_found?: boolean;
      avm_rent_found?: boolean;
      sales_history_found?: boolean;
      comps_found?: boolean;
      market_value_source?: string;
      price_per_m2_used?: number;
      price_per_sqft_used?: number;
      unit_required?: boolean;
      fallback_level_used?: string;
      property_size_used?: number;
      transactions_count_5y?: number;
      latest_transaction_amount?: number;
    };
    message?: string;
  } | null;
  latest?: { transaction_date: string; transaction_price: number; property_size: number; price_per_m2: number } | null;
  currencySymbol: string;
};

function DebugPanel({ address, parsed, canonical, insightsData, latest, currencySymbol }: DebugPanelProps) {
  const d = insightsData?.debug;
  const franceDataSource =
    insightsData != null &&
    typeof insightsData === "object" &&
    (insightsData as { data_source?: string }).data_source === "properties_france";
  const apiCanon = d?.canonical_address ?? {
    city_key: canonical.cityKey,
    street_key: canonical.streetKey,
    house_key: canonical.houseKey,
  };
  return (
    <div className="space-y-2 text-[11px] sm:text-xs">
      <div className="flex items-center gap-2 text-amber-300/90">
        <Bug className="size-4 shrink-0" aria-hidden />
        <span className="font-medium uppercase tracking-wider">Debug Mode</span>
      </div>
      <div className="space-y-1.5 font-mono text-xs text-zinc-300">
        <div><span className="text-zinc-500">Input address:</span> {toEnglishDisplay(address)}</div>
        <div><span className="text-zinc-500">Parsed city:</span> {toEnglishDisplay(parsed.city)}</div>
        <div><span className="text-zinc-500">Parsed street:</span> {toEnglishDisplay(parsed.street)}</div>
        <div><span className="text-zinc-500">Parsed house number:</span> {toEnglishDisplay(parsed.houseNumber)}</div>
        {parsed.state != null && parsed.state !== "" && (
          <div><span className="text-zinc-500">Parsed state:</span> {parsed.state}</div>
        )}
        {parsed.zip != null && parsed.zip !== "" && (
          <div><span className="text-zinc-500">Parsed zip:</span> {parsed.zip}</div>
        )}
        {parsed.country != null && parsed.country !== "" && (
          <div><span className="text-zinc-500">Parsed country:</span> {parsed.country}</div>
        )}
        <div><span className="text-zinc-500">Canonical city_key:</span> {apiCanon.city_key}</div>
        <div><span className="text-zinc-500">Canonical street_key:</span> {apiCanon.street_key}</div>
        <div><span className="text-zinc-500">Canonical house_key:</span> {apiCanon.house_key}</div>
        {d?.active_provider_id != null && (
          <div><span className="text-zinc-500">Active provider ID:</span> {String(d.active_provider_id)}</div>
        )}
        {d?.provider_configured != null && (
          <div><span className="text-zinc-500">Provider configured:</span> {d.provider_configured ? "true" : "false"}</div>
        )}
        {d?.PROPERTY_PROVIDER_US != null && (
          <div><span className="text-zinc-500">PROPERTY_PROVIDER_US:</span> {String(d.PROPERTY_PROVIDER_US)}</div>
        )}
        {d?.RENTCAST_API_KEY_present != null && (
          <div><span className="text-zinc-500">RENTCAST_API_KEY present:</span> {d.RENTCAST_API_KEY_present ? "true" : "false"}</div>
        )}
        {d?.request_attempted != null && (
          <div><span className="text-zinc-500">Request attempted:</span> {d.request_attempted ? "true" : "false"}</div>
        )}
        {d?.http_status != null && (
          <div><span className="text-zinc-500">HTTP status:</span> {d.http_status}</div>
        )}
        {d?.api_error && (
          <div><span className="text-zinc-500">API error:</span> <span className="text-amber-300/90">{String(d.api_error)}</span></div>
        )}
        {d?.reason && (
          <div><span className="text-zinc-500">Reason:</span> <span className="text-amber-300/90">{String(d.reason)}</span></div>
        )}
        {d?.response_summary && (
          <div><span className="text-zinc-500">Response summary:</span> {String(d.response_summary)}</div>
        )}
        {d?.property_found != null && (
          <div><span className="text-zinc-500">Property found:</span> {d.property_found ? "true" : "false"}</div>
        )}
        {d?.avm_value_found != null && (
          <div><span className="text-zinc-500">AVM value found:</span> {d.avm_value_found ? "true" : "false"}</div>
        )}
        {d?.avm_rent_found != null && (
          <div><span className="text-zinc-500">AVM rent found:</span> {d.avm_rent_found ? "true" : "false"}</div>
        )}
        {d?.sales_history_found != null && (
          <div><span className="text-zinc-500">Sales history found:</span> {d.sales_history_found ? "true" : "false"}</div>
        )}
        {d?.comps_found != null && (
          <div><span className="text-zinc-500">Comps found:</span> {d.comps_found ? "true" : "false"}</div>
        )}
        {d?.market_value_source != null && (
          <div><span className="text-zinc-500">Market value source:</span> {String(d.market_value_source)}</div>
        )}
        {d?.price_per_m2_used != null && (
          <div>
            <span className="text-zinc-500">Price per m² used:</span>{" "}
            {franceDataSource ? formatFranceEuroPerSqm(d.price_per_m2_used) : d.price_per_m2_used}
          </div>
        )}
        {d?.price_per_sqft_used != null && (
          <div><span className="text-zinc-500">Price per sqft used:</span> {d.price_per_sqft_used}</div>
        )}
        {d?.unit_required != null && (
          <div><span className="text-zinc-500">Unit required:</span> {d.unit_required ? "true" : "false"}</div>
        )}
        {d?.fallback_level_used != null && (
          <div><span className="text-zinc-500">Fallback level used:</span> {String(d.fallback_level_used)}</div>
        )}
        {d?.property_size_used != null && (
          <div><span className="text-zinc-500">Property size used:</span> {d.property_size_used} m²</div>
        )}
        {d?.transactions_count_5y != null && (
          <div><span className="text-zinc-500">Transactions count 5y:</span> {d.transactions_count_5y}</div>
        )}
        {d?.latest_transaction_amount != null && (
          <div>
            <span className="text-zinc-500">Latest transaction amount:</span>{" "}
            {franceDataSource ? formatFranceEuroTotal(d.latest_transaction_amount) : d.latest_transaction_amount}
          </div>
        )}
        <div><span className="text-zinc-500">Records fetched:</span> {d?.records_fetched ?? "—"}</div>
        <div><span className="text-zinc-500">Records returned:</span> {d?.records_returned ?? "—"}</div>
        <div><span className="text-zinc-500">Candidate records:</span> {d?.records_after_filter ?? "—"}</div>
        {d?.api_status != null && (
          <div><span className="text-zinc-500">API status:</span> {d.api_status}</div>
        )}
        {d?.dataset_id && (
          <div><span className="text-zinc-500">Dataset ID:</span> {d.dataset_id}</div>
        )}
        {d?.resource_id_selected && (
          <div><span className="text-zinc-500">Resource ID:</span> {d.resource_id_selected}</div>
        )}
        {d?.datastore_active != null && (
          <div><span className="text-zinc-500">Datastore active:</span> {d.datastore_active ? "yes" : "no"}</div>
        )}
        <div><span className="text-zinc-500">Exact matches:</span> {d?.exact_matches_count ?? "—"}</div>
        <div><span className="text-zinc-500">Nearby matches:</span> {d?.nearby_matches_count ?? "—"}</div>
        {d?.street_matches_found && d.street_matches_found.length > 0 && (
          <div><span className="text-zinc-500">Street matches:</span> {d.street_matches_found.join(", ")}</div>
        )}
        {d?.building_numbers_found && d.building_numbers_found.length > 0 && (
          <div><span className="text-zinc-500">Building numbers:</span> {d.building_numbers_found.join(", ")}</div>
        )}
        {d?.distance_from_requested_m != null && (
          <div><span className="text-zinc-500">Distance from requested:</span> {d.distance_from_requested_m} m</div>
        )}
        {d?.rejection_reason && (
          <div><span className="text-zinc-500">Rejection reason:</span> <span className="text-amber-300/90">{d.rejection_reason}</span></div>
        )}
        {d?.normalized_postcode != null && (
          <div><span className="text-zinc-500">Normalized postcode:</span> {String(d.normalized_postcode)}</div>
        )}
        {d?.postcode_query_executed != null && (
          <div><span className="text-zinc-500">Postcode query mode:</span> {String(d.postcode_query_executed)}</div>
        )}
        {d?.postcode_query_url != null && (
          <div><span className="text-zinc-500">Postcode query URL:</span> {String(d.postcode_query_url)}</div>
        )}
        {d?.postcode_query_raw_result_count != null && (
          <div><span className="text-zinc-500">Postcode raw result count:</span> {d.postcode_query_raw_result_count}</div>
        )}
        {d?.postcode_results_count != null && (
          <div><span className="text-zinc-500">Postcode results:</span> {d.postcode_results_count}</div>
        )}
        {d?.exact_building_matches_count != null && (
          <div><span className="text-zinc-500">Exact building matches:</span> {d.exact_building_matches_count}</div>
        )}
        {d?.fuzzy_building_matches_count != null && (
          <div><span className="text-zinc-500">Fuzzy building matches:</span> {d.fuzzy_building_matches_count}</div>
        )}
        {d?.address_match_mode != null && (
          <div><span className="text-zinc-500">Address match mode:</span> {String(d.address_match_mode)}</div>
        )}
      </div>
      {latest && (
        <div className="rounded border border-emerald-500/30 bg-emerald-500/5 p-1.5 sm:p-2">
          <div className="text-[10px] uppercase tracking-wider text-emerald-400/90">Matched transaction</div>
          <div className="mt-1 space-y-0.5 text-zinc-300">
            <div>Date: {formatSaleDate(latest.transaction_date)}</div>
            <div>
              Price:{" "}
              {franceDataSource ? formatFranceEuroTotal(latest.transaction_price) : formatCurrency(latest.transaction_price, currencySymbol)}
            </div>
            <div>Size: {latest.property_size} m²</div>
            <div>
              Price/m²:{" "}
              {franceDataSource ? formatFranceEuroPerSqm(latest.price_per_m2) : formatCurrency(latest.price_per_m2, currencySymbol)}
            </div>
          </div>
        </div>
      )}
      {d?.raw_dataset_sample && d.raw_dataset_sample.length > 0 && (
        <details className="text-zinc-400">
          <summary>Raw dataset sample (first 5)</summary>
          <pre className="mt-1 max-h-40 overflow-auto rounded bg-black/30 p-2 font-mono text-[10px]">
            {JSON.stringify(d.raw_dataset_sample, null, 2)}
          </pre>
        </details>
      )}
      {d?.dataset_sample && d.dataset_sample.length > 0 && (
        <details className="text-zinc-400">
          <summary>Dataset sample (parsed)</summary>
          <div className="mt-1 space-y-0.5 font-mono text-[10px]">
            {d.dataset_sample.map((s, i) => (
              <div key={i}>
                city: {toEnglishDisplay(s.city)} | street: {toEnglishDisplay(s.street)} | house: {toEnglishDisplay(s.house_number)} | canonical: {s.canonical.city_key}/{s.canonical.street_key}/{s.canonical.house_key}
              </div>
            ))}
          </div>
        </details>
      )}
    </div>
  );
}

export function PropertyValueCard(props: PropertyValueCardProps) {
  console.log("[CARD_PROPS_FULL]", JSON.stringify(props).substring(0, 500));
  const {
    address,
    position,
    currencySymbol = "$",
    countryCode = "",
    status: statusProp,
    onClose,
    isSaved = false,
    onToggleSave,
    rawInputAddress,
    selectedFormattedAddress,
    typedAddressForFrance,
    postcode,
  } = props;
  const isDev = process.env.NODE_ENV !== "production";
  const isIsrael = countryCode === "IL";
  const isUS = countryCode === "US";
  const isUK = countryCode === "UK" || countryCode === "GB";
  const isIT = countryCode === "IT";
  const isFR = countryCode === "FR";
  const hasOfficialProvider = isIsrael || isUS || isUK || isIT || isFR;
  const mockData = React.useMemo(
    () => calculatePropertyValue(position.lat, position.lng, currencySymbol),
    [position.lat, position.lng, currencySymbol]
  );
  const addressForApi = isFR && typedAddressForFrance?.trim() ? typedAddressForFrance.trim() : address;
  const [aptNumber, setAptNumber] = React.useState("");
  const [searchApt, setSearchApt] = React.useState<string | undefined>(undefined);
  const [aptSearchTrigger, setAptSearchTrigger] = React.useState(0);
  /** NYC only: apartment/lot from API prompt — local until backend unit lookup exists. */
  const [nycUnitDraft, setNycUnitDraft] = React.useState("");
  const [nycUnitSubmitted, setNycUnitSubmitted] = React.useState<string | null>(null);
  /** NYC: full `/api/property-value` JSON after Apply with `unit_or_lot` (see also `unitOrLot` on the insights hook). */
  const [nycUnitApplyPayload, setNycUnitApplyPayload] = React.useState<Record<string, unknown> | null>(null);
  const [nycUnitApplyLoading, setNycUnitApplyLoading] = React.useState(false);
  const [nycUnitApplyError, setNycUnitApplyError] = React.useState<string | null>(null);
  const { data: insightsData, isLoading, refetch } = usePropertyValueInsights(addressForApi, countryCode, {
    latitude: position?.lat,
    longitude: position?.lng,
    countryCode,
    rawInputAddress,
    selectedFormattedAddress,
    aptNumber: searchApt,
    refetchTrigger: aptSearchTrigger,
    postcode: isFR ? postcode : undefined,
    unitOrLot: isUS && nycUnitSubmitted?.trim() ? nycUnitSubmitted.trim() : undefined,
  });

  React.useEffect(() => {
    if (!isUS || insightsData == null || typeof insightsData !== "object") return;
    window.dispatchEvent(new CustomEvent("streetiq-us-property-value-raw", { detail: insightsData }));
  }, [isUS, insightsData]);

  // Prevent France UI from flickering to an empty/no-data state while switching lots.
  // Keep rendering the last known "building/area" payload during loading.
  const lastGoodFrancePayloadRef = React.useRef<unknown>(null);
  const lastGoodFranceBuildingPayloadRef = React.useRef<unknown>(null);
  const franceAddressKey = React.useMemo(() => {
    if (!isFR) return "";
    const a = (addressForApi || "").trim().toLowerCase();
    const pc = (postcode || "").trim().toLowerCase();
    return `${a}|pc:${pc}`;
  }, [isFR, addressForApi, postcode]);

  React.useEffect(() => {
    // Reset sticky France payload when the address changes (not when the lot changes).
    lastGoodFrancePayloadRef.current = null;
    lastGoodFranceBuildingPayloadRef.current = null;
  }, [franceAddressKey]);

  const isGoodFrancePayload = React.useCallback((d: unknown): boolean => {
    if (!d || typeof d !== "object") return false;
    const r = d as {
      data_source?: string;
      multiple_units?: boolean;
      result_level?: "exact_property" | "building" | "commune_fallback";
      building_sales?: unknown[];
      average_building_value?: number;
      property_result?: { value_level?: string; exact_value?: number | null; street_average?: number | null };
    };
    const isFRData = r.data_source === "properties_france";
    if (!isFRData) return false;
    if (r.multiple_units === true) return true;
    if (r.result_level === "building" || r.result_level === "commune_fallback") return true;
    if (Array.isArray(r.building_sales) && r.building_sales.length > 0) return true;
    if ((r.average_building_value ?? 0) > 0) return true;
    const pr = r.property_result;
    if (!pr) return false;
    if (pr.value_level === "building-level" || pr.value_level === "area-level") {
      if ((pr.street_average ?? 0) > 0) return true;
      if ((pr.exact_value ?? 0) > 0) return true;
    }
    return false;
  }, []);

  const isGoodFranceBuildingPayload = React.useCallback((d: unknown): boolean => {
    if (!d || typeof d !== "object") return false;
    const r = d as {
      data_source?: string;
      multiple_units?: boolean;
      result_level?: "exact_property" | "building" | "commune_fallback";
      building_sales?: unknown[];
      available_lots?: unknown[];
      average_building_value?: number;
      property_result?: { value_level?: string; street_average?: number | null };
    };
    if (r.data_source !== "properties_france") return false;
    if (r.multiple_units === true) return true;
    if (r.result_level === "building") return true;
    if (Array.isArray(r.available_lots) && r.available_lots.length > 0) return true;
    if (Array.isArray(r.building_sales) && r.building_sales.length > 0) return true;
    if ((r.average_building_value ?? 0) > 0) return true;
    if (r.property_result?.value_level === "building-level" && ((r.property_result.street_average ?? 0) > 0)) return true;
    return false;
  }, []);

  React.useEffect(() => {
    if (isFR && !isLoading && isGoodFrancePayload(insightsData)) {
      lastGoodFrancePayloadRef.current = insightsData;
    }
    if (isFR && !isLoading && isGoodFranceBuildingPayload(insightsData)) {
      lastGoodFranceBuildingPayloadRef.current = insightsData;
    }
  }, [isFR, isLoading, insightsData, isGoodFrancePayload, isGoodFranceBuildingPayload]);

  const hasStickyFranceBuilding = isFR && !!lastGoodFranceBuildingPayloadRef.current;

  const isExactApartmentPayload = React.useCallback((d: unknown): boolean => {
    if (!d || typeof d !== "object") return false;
    const r = d as {
      data_source?: string;
      result_level?: string;
      fr?: { resultType?: string };
      property_result?: { value_level?: string };
    };
    if (r.data_source !== "properties_france") return false;
    const rt = r.fr?.resultType;
    if (rt === "exact_apartment" || rt === "exact_address" || rt === "exact_house") return true;
    return r.result_level === "exact_property" && r.property_result?.value_level === "property-level";
  }, []);

  const activeInsightsData =
    // If an exact apartment payload arrived, render it immediately (never override with sticky building data).
    isExactApartmentPayload(insightsData)
      ? insightsData
      : isFR && hasStickyFranceBuilding && (isLoading || !isGoodFranceBuildingPayload(insightsData))
      ? (lastGoodFranceBuildingPayloadRef.current as typeof insightsData)
      : isFR && lastGoodFrancePayloadRef.current && (isLoading || !isGoodFrancePayload(insightsData))
        ? (lastGoodFrancePayloadRef.current as typeof insightsData)
        : insightsData;

  /** NYC: main API `should_prompt_for_unit` / `nyc_pending_unit_prompt` only — no client heuristics. */
  const usNycApartmentFlowEnabled = React.useMemo(() => {
    if (!isUS || !activeInsightsData || typeof activeInsightsData !== "object") return false;
    const d = activeInsightsData as {
      data_source?: string;
      success?: boolean;
      should_prompt_for_unit?: boolean;
      nyc_pending_unit_prompt?: boolean | null;
    };
    if (d.data_source !== "us_nyc_truth" || d.success !== true) return false;
    return d.should_prompt_for_unit === true || d.nyc_pending_unit_prompt === true;
  }, [isUS, activeInsightsData]);

  React.useEffect(() => {
    if (!isUS) return;
    setNycUnitDraft("");
    setNycUnitSubmitted(null);
    setNycUnitApplyPayload(null);
    setNycUnitApplyError(null);
  }, [address, isUS]);

  const submitNycUnitApply = React.useCallback(async () => {
    const t = nycUnitDraft.trim();
    if (!t || !isUS) return;
    setNycUnitApplyLoading(true);
    setNycUnitApplyError(null);
    try {
      console.log("[UNIT_FETCH_TRIGGERED] address:", addressForApi.trim(), "unit:", t);
      const params = new URLSearchParams();
      params.set("address", addressForApi.trim());
      params.set("countryCode", "US");
      if (position?.lat != null && Number.isFinite(position.lat)) params.set("latitude", String(position.lat));
      if (position?.lng != null && Number.isFinite(position.lng)) params.set("longitude", String(position.lng));
      params.set("unit_or_lot", t);
      const res = await fetch(`/api/property-value?${params.toString()}`, { signal: AbortSignal.timeout(20000) });
      const data = (await res.json().catch(() => ({}))) as Record<string, unknown>;
      if (!res.ok) {
        setNycUnitApplyError(typeof data.message === "string" ? data.message : "Request failed");
        return;
      }
      setNycUnitApplyPayload(data);
      const norm =
        typeof data.unit_or_lot_submitted === "string" && data.unit_or_lot_submitted.trim() !== ""
          ? data.unit_or_lot_submitted.trim()
          : t;
      setNycUnitSubmitted(norm);
    } catch (e) {
      setNycUnitApplyError(e instanceof Error ? e.message : "Request failed");
    } finally {
      setNycUnitApplyLoading(false);
    }
  }, [addressForApi, isUS, nycUnitDraft, position?.lat, position?.lng]);

  const nycTruthDisplay =
    isUS &&
    activeInsightsData &&
    typeof activeInsightsData === "object" &&
    ((activeInsightsData as { data_source?: string }).data_source === "us_nyc_truth" ||
      (activeInsightsData as { status?: string }).status === "requires_unit" ||
      (activeInsightsData as { status?: string }).status === "commercial_property")
      ? (nycUnitApplyPayload ?? (activeInsightsData as Record<string, unknown>))
      : null;

  /** Prefer Apply response status so UI leaves requires_unit after unit submit. */
  const usNycMergedStatus = React.useMemo(() => {
    if (!isUS) return undefined;
    if (nycUnitApplyPayload && typeof nycUnitApplyPayload === "object") {
      const st = (nycUnitApplyPayload as { status?: string }).status;
      if (typeof st === "string") return st;
    }
    if (activeInsightsData && typeof activeInsightsData === "object") {
      return (activeInsightsData as { status?: string }).status;
    }
    return undefined;
  }, [isUS, activeInsightsData, nycUnitApplyPayload]);

  const usNycHasEstimatedValue =
    isUS &&
    activeInsightsData &&
    typeof activeInsightsData === "object" &&
    (activeInsightsData as { estimated_value?: number | null }).estimated_value != null;

  /** Align with US route: null/empty status + estimated_value ⇒ treat as success for UI. */
  const usNycDisplayStatusResolved = React.useMemo(() => {
    if (!isUS) return undefined;
    if (usNycMergedStatus === "success") return "success";
    if (
      usNycHasEstimatedValue &&
      (usNycMergedStatus == null || usNycMergedStatus === "")
    ) {
      return "success";
    }
    return usNycMergedStatus;
  }, [isUS, usNycMergedStatus, usNycHasEstimatedValue]);

  const isMobileViewport = React.useMemo(() => {
    if (typeof window === "undefined") return false;
    return window.matchMedia?.("(max-width: 640px)")?.matches ?? window.innerWidth <= 640;
  }, []);

  // Mobile keyboard support: keep the card above the keyboard while the lot input is focused.
  const [isLotInputFocused, setIsLotInputFocused] = React.useState(false);
  const [keyboardInsetPx, setKeyboardInsetPx] = React.useState(0);
  const lotSubmitInFlightRef = React.useRef(false);
  const lastSubmittedLotRef = React.useRef<string>("");

  const submitLotSearch = React.useCallback((source: "enter" | "button") => {
    const requested = (aptNumber ?? "").trim();
    if (isDev) console.log("[France lot] submit triggered", { source, requestedLot: requested || "(empty)" });
    if (lotSubmitInFlightRef.current) {
      if (isDev) console.log("[France lot] submit skipped (in flight)");
      return;
    }
    // Avoid rapid double-submit of the same value.
    if (requested && requested === lastSubmittedLotRef.current && isLoading) {
      if (isDev) console.log("[France lot] submit skipped (same lot still loading)");
      return;
    }
    lotSubmitInFlightRef.current = true;
    lastSubmittedLotRef.current = requested;
    if (isDev) console.log("[France lot] request started", { requestedLot: requested || "(empty)" });
    setSearchApt(requested || undefined);
    setAptSearchTrigger((t) => t + 1);
    // Release the in-flight guard shortly after state updates; the hook has its own request id guard.
    window.setTimeout(() => {
      lotSubmitInFlightRef.current = false;
    }, 300);
  }, [aptNumber, isLoading, isDev]);
  React.useEffect(() => {
    if (!isFR) return;
    if (!isLotInputFocused) {
      setKeyboardInsetPx(0);
      return;
    }
    const vv = window.visualViewport;
    const update = () => {
      if (vv) {
        const inset = Math.max(0, Math.round(window.innerHeight - vv.height - vv.offsetTop));
        setKeyboardInsetPx(inset);
      } else {
        // Fallback for browsers where visualViewport is unavailable/unreliable.
        // We keep a conservative inset so the card lifts above the keyboard area.
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
  }, [isFR, isLotInputFocused]);

  const [debugMode, setDebugMode] = React.useState(false);
  const multipleUnits = activeInsightsData && "multiple_units" in activeInsightsData && (activeInsightsData as { multiple_units?: boolean }).multiple_units === true;
  const promptForApartment = activeInsightsData && "prompt_for_apartment" in activeInsightsData && (activeInsightsData as { prompt_for_apartment?: boolean }).prompt_for_apartment === true;
  const apartmentNotMatched = activeInsightsData && "apartment_not_matched" in activeInsightsData && (activeInsightsData as { apartment_not_matched?: boolean }).apartment_not_matched === true;
  const availableLots = activeInsightsData && "available_lots" in activeInsightsData ? (activeInsightsData as { available_lots?: string[] }).available_lots : undefined;
  const averageBuildingValue = activeInsightsData && "average_building_value" in activeInsightsData ? (activeInsightsData as { average_building_value?: number }).average_building_value : undefined;
  const hasPropertyData = activeInsightsData?.address != null;
  const isUsRequiresUnit =
    isUS &&
    usNycMergedStatus === "requires_unit" &&
    usNycDisplayStatusResolved !== "success";
  const isUsCommercial = isUS && usNycMergedStatus === "commercial_property";
  const resolvedStatus =
    statusProp ??
    (isUS ? usNycDisplayStatusResolved : undefined) ??
    (activeInsightsData && typeof activeInsightsData === "object"
      ? (activeInsightsData as { status?: string }).status
      : undefined);
  const hasUSData =
    isUS &&
    (activeInsightsData?.avm_value != null ||
      activeInsightsData?.avm_rent != null ||
      (activeInsightsData?.last_sale != null && activeInsightsData.last_sale.price > 0) ||
      (activeInsightsData?.sales_history != null && activeInsightsData.sales_history.length > 0) ||
      (activeInsightsData &&
        typeof activeInsightsData === "object" &&
        (activeInsightsData as { data_source?: string }).data_source === "us_nyc_truth" &&
        (activeInsightsData as { success?: boolean }).success === true));
  const unitRequired = isUS && (activeInsightsData?.error === "UNIT_REQUIRED" || activeInsightsData?.debug?.unit_required === true);
  const noDataAvailable =
    isUS &&
    activeInsightsData?.message === "No Data Available" &&
    !hasPropertyData &&
    !unitRequired &&
    !isUsRequiresUnit &&
    !isUsCommercial;
  const hasMatch = hasPropertyData && (activeInsightsData?.match_quality === "exact_building" || activeInsightsData?.match_quality === "nearby_building");
  const isNearbyBuilding = activeInsightsData?.match_quality === "nearby_building";
  const latest = activeInsightsData?.latest_transaction;
  const estimate = activeInsightsData?.current_estimated_value;
  const building = activeInsightsData?.building_summary_last_3_years;
  const transactions5y = building?.transactions_count_last_5_years ?? building?.transactions_count_last_3_years ?? 0;
  const latestBuildingAmount = building?.latest_building_transaction_price ?? latest?.transaction_price ?? 0;

  const avmValue = activeInsightsData && "avm_value" in activeInsightsData ? (activeInsightsData as { avm_value?: number }).avm_value : undefined;
  const avmRent = activeInsightsData && "avm_rent" in activeInsightsData ? (activeInsightsData as { avm_rent?: number }).avm_rent : undefined;
  const lastSaleFromProvider = activeInsightsData && "last_sale" in activeInsightsData ? (activeInsightsData as { last_sale?: { price: number; date: string } }).last_sale : undefined;
  const salesHistory = activeInsightsData && "sales_history" in activeInsightsData ? (activeInsightsData as { sales_history?: Array<{ date: string; price: number }> }).sales_history : undefined;
  const lastSale = React.useMemo(() => {
    if (salesHistory != null && salesHistory.length > 0) {
      const sorted = [...salesHistory].sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
      return { price: sorted[0].price, date: sorted[0].date };
    }
    return lastSaleFromProvider;
  }, [salesHistory, lastSaleFromProvider]);
  const nearbyComps = activeInsightsData && "nearby_comps" in activeInsightsData ? (activeInsightsData as { nearby_comps?: { avg_price: number; avg_price_per_sqft: number; count: number } }).nearby_comps : undefined;
  const propertyDetails = activeInsightsData && "property_details" in activeInsightsData ? (activeInsightsData as { property_details?: { beds?: number; baths?: number; sqft?: number; year_built?: number; property_type?: string } }).property_details : undefined;
  const neighborhoodStats = activeInsightsData && "neighborhood_stats" in activeInsightsData ? (activeInsightsData as { neighborhood_stats?: { median_home_value: number; median_household_income: number; population: number; median_rent?: number; population_growth_percent?: number; income_growth_percent?: number } }).neighborhood_stats : undefined;
  const investmentMetrics = activeInsightsData && "investment_metrics" in activeInsightsData ? (activeInsightsData as { investment_metrics?: { median_rent: number; gross_rent_yield_percent?: number; estimated_roi_percent?: number; median_price_per_sqft?: number } }).investment_metrics : undefined;
  const marketTrend = activeInsightsData && "market_trend" in activeInsightsData ? (activeInsightsData as { market_trend?: { hpi_index: number; change_1y_percent: number } }).market_trend : undefined;
  const dataSource = activeInsightsData && "data_source" in activeInsightsData ? (activeInsightsData as { data_source?: "live" | "cache" | "mock" | "properties_france" }).data_source : undefined;
  const surfaceReelleBati = activeInsightsData && "surface_reelle_bati" in activeInsightsData ? (activeInsightsData as { surface_reelle_bati?: number | null }).surface_reelle_bati : undefined;
  const lotNumber = activeInsightsData && "lot_number" in activeInsightsData ? (activeInsightsData as { lot_number?: string | null }).lot_number : undefined;
  const buildingSales = activeInsightsData && "building_sales" in activeInsightsData ? (activeInsightsData as { building_sales?: Array<{ date: string | null; type: string; price: number; surface: number | null; lot_number?: string | null }> }).building_sales : undefined;
  const resultLevel = activeInsightsData && "result_level" in activeInsightsData ? (activeInsightsData as { result_level?: "exact_property" | "building" | "commune_fallback" }).result_level : undefined;
  const isFranceData = dataSource === "properties_france" || isFR;
  const propertyResult = activeInsightsData && "property_result" in activeInsightsData ? (activeInsightsData as {
    property_result?: {
      exact_value: number | null;
      exact_value_message: string | null;
      value_level: "property-level" | "building-level" | "street-level" | "area-level" | "no_match";
      last_transaction: { amount: number; date: string | null; message?: string };
      street_average: number | null;
      street_average_message: string | null;
      livability_rating: "POOR" | "FAIR" | "GOOD" | "VERY GOOD" | "EXCELLENT";
    };
  }).property_result : undefined;

  const frPropertyResultForDisplay = React.useMemo(() => {
    if (!propertyResult || !isFranceData) return propertyResult;
    return sanitizeFrancePropertyResultForDisplay(propertyResult as FrancePropertyResultLike);
  }, [propertyResult, isFranceData]);

  const surfaceReelleBatiFrance = React.useMemo(() => {
    if (!isFranceData) return surfaceReelleBati;
    const c = coercePositiveNumber(surfaceReelleBati as unknown);
    if (c != null) return c;
    return typeof surfaceReelleBati === "number" && surfaceReelleBati > 0 ? surfaceReelleBati : undefined;
  }, [isFranceData, surfaceReelleBati]);

  const averageBuildingValueFrance = React.useMemo(() => {
    if (!isFranceData) return averageBuildingValue;
    const c = coercePositiveNumber(averageBuildingValue as unknown);
    if (c != null) return c;
    return typeof averageBuildingValue === "number" && averageBuildingValue > 0 ? averageBuildingValue : undefined;
  }, [isFranceData, averageBuildingValue]);

  React.useEffect(() => {
    if (!isFR) return;
    if (!activeInsightsData || typeof activeInsightsData !== "object") return;
    const d = activeInsightsData as { data_source?: string; result_level?: string; property_result?: { value_level?: string } };
    if (d.data_source === "properties_france") {
      console.log("[France lot] response received", {
        result_level: d.result_level,
        value_level: d.property_result?.value_level,
        renderedBranch: d.result_level === "exact_property" ? "exact_apartment" : (d.result_level === "building" ? "building" : "other"),
      });
    }
  }, [isFR, activeInsightsData]);
  const hasFranceBuildingOrAreaData =
    isFranceData &&
    (
      multipleUnits ||
      resultLevel === "building" ||
      resultLevel === "commune_fallback" ||
      (
        propertyResult &&
        (
          propertyResult.value_level === "building-level" ||
          propertyResult.value_level === "area-level"
        ) &&
        (
          (propertyResult.street_average ?? 0) > 0 ||
          (propertyResult.exact_value ?? 0) > 0 ||
          (averageBuildingValue ?? 0) > 0 ||
          (Array.isArray(buildingSales) && buildingSales.length > 0)
        )
      )
    );
  const dataSources = insightsData && "data_sources" in insightsData ? (insightsData as { data_sources?: ("RentCast" | "Zillow" | "Redfin")[] }).data_sources : undefined;
  const usMatchConfidence = insightsData && "us_match_confidence" in insightsData ? (insightsData as { us_match_confidence?: "high" | "medium" | "low" }).us_match_confidence : undefined;
  const isAreaLevelEstimate = insightsData && "is_area_level_estimate" in insightsData ? (insightsData as { is_area_level_estimate?: boolean }).is_area_level_estimate : undefined;
  const valueRange = insightsData && "value_range" in insightsData ? (insightsData as { value_range?: { low_estimate: number; estimated_value: number; high_estimate: number } }).value_range : undefined;
  const sourceSummary = insightsData && "source_summary" in insightsData ? (insightsData as { source_summary?: string }).source_summary : undefined;
  const lastMarketUpdate = insightsData && "last_market_update" in insightsData ? (insightsData as { last_market_update?: string }).last_market_update : undefined;
  const nearbySales = insightsData && "nearby_sales" in insightsData ? (insightsData as { nearby_sales?: Array<{ address: string; price: number; date: string; distance_m?: number; price_per_sqft?: number; is_same_property?: boolean }> }).nearby_sales : undefined;
  const lastRecordedSale = insightsData && "last_recorded_sale" in insightsData ? (insightsData as { last_recorded_sale?: { price: number; date: string; source?: string } }).last_recorded_sale : undefined;
  const estimatedAreaPrice = insightsData && "estimated_area_price" in insightsData ? (insightsData as { estimated_area_price?: number | null }).estimated_area_price : undefined;
  const medianSalePrice = insightsData && "median_sale_price" in insightsData ? (insightsData as { median_sale_price?: number | null }).median_sale_price : undefined;
  const medianPricePerSqft = insightsData && "median_price_per_sqft" in insightsData ? (insightsData as { median_price_per_sqft?: number | null }).median_price_per_sqft : undefined;
  const usMarketTrend = insightsData && "market_trend" in insightsData ? (insightsData as { market_trend?: { change_1y_percent: number } }).market_trend : undefined;
  const inventorySignal = insightsData && "inventory_signal" in insightsData ? (insightsData as { inventory_signal?: number | null }).inventory_signal : undefined;
  const daysOnMarket = insightsData && "days_on_market" in insightsData ? (insightsData as { days_on_market?: number | null }).days_on_market : undefined;
  const ukLandRegistryRaw = insightsData && "uk_land_registry" in insightsData ? (insightsData as { uk_land_registry?: { building_average_price: number | null; transactions_in_building: number; latest_building_transaction: { price: number; date: string; property_type?: string } | null; latest_nearby_transaction?: { price: number; date: string; property_type?: string } | null; has_building_match: boolean; average_area_price: number | null; median_area_price?: number | null; price_trend?: { change_1y_percent: number; ref_month?: string } | null; area_data_source?: "land_registry" | "HPI"; area_transaction_count: number; area_fallback_level: "postcode" | "outward_postcode" | "postcode_area" | "street" | "locality" | "none"; fallback_level_used?: "building" | "postcode" | "locality" | "area"; match_confidence?: "high" | "medium" | "low" } }).uk_land_registry : undefined;
  const ukLandRegistryFallback =
    isUK && insightsData != null && !ukLandRegistryRaw
      ? {
          building_average_price: null as number | null,
          transactions_in_building: 0,
          latest_building_transaction: null as { price: number; date: string; property_type?: string } | null,
          latest_nearby_transaction: null as { price: number; date: string; property_type?: string } | null,
          has_building_match: false,
          average_area_price: null as number | null,
          median_area_price: null as number | null,
          price_trend: null as { change_1y_percent: number; ref_month?: string } | null,
          area_transaction_count: 0,
          area_fallback_level: "none" as const,
          fallback_level_used: "area" as const,
          match_confidence: "low" as const,
          area_data_source: "land_registry" as const,
        }
      : undefined;
  const ukLandRegistry = ukLandRegistryRaw ?? ukLandRegistryFallback;

  const parsedLocal = React.useMemo((): ParsedAddress => {
    if (countryCode === "US") {
      const us = parseUSAddressFromFullString(address);
      return {
        city: us.city,
        street: us.street,
        houseNumber: us.houseNumber,
        state: us.state,
        zip: us.zip,
        country: "US",
      };
    }
    if (countryCode === "UK" || countryCode === "GB") {
      const uk = parseUKAddressFromFullString(address);
      return {
        city: uk.city,
        street: uk.street,
        houseNumber: uk.houseNumber,
        zip: uk.postcode,
        country: "UK",
      };
    }
    if (countryCode === "FR") {
      const fr = parseFRAddressFromFullString(address);
      return {
        city: fr.city,
        street: fr.street,
        houseNumber: fr.houseNumber,
        zip: fr.postcode,
        country: "FR",
      };
    }
    const g = parseAddressFromFullString(address);
    return { city: g.city, street: g.street, houseNumber: g.houseNumber };
  }, [address, countryCode]);
  const canonicalLocal = React.useMemo(
    () => toCanonicalAddress(parsedLocal.city, parsedLocal.street, parsedLocal.houseNumber),
    [parsedLocal]
  );

  const [mounted, setMounted] = React.useState(false);
  React.useEffect(() => {
    const id = requestAnimationFrame(() => setMounted(true));
    return () => cancelAnimationFrame(id);
  }, []);

  const source = insightsData && "source" in insightsData ? (insightsData as { source?: string }).source : undefined;
  const estimateIsRent = estimate && isLikelyRent(estimate.estimated_value, source);
  const estimateIsStreetValue = estimate && "value_type" in estimate && estimate.value_type === "street_median";

  const insightsForTitle = activeInsightsData ?? insightsData;
  const providerAddr =
    insightsForTitle && "address" in insightsForTitle
      ? (insightsForTitle as { address?: { city?: string; street?: string; house_number?: string } }).address
      : undefined;
  const isNycTruthTitle =
    isUS &&
    insightsForTitle &&
    typeof insightsForTitle === "object" &&
    (insightsForTitle as { data_source?: string }).data_source === "us_nyc_truth";
  const formatProviderAddress = (a: { city?: string; street?: string; house_number?: string }) =>
    [a.house_number, a.street, a.city].filter(Boolean).join(", ").trim() || "";
  const displayAddress =
    selectedFormattedAddress ??
    (providerAddr
      ? isNycTruthTitle
        ? formatNycCardDisplayAddress(providerAddr) || formatProviderAddress(providerAddr)
        : formatProviderAddress(providerAddr)
      : null) ??
    address;

  const data = insightsData;
  const apiStatus = isUS
    ? (usNycDisplayStatusResolved ?? (insightsData as { status?: string } | null | undefined)?.status)
    : (insightsData as { status?: string } | null | undefined)?.status;
  console.log("[PROPERTY_CARD] received status:", apiStatus);

  return (
    <div
      className={[
        "pointer-events-none transition-all duration-300 ease-out",
        isLotInputFocused && isMobileViewport
          ? "fixed inset-x-2 z-50"
          : "fixed right-3 bottom-[100px] z-50 w-[80vw] max-w-[300px]",
        mounted ? "translate-y-0 opacity-100" : "translate-y-4 opacity-0",
      ].join(" ")}
      style={{
        ...(isLotInputFocused && isMobileViewport
          ? {
              bottom: `calc(${isMobileViewport ? "6rem" : "1.5rem"} + ${keyboardInsetPx}px)`,
              top: "0.5rem",
            }
          : {}),
      }}
    >
      <div className={[
        "pointer-events-auto flex min-h-0 w-full shrink-0 flex-col overflow-hidden rounded-xl border border-amber-400/20 bg-black/85 shadow-[0_12px_40px_-8px_rgba(0,0,0,0.55)] backdrop-blur-xl",
        isLotInputFocused && isMobileViewport
          ? "max-h-[calc(100dvh-1rem)]"
          : "max-h-[calc(100vh-8rem)]",
      ].join(" ")}>
        <div className="flex shrink-0 items-start justify-between gap-1.5 border-b border-amber-400/15 bg-black/90 px-2 py-1 sm:px-2.5 sm:py-1">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-1.5">
              <div className="text-[8px] uppercase tracking-[0.15em] text-amber-400/90">Property Value</div>
              {dataSource === "mock" && (
                <span className="rounded border border-amber-500/50 bg-amber-500/20 px-1.5 py-0.5 text-[9px] font-medium text-amber-300">
                  Mock Data Mode
                </span>
              )}
            </div>
            <div className="mt-0.5 truncate text-[11px] font-medium text-white">{toEnglishDisplay(displayAddress)}</div>
          </div>
          <div className="flex shrink-0 items-center gap-1.5">
            {hasOfficialProvider && (
              <button
                type="button"
                onClick={() => setDebugMode((d) => !d)}
                aria-label={debugMode ? "Hide debug" : "Show debug"}
                title={debugMode ? "Hide debug mode" : "Show debug mode"}
                className={`rounded-full p-1.5 transition-colors ${debugMode ? "bg-amber-500/30 text-amber-300" : "text-zinc-500 hover:text-zinc-400"}`}
              >
                <Bug className="size-3.5" aria-hidden />
              </button>
            )}
            {onToggleSave && <HeartButton isSaved={isSaved} onToggle={onToggleSave} iconSize="size-3.5" />}
            <button type="button" onClick={onClose} aria-label="Close" className="rounded-full border border-white/10 p-1.5 text-zinc-400 transition-colors hover:border-white/20 hover:text-white">
              <X className="size-4" />
            </button>
          </div>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden rounded-b-xl bg-amber-400/5 px-2 py-1 sm:px-2.5 sm:py-1 overscroll-contain">
          {isLoading && hasOfficialProvider ? (
            <div className="space-y-2 animate-pulse">
              <div className="h-4 w-32 rounded bg-zinc-600/40" />
              <div className="h-6 w-40 rounded bg-zinc-600/50" />
              <div className="h-3 w-full max-w-[200px] rounded bg-zinc-600/30" />
              <div className="mt-3 h-16 rounded-lg border border-zinc-500/20 bg-zinc-500/5" />
              <div className="h-12 rounded-lg border border-zinc-500/20 bg-zinc-500/5" />
              <div className="h-12 rounded-lg border border-zinc-500/20 bg-zinc-500/5" />
            </div>
          ) : !hasOfficialProvider ? (
            <div className="space-y-0.5">
              <div className="text-[8px] uppercase tracking-[0.15em] text-amber-300/80">Estimated Value</div>
              <div className="text-sm font-bold text-amber-400">{formatCurrency(mockData.valueNumber, currencySymbol)}</div>
              <div className="text-[10px] text-zinc-400">
                {mockData.pricePerSqm.toLocaleString()} {currencySymbol}/ sqm
                <span className="ml-2 text-emerald-400">↑ {mockData.trendYoY >= 0 ? "+" : ""}{mockData.trendYoY.toFixed(1)}% YoY</span>
              </div>
            </div>
          ) : isUS &&
            hasOfficialProvider &&
            (apiStatus === "commercial_property" ||
              (apiStatus == null && nycUnitSubmitted?.trim())) ? (
            <div className="space-y-2 rounded-lg border border-amber-500/15 bg-zinc-950/85 p-2.5 shadow-inner shadow-black/30">
              <p className="text-[10px] leading-tight text-zinc-300">
                🏢 Commercial Property — No residential data available
              </p>
            </div>
          ) : isUS && apiStatus === "requires_unit" && hasOfficialProvider ? (
            <div className="space-y-2 rounded-lg border border-amber-500/15 bg-zinc-950/85 p-2.5 shadow-inner shadow-black/30">
              <p>Please enter a unit number</p>
              <div className="mt-1.5 flex flex-wrap gap-1.5">
                <input
                  type="text"
                  value={nycUnitDraft}
                  onChange={(e) => setNycUnitDraft(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      const unitValue = nycUnitDraft.trim();
                      console.log("[UNIT_SUBMIT] unit value:", unitValue);
                      void submitNycUnitApply();
                    }
                  }}
                  placeholder="e.g. 4B"
                  disabled={nycUnitApplyLoading || isLoading}
                  className="min-w-[5rem] flex-1 rounded border border-amber-500/25 bg-black/60 px-1.5 py-1 text-[10px] leading-tight text-zinc-100 placeholder:text-zinc-600 focus:border-amber-400/50 focus:outline-none focus:ring-1 focus:ring-amber-500/30 disabled:opacity-50"
                  autoComplete="off"
                />
                <button
                  type="button"
                  disabled={nycUnitApplyLoading || isLoading || !nycUnitDraft.trim()}
                  onClick={() => {
                    const unitValue = nycUnitDraft.trim();
                    console.log("[UNIT_SUBMIT] unit value:", unitValue);
                    void submitNycUnitApply();
                  }}
                  className="shrink-0 rounded border border-amber-500/40 bg-amber-500/15 px-2 py-1 text-[9px] font-semibold uppercase tracking-wide text-amber-100 hover:bg-amber-500/25 disabled:pointer-events-none disabled:opacity-40"
                >
                  {nycUnitApplyLoading || isLoading ? "…" : "Apply"}
                </button>
              </div>
              {nycUnitApplyLoading ? (
                <p className="mt-1.5 text-[8px] leading-tight text-zinc-500">Searching…</p>
              ) : null}
              {nycUnitApplyError ? <p className="text-[9px] text-amber-400/90">{nycUnitApplyError}</p> : null}
            </div>
          ) : unitRequired ? (
            <div className="space-y-0.5">
              <div className="flex items-center gap-1 text-amber-300/90">
                <Building2 className="size-2.5 shrink-0" aria-hidden />
                <span className="text-[11px] font-medium">Unit number required</span>
              </div>
              <p className="text-[10px] text-zinc-400">
                This building requires a unit number to retrieve property data.
              </p>
            </div>
          ) : noDataAvailable ? (
            <div className="space-y-0.5">
              <div className="flex items-center gap-1 text-amber-300/90">
                <FileText className="size-2.5 shrink-0" aria-hidden />
                <span className="text-[11px] font-medium">{isUS ? "No estimate available" : "No Data Available"}</span>
              </div>
              <p className="text-[10px] text-zinc-400">
                {isUS
                  ? "We couldn't retrieve an estimate or official sale for this address."
                  : "No AVM estimate or sale history could be retrieved for this address."}
              </p>
            </div>
          ) : isFranceData && multipleUnits ? (
            <div className="space-y-1">
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-2 py-1">
                <div className="text-[8px] uppercase tracking-wider text-amber-400/90">Apartment building</div>
                <p className="mt-0.5 text-[10px] text-zinc-300">Enter apartment/lot number to see exact value.</p>
                {Array.isArray(availableLots) && availableLots.length > 0 && (
                  <p className="mt-0.5 text-[9px] text-zinc-400">Available lots: {availableLots.join(", ")}</p>
                )}
                <div className="mt-1 flex gap-1.5">
                  <input
                    type="text"
                    placeholder="Apartment / Lot"
                    value={aptNumber}
                    onChange={(e) => setAptNumber(e.target.value)}
                    onFocus={() => setIsLotInputFocused(true)}
                    onBlur={() => setIsLotInputFocused(false)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        e.stopPropagation();
                        if ((e as unknown as { isComposing?: boolean }).isComposing) return;
                        submitLotSearch("enter");
                      }
                    }}
                    className="flex-1 rounded border border-zinc-600 bg-zinc-900/80 px-1.5 py-1 text-[16px] sm:text-[11px] text-white placeholder:text-zinc-500"
                  />
                  <button
                    type="button"
                    disabled={isLoading}
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      submitLotSearch("button");
                    }}
                    className="shrink-0 rounded border border-violet-500/50 bg-violet-500/20 px-2 py-1 text-[10px] font-medium text-violet-300 hover:bg-violet-500/30 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {isLoading ? "…" : "Search"}
                  </button>
                </div>
              </div>
              <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1">
                <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Average Building Value</div>
                <div className="mt-0.5 text-sm font-semibold text-zinc-200">
                  {isLoading ? (
                    <span className="inline-block h-5 w-24 animate-pulse rounded bg-zinc-600/40" aria-hidden />
                  ) : averageBuildingValueFrance != null && averageBuildingValueFrance > 0 ? (
                    formatFranceEuroTotal(averageBuildingValueFrance)
                  ) : (
                    "—"
                  )}
                </div>
                <div className="mt-0.5 text-[9px] text-zinc-500">
                  {aptNumber.trim() ? "Enter lot and Search for exact value" : "Building-level estimate (enter lot for apartment-specific data)"}
                </div>
              </div>
              {Array.isArray(buildingSales) && buildingSales.length > 0 && (
                <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 overflow-hidden">
                  <div className="border-b border-zinc-500/20 bg-zinc-500/10 px-2 py-0.5">
                    <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Recent sales ({buildingSales.length})</div>
                  </div>
                  <div className="max-h-[80px] overflow-y-auto overflow-x-auto">
                    <table className="w-full text-[9px] sm:text-[10px]">
                      <thead>
                        <tr className="border-b border-zinc-500/20 text-left text-zinc-500">
                          <th className="px-1.5 py-0.5 font-medium">Lot</th>
                          <th className="px-1.5 py-0.5 font-medium">Date</th>
                          <th className="px-1.5 py-0.5 font-medium">Type</th>
                          <th className="px-1.5 py-0.5 font-medium text-right">Price</th>
                          <th className="px-1.5 py-0.5 font-medium text-right">m²</th>
                        </tr>
                      </thead>
                      <tbody>
                        {buildingSales.slice(0, 4).map((sale, i) => (
                          <tr key={i} className="border-b border-zinc-500/10 last:border-0">
                            <td className="px-1.5 py-0.5 text-zinc-400">{sale.lot_number ?? "—"}</td>
                            <td className="px-1.5 py-0.5 text-zinc-300">{sale.date ? formatSaleDate(sale.date) : "—"}</td>
                            <td className="px-1.5 py-0.5 text-zinc-400 truncate max-w-[48px]">{sale.type}</td>
                            <td className="px-1.5 py-0.5 text-right font-medium text-zinc-200">{formatFranceEuroTotal(sale.price)}</td>
                            <td className="px-1.5 py-0.5 text-right text-zinc-400">{sale.surface != null && sale.surface > 0 ? sale.surface : "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          ) : isFranceData && propertyResult && hasFranceBuildingOrAreaData ? (() => {
            const frRow = frPropertyResultForDisplay ?? propertyResult;
            if (!frRow) return null;
            return (
            <div className="space-y-1">
              {promptForApartment && (
                <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-2 py-1">
                  <div className="text-[8px] uppercase tracking-wider text-amber-400/90">
                    {apartmentNotMatched ? "Apartment not found" : "Multiple units"}
                  </div>
                  <p className="mt-0.5 text-[10px] text-zinc-300">
                    {apartmentNotMatched
                      ? "Enter a different apartment/lot number to see exact value."
                      : "Enter apartment/lot number to see exact value."}
                  </p>
                  {Array.isArray(availableLots) && availableLots.length > 0 && (
                    <p className="mt-0.5 text-[9px] text-zinc-400">Available lots: {availableLots.join(", ")}</p>
                  )}
                  <div className="mt-1 flex gap-1.5">
                    <input
                      type="text"
                      placeholder="Apartment / Lot"
                      value={aptNumber}
                      onChange={(e) => setAptNumber(e.target.value)}
                      onFocus={() => setIsLotInputFocused(true)}
                      onBlur={() => setIsLotInputFocused(false)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") {
                          e.preventDefault();
                          e.stopPropagation();
                          if ((e as unknown as { isComposing?: boolean }).isComposing) return;
                          submitLotSearch("enter");
                        }
                      }}
                      className="flex-1 rounded border border-zinc-600 bg-zinc-900/80 px-1.5 py-1 text-[16px] sm:text-[11px] text-white placeholder:text-zinc-500"
                    />
                    <button
                      type="button"
                      disabled={isLoading}
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        submitLotSearch("button");
                      }}
                      className="shrink-0 rounded border border-violet-500/50 bg-violet-500/20 px-2 py-1 text-[10px] font-medium text-violet-300 hover:bg-violet-500/30 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {isLoading ? "…" : "Search"}
                    </button>
                  </div>
                </div>
              )}
              <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 px-2 py-1">
                <div className="text-[8px] uppercase tracking-wider text-violet-400/90">
                  {frRow.value_level === "building-level" ? "Building-level estimate" : frRow.value_level === "area-level" ? "Area-level estimate" : "Estimated value"}
                </div>
                {lotNumber && (
                  <div className="mt-0.5 text-[9px] text-zinc-400">Lot: {String(lotNumber)}</div>
                )}
                <div className="mt-0.5 text-sm font-semibold text-violet-300">
                  {frRow.exact_value != null && frRow.exact_value > 0
                    ? formatFranceEuroTotal(frRow.exact_value)
                    : (frRow.value_level === "building-level" || frRow.value_level === "area-level") &&
                      (frRow.street_average != null && frRow.street_average > 0)
                    ? formatFranceEuroTotal(frRow.street_average)
                    : (frRow.exact_value_message ?? "No DVF data for this area")}
                </div>
                {apartmentNotMatched && frRow.exact_value_message && (
                  <div className="mt-0.5 text-[9px] text-amber-400/90">{frRow.exact_value_message}</div>
                )}
                {surfaceReelleBatiFrance != null && surfaceReelleBatiFrance > 0 && frRow.exact_value != null && frRow.exact_value > 0 && (
                  <div className="mt-0.5 text-[9px] text-zinc-400">
                    Estimated €/m²: {formatFranceEuroPerSqm(Math.round(frRow.exact_value / surfaceReelleBatiFrance))}
                  </div>
                )}
                <div className="mt-0.5 text-[9px] text-zinc-500">DVF government data</div>
              </div>
              <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1">
                <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Last transaction</div>
                <div className="mt-0.5 text-[11px] font-medium text-zinc-300">
                  {frRow.last_transaction.amount > 0
                    ? `${formatFranceEuroTotal(frRow.last_transaction.amount)}${frRow.last_transaction.date ? ` · Sold in: ${formatSaleDate(frRow.last_transaction.date)}` : ""}`
                    : (frRow.last_transaction.message ?? "No recorded transaction")}
                </div>
                {surfaceReelleBatiFrance != null && surfaceReelleBatiFrance > 0 && frRow.last_transaction.amount > 0 && (
                  <div className="mt-0.5 text-[9px] text-zinc-400">
                    Last transaction €/m²: {formatFranceEuroPerSqm(Math.round(frRow.last_transaction.amount / surfaceReelleBatiFrance))}
                  </div>
                )}
              </div>
              <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1">
                <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Area average</div>
                <div className="mt-0.5 text-[11px] font-medium text-zinc-300">
                  {frRow.street_average != null && frRow.street_average > 0
                    ? formatFranceEuroTotal(frRow.street_average)
                    : (frRow.street_average_message ?? "No DVF data for this area")}
                </div>
              </div>
              <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1">
                <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Livability</div>
                <div className={`mt-0.5 text-[11px] font-medium ${
                  frRow.livability_rating === "EXCELLENT" ? "text-emerald-400" :
                  frRow.livability_rating === "VERY GOOD" ? "text-emerald-500/90" :
                  frRow.livability_rating === "GOOD" ? "text-amber-400" :
                  frRow.livability_rating === "FAIR" ? "text-amber-500/90" :
                  "text-zinc-400"
                }`}>
                  {frRow.livability_rating}
                </div>
              </div>
              {Array.isArray(buildingSales) && buildingSales.length > 0 && (
                <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 overflow-hidden">
                  <div className="border-b border-zinc-500/20 bg-zinc-500/10 px-2 py-0.5">
                    <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Recent sales ({buildingSales.length})</div>
                  </div>
                  <div className="max-h-[80px] overflow-y-auto overflow-x-auto">
                    <table className="w-full text-[9px] sm:text-[10px]">
                      <thead>
                        <tr className="border-b border-zinc-500/20 text-left text-zinc-500">
                          <th className="px-1.5 py-0.5 font-medium">Lot</th>
                          <th className="px-1.5 py-0.5 font-medium">Date</th>
                          <th className="px-1.5 py-0.5 font-medium">Type</th>
                          <th className="px-1.5 py-0.5 font-medium text-right">Price</th>
                          <th className="px-1.5 py-0.5 font-medium text-right">m²</th>
                        </tr>
                      </thead>
                      <tbody>
                        {buildingSales.slice(0, 4).map((sale, i) => (
                          <tr key={i} className="border-b border-zinc-500/10 last:border-0">
                            <td className="px-1.5 py-0.5 text-zinc-400">{sale.lot_number ?? "—"}</td>
                            <td className="px-1.5 py-0.5 text-zinc-300">{sale.date ? formatSaleDate(sale.date) : "—"}</td>
                            <td className="px-1.5 py-0.5 text-zinc-400 truncate max-w-[48px]">{sale.type}</td>
                            <td className="px-1.5 py-0.5 text-right font-medium text-zinc-200">{formatFranceEuroTotal(sale.price)}</td>
                            <td className="px-1.5 py-0.5 text-right text-zinc-400">{sale.surface != null && sale.surface > 0 ? sale.surface : "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
              <div className="pt-0.5 text-[9px] text-zinc-500">
                DVF (Demandes de Valeurs Foncières). DGFiP. Government open data.
              </div>
              {debugMode && hasOfficialProvider && (
                <CollapsibleSection title="Debug Info">
                  <DebugPanel
                    address={address}
                    parsed={parsedLocal}
                    canonical={canonicalLocal}
                    insightsData={insightsData}
                    latest={latest}
                    currencySymbol={currencySymbol}
                  />
                </CollapsibleSection>
              )}
            </div>
            );
          })() : isIsrael && propertyResult ? (
            <div className="space-y-1.5" dir="rtl">
              <div className="rounded-full border border-zinc-600/50 bg-zinc-900/80 px-3 py-2">
                <div className="text-[9px] font-medium uppercase tracking-wider text-amber-400/90">שווי נכס כיום</div>
                <div className="mt-0.5 text-sm font-semibold text-white">
                  {propertyResult.exact_value != null && propertyResult.exact_value > 0
                    ? formatCurrency(propertyResult.exact_value, currencySymbol)
                    : propertyResult.exact_value_message ?? "—"}
                </div>
              </div>
              <div className="rounded-full border border-zinc-600/50 bg-zinc-900/80 px-3 py-2">
                <div className="text-[9px] font-medium uppercase tracking-wider text-amber-400/90">מכירה אחרונה</div>
                <div className="mt-0.5 text-[11px] font-medium text-zinc-200">
                  {propertyResult.last_transaction.amount > 0
                    ? `${formatCurrency(propertyResult.last_transaction.amount, currencySymbol)}${propertyResult.last_transaction.date ? ` · ${formatSaleDate(propertyResult.last_transaction.date)}` : ""}`
                    : propertyResult.last_transaction.message ?? "—"}
                </div>
              </div>
              <div className="rounded-full border border-zinc-600/50 bg-zinc-900/80 px-3 py-2">
                <div className="text-[9px] font-medium uppercase tracking-wider text-amber-400/90">ממוצע רחוב</div>
                <div className="mt-0.5 text-[11px] font-medium text-zinc-200">
                  {propertyResult.street_average != null && propertyResult.street_average > 0
                    ? formatCurrency(propertyResult.street_average, currencySymbol)
                    : propertyResult.street_average_message ?? "—"}
                </div>
              </div>
              <div className="rounded-full border border-zinc-600/50 bg-zinc-900/80 px-3 py-2">
                <div className="text-[9px] font-medium uppercase tracking-wider text-amber-400/90">איכות השכונה</div>
                <div className={`mt-0.5 text-[11px] font-medium ${
                  propertyResult.livability_rating === "EXCELLENT" ? "text-emerald-400" :
                  propertyResult.livability_rating === "VERY GOOD" ? "text-emerald-500/90" :
                  propertyResult.livability_rating === "GOOD" ? "text-amber-400" :
                  propertyResult.livability_rating === "FAIR" ? "text-amber-500/90" :
                  "text-zinc-400"
                }`}>
                  {propertyResult.livability_rating}
                </div>
              </div>
              {debugMode && hasOfficialProvider && (
                <CollapsibleSection title="Debug Info">
                  <DebugPanel
                    address={address}
                    parsed={parsedLocal}
                    canonical={canonicalLocal}
                    insightsData={insightsData}
                    latest={latest}
                    currencySymbol={currencySymbol}
                  />
                </CollapsibleSection>
              )}
            </div>
          ) : isFranceData && !hasFranceBuildingOrAreaData && !hasStickyFranceBuilding ? (
            <div className="text-[11px] text-zinc-500">
              {isLoading ? "Loading property data…" : "No DVF data for this area."}
            </div>
          ) : !hasPropertyData && !ukLandRegistry && !isUsRequiresUnit && !isUsCommercial ? (
            <div className="space-y-1">
              <div className="flex items-center gap-1.5 text-amber-300/90">
                <FileText className="size-3 shrink-0" aria-hidden />
                <span className="text-xs font-medium">
                  {isUK
                    ? "No exact UK property record found for this address"
                    : isUS
                      ? "No NYC property record found"
                      : "No property data found for this address"}
                </span>
              </div>
              <p className="text-[11px] text-zinc-400">
                {isUK
                  ? "Map location found, but no Land Registry or EPC record matches this address."
                  : isUS
                    ? "We couldn't match this address to a property in our NYC records. It may be outside current coverage, or the address may need a small correction."
                    : "We only show data when there is a high-confidence match for the exact address."}
              </p>
              {insightsData?.debug && (
                <CollapsibleSection title="Debug Info">
                  <pre className="max-h-32 overflow-auto rounded bg-black/30 p-2 font-mono text-[10px]">
                    {JSON.stringify(sanitizeForDisplay(insightsData.debug), null, 2)}
                  </pre>
                </CollapsibleSection>
              )}
            </div>
          ) : isUS &&
            activeInsightsData &&
            typeof activeInsightsData === "object" &&
            (((activeInsightsData as { data_source?: string }).data_source === "us_nyc_truth") ||
              isUsRequiresUnit ||
              isUsCommercial) ? (
            <div className="space-y-2 rounded-lg border border-amber-500/15 bg-zinc-950/85 p-2.5 shadow-inner shadow-black/30">
              <UsNycTruthPropertyCard
                data={(nycTruthDisplay ?? activeInsightsData) as UsNycTruthCardData}
                addressForFetch={addressForApi}
                currencySymbol={currencySymbol}
                status={resolvedStatus}
                isCommercial={isUsCommercial}
                apartmentFlowEnabled={!!((usNycApartmentFlowEnabled || isUsRequiresUnit) && !isUsCommercial)}
                showApartmentInput={!!((usNycApartmentFlowEnabled || isUsRequiresUnit) && !isUsCommercial && nycUnitSubmitted == null)}
                apartmentDraft={nycUnitDraft}
                onApartmentDraftChange={setNycUnitDraft}
                onApartmentSearch={() => {
                  void submitNycUnitApply();
                }}
                apartmentSearchInFlight={nycUnitApplyLoading || isLoading}
                submittedApartment={nycUnitSubmitted ?? undefined}
                onCheckAnotherApartment={() => {
                  setNycUnitSubmitted(null);
                  setNycUnitDraft("");
                  setNycUnitApplyPayload(null);
                  setNycUnitApplyError(null);
                  refetch();
                }}
              />
              {nycUnitApplyError ? (
                <p className="text-[9px] text-amber-400/90">{nycUnitApplyError}</p>
              ) : null}
              {(() => {
                if (!nycUnitApplyPayload || typeof nycUnitApplyPayload !== "object") return null;
                const st = nycUnitApplyPayload.unit_lookup_status;
                const sub = nycUnitApplyPayload.unit_or_lot_submitted;
                const subStr = typeof sub === "string" && sub.trim() !== "" ? sub.trim() : null;
                if (st === "matched") {
                  return (
                    <p className="text-[9px] text-emerald-400/90">
                      Unit or lot matched in NYC records{subStr ? ` (${subStr})` : ""}.
                    </p>
                  );
                }
                if (st === "not_found") {
                  return (
                    <p className="text-[9px] text-zinc-400">
                      No direct data for this unit. Showing similar unit
                      {subStr ? ` (${subStr})` : ""}.
                    </p>
                  );
                }
                return null;
              })()}
              {debugMode && hasOfficialProvider && (
                <CollapsibleSection title="Debug Info">
                  <DebugPanel
                    address={address}
                    parsed={parsedLocal}
                    canonical={canonicalLocal}
                    insightsData={insightsData}
                    latest={latest}
                    currencySymbol={currencySymbol}
                  />
                </CollapsibleSection>
              )}
            </div>
          ) : isUS ? (
            <div className="space-y-1.5">
              {propertyResult ? (
                <>
                  <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 px-2 py-1 sm:px-2 sm:py-1.5">
                    <div className="text-[8px] uppercase tracking-wider text-violet-400/90">{getFirstCardWording(propertyResult.value_level).label}</div>
                    <div className="mt-0.5 text-sm font-semibold text-violet-300">
                      {propertyResult.exact_value != null && propertyResult.exact_value > 0
                        ? formatCurrency(propertyResult.exact_value, currencySymbol)
                        : propertyResult.exact_value_message ?? "No exact property-level value found"}
                    </div>
                    <div className="mt-0.5 text-[9px] text-zinc-500">{getFirstCardWording(propertyResult.value_level).supportText}</div>
                    {propertyResult.value_level && (
                      <div className="mt-0.5 text-[9px] text-zinc-500/80">Level: {propertyResult.value_level}</div>
                    )}
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1 sm:px-2 sm:py-1.5">
                    <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Last transaction</div>
                    <div className="mt-0.5 text-[11px] font-medium text-zinc-300">
                      {propertyResult.last_transaction.amount > 0
                        ? `${formatCurrency(propertyResult.last_transaction.amount, currencySymbol)}${propertyResult.last_transaction.date ? ` · ${formatSaleDate(propertyResult.last_transaction.date)}` : ""}`
                        : propertyResult.last_transaction.message ?? "No recorded transaction found"}
                    </div>
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1 sm:px-2 sm:py-1.5">
                    <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Street average</div>
                    <div className="mt-0.5 text-[11px] font-medium text-zinc-300">
                      {propertyResult.street_average != null && propertyResult.street_average > 0
                        ? formatCurrency(propertyResult.street_average, currencySymbol)
                        : propertyResult.street_average_message ?? "No street-level average found"}
                    </div>
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1 sm:px-2 sm:py-1.5">
                    <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Livability</div>
                    <div className={`mt-0.5 text-[11px] font-medium ${
                      propertyResult.livability_rating === "EXCELLENT" ? "text-emerald-400" :
                      propertyResult.livability_rating === "VERY GOOD" ? "text-emerald-500/90" :
                      propertyResult.livability_rating === "GOOD" ? "text-amber-400" :
                      propertyResult.livability_rating === "FAIR" ? "text-amber-500/90" :
                      "text-zinc-400"
                    }`}>
                      {propertyResult.livability_rating}
                    </div>
                  </div>
                </>
              ) : (
                <div className="text-[10px] text-zinc-500">Loading property data…</div>
              )}
              {debugMode && hasOfficialProvider && (
                <CollapsibleSection title="Debug Info">
                  <DebugPanel
                    address={address}
                    parsed={parsedLocal}
                    canonical={canonicalLocal}
                    insightsData={insightsData}
                    latest={latest}
                    currencySymbol={currencySymbol}
                  />
                </CollapsibleSection>
              )}
            </div>
          ) : isUK && (propertyResult || ukLandRegistry) ? (
            <div className="space-y-1.5">
              {propertyResult ? (
                <>
                  <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 px-2 py-1 sm:px-2 sm:py-1.5">
                    <div className="text-[8px] uppercase tracking-wider text-violet-400/90">{getFirstCardWording(propertyResult.value_level).label}</div>
                    <div className="mt-0.5 text-sm font-semibold text-violet-300">
                      {propertyResult.exact_value != null && propertyResult.exact_value > 0
                        ? formatCurrency(propertyResult.exact_value, currencySymbol)
                        : propertyResult.exact_value_message ?? "No exact property-level value found"}
                    </div>
                    <div className="mt-0.5 text-[9px] text-zinc-500">{getFirstCardWording(propertyResult.value_level).supportText}</div>
                    {propertyResult.value_level && propertyResult.value_level !== "no_match" && (
                      <div className="mt-0.5 text-[9px] text-zinc-500/80">Level: {propertyResult.value_level}</div>
                    )}
                    {propertyResult.value_level === "no_match" && (
                      <div className="mt-0.5 text-[9px] text-zinc-500">Map location found; no property record</div>
                    )}
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1 sm:px-2 sm:py-1.5">
                    <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Last transaction</div>
                    <div className="mt-0.5 text-[11px] font-medium text-zinc-300">
                      {propertyResult.last_transaction.amount > 0
                        ? `${formatCurrency(propertyResult.last_transaction.amount, currencySymbol)}${propertyResult.last_transaction.date ? ` · ${formatSaleDate(propertyResult.last_transaction.date)}` : ""}`
                        : propertyResult.last_transaction.message ?? "No recorded transaction found"}
                    </div>
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1 sm:px-2 sm:py-1.5">
                    <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Street average</div>
                    <div className="mt-0.5 text-[11px] font-medium text-zinc-300">
                      {propertyResult.street_average != null && propertyResult.street_average > 0
                        ? formatCurrency(propertyResult.street_average, currencySymbol)
                        : propertyResult.street_average_message ?? "No street-level average found"}
                    </div>
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1 sm:px-2 sm:py-1.5">
                    <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Livability</div>
                    <div className={`mt-0.5 text-[11px] font-medium ${
                      propertyResult.livability_rating === "EXCELLENT" ? "text-emerald-400" :
                      propertyResult.livability_rating === "VERY GOOD" ? "text-emerald-500/90" :
                      propertyResult.livability_rating === "GOOD" ? "text-amber-400" :
                      propertyResult.livability_rating === "FAIR" ? "text-amber-500/90" :
                      "text-zinc-400"
                    }`}>
                      {propertyResult.livability_rating}
                    </div>
                  </div>
                </>
              ) : (
                <div className="text-[10px] text-zinc-500">Loading property data…</div>
              )}
              <div className="pt-0.5 text-[9px] text-zinc-500">
                {ukLandRegistry?.area_data_source === "HPI"
                  ? "UK House Price Index. ONS / HM Land Registry. Government open data."
                  : "HM Land Registry Price Paid Data. Government open data."}
              </div>
              {debugMode && hasOfficialProvider && (
                <CollapsibleSection title="Debug Info">
                  <DebugPanel
                    address={address}
                    parsed={parsedLocal}
                    canonical={canonicalLocal}
                    insightsData={insightsData}
                    latest={latest}
                    currencySymbol={currencySymbol}
                  />
                </CollapsibleSection>
              )}
            </div>
          ) : (
            <div className="space-y-1.5">
              {estimate && (
                <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 px-2 py-0.5 sm:px-2.5 sm:py-1">
                  <div className="flex items-center gap-1 text-[9px] uppercase tracking-wider text-violet-400/90">
                    <Sparkles className="size-3 shrink-0" aria-hidden />
                    Market Value
                  </div>
                  <div className="mt-0.5 text-sm font-semibold text-violet-300">
                    {isFranceData ? formatFranceEuroTotal(estimate.estimated_value) : formatCurrency(estimate.estimated_value, currencySymbol)}
                  </div>
                  <div className="mt-0.5 text-[9px] text-violet-400/70">
                    {estimateIsStreetValue
                      ? "Estimated from provider data"
                      : estimateIsRent
                        ? "Estimated monthly rent from property data"
                        : "Estimated from provider data"}
                  </div>
                </div>
              )}
              <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-2 py-0.5 sm:px-2.5 sm:py-1">
                <div className="flex items-center gap-1 text-[9px] uppercase tracking-wider text-emerald-400/90">
                  <Building2 className="size-3 shrink-0" aria-hidden />
                  Transactions in Building (Last 5 Years)
                </div>
                <div className="mt-0.5 text-sm font-medium text-emerald-300">
                  {transactions5y > 0 ? transactions5y : "0"}
                </div>
              </div>
              <div className="rounded-lg border border-amber-400/20 bg-amber-400/5 px-2 py-0.5 sm:px-2.5 sm:py-1">
                <div className="flex items-center gap-1 text-[9px] uppercase tracking-wider text-amber-400/90">
                  <FileText className="size-3 shrink-0" aria-hidden />
                  Latest Building Transaction
                </div>
                <div className="mt-0.5 text-sm font-medium text-amber-200">
                  {latestBuildingAmount > 0
                    ? isFranceData
                      ? formatFranceEuroTotal(latestBuildingAmount)
                      : formatCurrency(latestBuildingAmount, currencySymbol)
                    : "No recent building transaction available"}
                  {latest?.transaction_date && (
                    <span className="ml-1 text-[10px] text-amber-400/70">
                      · {formatSaleDate(latest.transaction_date)}
                    </span>
                  )}
                </div>
              </div>
              <div className="flex shrink-0 items-center gap-2 pt-0.5">
                {isNearbyBuilding ? (
                  <span className="inline-flex items-center gap-1 rounded-full border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[9px] sm:text-[10px] font-medium text-amber-300">
                    {latestBuildingAmount > 0
                      ? "Based on the closest verified transaction on this street."
                      : "Estimated from provider data"}
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-1.5 py-0.5 text-[9px] sm:text-[10px] font-medium uppercase tracking-wider text-emerald-400">
                    <BadgeCheck className="size-2.5" aria-hidden /> Exact Building Match
                  </span>
                )}
              </div>
              {debugMode && hasOfficialProvider && (
                <CollapsibleSection title="Debug Info">
                  <DebugPanel
                    address={address}
                    parsed={parsedLocal}
                    canonical={canonicalLocal}
                    insightsData={insightsData}
                    latest={latest}
                    currencySymbol={currencySymbol}
                  />
                </CollapsibleSection>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
