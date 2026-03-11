"use client";

import * as React from "react";
import { X, FileText, Sparkles, Building2, BadgeCheck, Bug, ChevronDown, ChevronUp, Info } from "lucide-react";
import { HeartButton } from "@/components/heart-button";
import { calculatePropertyValue } from "@/lib/property-value";
import { usePropertyValueInsights } from "@/hooks/use-property-value-insights";
import { parseAddressFromFullString, parseUSAddressFromFullString, parseUKAddressFromFullString } from "@/lib/address-parse";
import { toCanonicalAddress } from "@/lib/address-canonical";
import { toEnglishDisplay, sanitizeForDisplay } from "@/lib/display-utils";

export type PropertyValueCardProps = {
  address: string;
  position: { lat: number; lng: number };
  currencySymbol?: string;
  countryCode?: string;
  onClose: () => void;
  isSaved?: boolean;
  onToggleSave?: () => void;
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
        className="flex w-full items-center justify-between px-2 py-0.5 sm:px-3 sm:py-1.5 text-left text-[9px] uppercase tracking-wider text-zinc-400/90 hover:bg-zinc-500/10 transition-colors"
      >
        <span>{title}{count != null && count > 0 ? ` (${count})` : ""}</span>
        {open ? <ChevronUp className="size-3 shrink-0" /> : <ChevronDown className="size-3 shrink-0" />}
      </button>
      {open && <div className="border-t border-zinc-500/20 px-2 py-1 sm:px-3 sm:py-1.5">{children}</div>}
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
          <div><span className="text-zinc-500">Price per m² used:</span> {d.price_per_m2_used}</div>
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
          <div><span className="text-zinc-500">Latest transaction amount:</span> {d.latest_transaction_amount}</div>
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
            <div>Price: {formatCurrency(latest.transaction_price, currencySymbol)}</div>
            <div>Size: {latest.property_size} m²</div>
            <div>Price/m²: {formatCurrency(latest.price_per_m2, currencySymbol)}</div>
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

export function PropertyValueCard({
  address,
  position,
  currencySymbol = "$",
  countryCode = "",
  onClose,
  isSaved = false,
  onToggleSave,
}: PropertyValueCardProps) {
  const isIsrael = countryCode === "IL";
  const isUS = countryCode === "US";
  const isUK = countryCode === "UK" || countryCode === "GB";
  const hasOfficialProvider = isIsrael || isUS || isUK;
  const mockData = React.useMemo(
    () => calculatePropertyValue(position.lat, position.lng, currencySymbol),
    [position.lat, position.lng, currencySymbol]
  );
  const { data: insightsData, isLoading } = usePropertyValueInsights(address, countryCode, {
    latitude: position?.lat,
    longitude: position?.lng,
    countryCode,
  });

  const [debugMode, setDebugMode] = React.useState(false);
  const hasPropertyData = insightsData?.address != null;
  const hasUSData =
    isUS &&
    (insightsData?.avm_value != null ||
      insightsData?.avm_rent != null ||
      (insightsData?.last_sale != null && insightsData.last_sale.price > 0) ||
      (insightsData?.sales_history != null && insightsData.sales_history.length > 0));
  const unitRequired = isUS && (insightsData?.error === "UNIT_REQUIRED" || insightsData?.debug?.unit_required === true);
  const noDataAvailable = isUS && insightsData?.message === "No Data Available" && !hasPropertyData && !unitRequired;
  const hasMatch = hasPropertyData && (insightsData?.match_quality === "exact_building" || insightsData?.match_quality === "nearby_building");
  const isNearbyBuilding = insightsData?.match_quality === "nearby_building";
  const latest = insightsData?.latest_transaction;
  const estimate = insightsData?.current_estimated_value;
  const building = insightsData?.building_summary_last_3_years;
  const transactions5y = building?.transactions_count_last_5_years ?? building?.transactions_count_last_3_years ?? 0;
  const latestBuildingAmount = building?.latest_building_transaction_price ?? latest?.transaction_price ?? 0;

  const avmValue = insightsData && "avm_value" in insightsData ? (insightsData as { avm_value?: number }).avm_value : undefined;
  const avmRent = insightsData && "avm_rent" in insightsData ? (insightsData as { avm_rent?: number }).avm_rent : undefined;
  const lastSaleFromProvider = insightsData && "last_sale" in insightsData ? (insightsData as { last_sale?: { price: number; date: string } }).last_sale : undefined;
  const salesHistory = insightsData && "sales_history" in insightsData ? (insightsData as { sales_history?: Array<{ date: string; price: number }> }).sales_history : undefined;
  const lastSale = React.useMemo(() => {
    if (salesHistory != null && salesHistory.length > 0) {
      const sorted = [...salesHistory].sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
      return { price: sorted[0].price, date: sorted[0].date };
    }
    return lastSaleFromProvider;
  }, [salesHistory, lastSaleFromProvider]);
  const nearbyComps = insightsData && "nearby_comps" in insightsData ? (insightsData as { nearby_comps?: { avg_price: number; avg_price_per_sqft: number; count: number } }).nearby_comps : undefined;
  const propertyDetails = insightsData && "property_details" in insightsData ? (insightsData as { property_details?: { beds?: number; baths?: number; sqft?: number; year_built?: number; property_type?: string } }).property_details : undefined;
  const neighborhoodStats = insightsData && "neighborhood_stats" in insightsData ? (insightsData as { neighborhood_stats?: { median_home_value: number; median_household_income: number; population: number; median_rent?: number; population_growth_percent?: number; income_growth_percent?: number } }).neighborhood_stats : undefined;
  const investmentMetrics = insightsData && "investment_metrics" in insightsData ? (insightsData as { investment_metrics?: { median_rent: number; gross_rent_yield_percent?: number; estimated_roi_percent?: number; median_price_per_sqft?: number } }).investment_metrics : undefined;
  const marketTrend = insightsData && "market_trend" in insightsData ? (insightsData as { market_trend?: { hpi_index: number; change_1y_percent: number } }).market_trend : undefined;
  const dataSource = insightsData && "data_source" in insightsData ? (insightsData as { data_source?: "live" | "cache" | "mock" }).data_source : undefined;
  const dataSources = insightsData && "data_sources" in insightsData ? (insightsData as { data_sources?: ("RentCast" | "Zillow" | "Redfin")[] }).data_sources : undefined;
  const usMatchConfidence = insightsData && "us_match_confidence" in insightsData ? (insightsData as { us_match_confidence?: "high" | "medium" | "low" }).us_match_confidence : undefined;
  const isAreaLevelEstimate = insightsData && "is_area_level_estimate" in insightsData ? (insightsData as { is_area_level_estimate?: boolean }).is_area_level_estimate : undefined;
  const valueRange = insightsData && "value_range" in insightsData ? (insightsData as { value_range?: { low_estimate: number; estimated_value: number; high_estimate: number } }).value_range : undefined;
  const propertyResult = insightsData && "property_result" in insightsData ? (insightsData as {
    property_result?: {
      exact_value: number | null;
      exact_value_message: string | null;
      value_level: "property-level" | "street-level" | "area-level";
      last_transaction: { amount: number; date: string | null; message?: string };
      street_average: number | null;
      street_average_message: string | null;
      livability_rating: "BAD" | "ALMOST GOOD" | "GOOD" | "VERY GOOD" | "EXCELLENT";
    };
  }).property_result : undefined;
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

  return (
    <div
      className={[
        "pointer-events-none absolute inset-x-4 bottom-4 z-20 flex justify-end transition-all duration-300 ease-out",
        mounted ? "translate-y-0 opacity-100" : "translate-y-4 opacity-0",
      ].join(" ")}
    >
      <div className="pointer-events-auto flex max-h-[55vh] sm:max-h-[65vh] w-full max-w-[360px] flex-col overflow-hidden rounded-2xl border border-amber-400/20 bg-black/85 shadow-2xl backdrop-blur-xl sm:max-w-[380px]">
        <div className="sticky top-0 z-10 flex shrink-0 items-start justify-between gap-2 border-b border-amber-400/15 bg-black/90 px-2 py-1.5 sm:px-3 sm:py-2">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <div className="text-[9px] uppercase tracking-[0.2em] text-amber-400/90">Property Value</div>
              {dataSource === "mock" && (
                <span className="rounded border border-amber-500/50 bg-amber-500/20 px-1.5 py-0.5 text-[9px] font-medium text-amber-300">
                  Mock Data Mode
                </span>
              )}
            </div>
            <div className="mt-0.5 truncate text-xs font-semibold text-white">{toEnglishDisplay(address)}</div>
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

        <div className="min-h-0 flex-1 overflow-y-auto rounded-b-2xl bg-amber-400/5 px-2 py-1.5 sm:px-3 sm:py-2.5">
          {isLoading && hasOfficialProvider ? (
            <div className="py-1.5 text-xs sm:text-sm text-amber-200/70">Loading official data…</div>
          ) : !hasOfficialProvider ? (
            <div className="space-y-1">
              <div className="text-[9px] sm:text-[10px] uppercase tracking-[0.18em] text-amber-300/80">Estimated Value</div>
              <div className="text-base font-bold text-amber-400 sm:text-lg">{formatCurrency(mockData.valueNumber, currencySymbol)}</div>
              <div className="text-[11px] sm:text-xs text-zinc-400">
                {mockData.pricePerSqm.toLocaleString()} {currencySymbol}/ sqm
                <span className="ml-2 text-emerald-400">↑ {mockData.trendYoY >= 0 ? "+" : ""}{mockData.trendYoY.toFixed(1)}% YoY</span>
              </div>
            </div>
          ) : unitRequired ? (
            <div className="space-y-1">
              <div className="flex items-center gap-1.5 text-amber-300/90">
                <Building2 className="size-3 shrink-0" aria-hidden />
                <span className="text-xs font-medium">Unit number required</span>
              </div>
              <p className="text-[11px] text-zinc-400">
                This building requires a unit number to retrieve property data.
              </p>
            </div>
          ) : noDataAvailable ? (
            <div className="space-y-1">
              <div className="flex items-center gap-1.5 text-amber-300/90">
                <FileText className="size-3 shrink-0" aria-hidden />
                <span className="text-xs font-medium">No Data Available</span>
              </div>
              <p className="text-[11px] text-zinc-400">
                No AVM estimate or sale history could be retrieved for this address.
              </p>
            </div>
          ) : !hasPropertyData && !ukLandRegistry ? (
            <div className="space-y-1">
              <div className="flex items-center gap-1.5 text-amber-300/90">
                <FileText className="size-3 shrink-0" aria-hidden />
                <span className="text-xs font-medium">
                  {isUK && insightsData?.message === "no transaction found"
                    ? "No UK Land Registry transaction found."
                    : "No property data found for this address"}
                </span>
              </div>
              <p className="text-[11px] text-zinc-400">
                {isUK && insightsData?.message === "no transaction found"
                  ? "No UK Land Registry transaction found."
                  : isUS
                    ? insightsData?.message ?? "No property record could be retrieved for this address."
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
          ) : isUS ? (
            <div className="space-y-2">
              {propertyResult ? (
                <>
                  <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 px-2 py-1.5 sm:px-2.5 sm:py-2">
                    <div className="text-[9px] uppercase tracking-wider text-violet-400/90">1. Exact value of this property</div>
                    <div className="mt-0.5 text-base font-semibold text-violet-300 sm:text-lg">
                      {propertyResult.exact_value != null && propertyResult.exact_value > 0
                        ? formatCurrency(propertyResult.exact_value, currencySymbol)
                        : propertyResult.exact_value_message ?? "No exact property-level value found"}
                    </div>
                    <div className="mt-0.5 text-[10px] text-zinc-500">Level: {propertyResult.value_level}</div>
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1.5 sm:px-2.5 sm:py-2">
                    <div className="text-[9px] uppercase tracking-wider text-zinc-400/90">2. Last recorded transaction</div>
                    <div className="mt-0.5 text-sm font-medium text-zinc-300">
                      {propertyResult.last_transaction.amount > 0
                        ? `${formatCurrency(propertyResult.last_transaction.amount, currencySymbol)}${propertyResult.last_transaction.date ? ` · ${formatSaleDate(propertyResult.last_transaction.date)}` : ""}`
                        : propertyResult.last_transaction.message ?? "No recorded transaction found"}
                    </div>
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1.5 sm:px-2.5 sm:py-2">
                    <div className="text-[9px] uppercase tracking-wider text-zinc-400/90">3. Average home price on the same street</div>
                    <div className="mt-0.5 text-sm font-medium text-zinc-300">
                      {propertyResult.street_average != null && propertyResult.street_average > 0
                        ? formatCurrency(propertyResult.street_average, currencySymbol)
                        : propertyResult.street_average_message ?? "No street-level average found"}
                    </div>
                  </div>
                  <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-1.5 sm:px-2.5 sm:py-2">
                    <div className="text-[9px] uppercase tracking-wider text-zinc-400/90">4. Neighborhood livability rating</div>
                    <div className={`mt-0.5 text-sm font-medium ${
                      propertyResult.livability_rating === "EXCELLENT" ? "text-emerald-400" :
                      propertyResult.livability_rating === "VERY GOOD" ? "text-emerald-500/90" :
                      propertyResult.livability_rating === "GOOD" ? "text-amber-400" :
                      propertyResult.livability_rating === "ALMOST GOOD" ? "text-amber-500/90" :
                      "text-zinc-400"
                    }`}>
                      {propertyResult.livability_rating}
                    </div>
                  </div>
                </>
              ) : (
                <div className="text-[11px] text-zinc-500">Loading property data…</div>
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
          ) : isUK && ukLandRegistry ? (
            <div className="space-y-1.5">
              <div className="flex items-center gap-2 flex-wrap">
                <div className="text-[9px] sm:text-[10px] uppercase tracking-[0.18em] text-amber-300/80">
                  {ukLandRegistry.area_data_source === "HPI" ? "UK House Price Index data" : "UK Land Registry Data"}
                </div>
                {ukLandRegistry.match_confidence && (
                  <span
                    className={`inline-flex items-center rounded px-1.5 py-0.5 text-[9px] font-medium uppercase tracking-wider ${
                      ukLandRegistry.match_confidence === "high"
                        ? "bg-emerald-500/20 text-emerald-400"
                        : ukLandRegistry.match_confidence === "medium"
                          ? "bg-amber-500/20 text-amber-400"
                          : "bg-zinc-500/20 text-zinc-400"
                    }`}
                    title={
                      ukLandRegistry.match_confidence === "high"
                        ? "Exact building match"
                        : ukLandRegistry.match_confidence === "medium"
                          ? ukLandRegistry.area_data_source === "HPI"
                            ? "HPI fallback"
                            : "Postcode or locality fallback"
                          : "Area-only fallback"
                    }
                  >
                    {ukLandRegistry.match_confidence} confidence
                  </span>
                )}
              </div>
              {(ukLandRegistry.has_building_match === false) && (
                <div className="rounded border border-amber-400/30 bg-amber-400/10 px-2 py-1 text-[10px] text-amber-300/90">
                  Area insights – no exact building match
                </div>
              )}
              <div className="space-y-1">
                <div className="flex items-center gap-1.5 text-[11px] text-amber-200/90">
                  <FileText className="size-3 shrink-0" aria-hidden />
                  {lastRecordedSale != null && lastRecordedSale.price > 0 ? (
                    <>
                      <span>Last Recorded Sale: {formatCurrency(lastRecordedSale.price, currencySymbol)} — Sold in {lastRecordedSale.date ? formatSaleDate(lastRecordedSale.date) : "—"}</span>
                      {lastRecordedSale.source && (
                        <span title={`Source: ${lastRecordedSale.source}`} className="text-[10px] text-zinc-500">({lastRecordedSale.source})</span>
                      )}
                    </>
                  ) : (
                    <span className="text-zinc-500">Last Recorded Sale: No recorded sale found</span>
                  )}
                </div>
                {(ukLandRegistry.has_building_match !== false) && (
                  <>
                    <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 px-2 py-0.5 sm:px-2.5 sm:py-1">
                      <div className="text-[9px] uppercase tracking-wider text-violet-400/90">Building Average Price</div>
                      <div className="mt-0.5 text-sm font-medium text-violet-300">
                        {ukLandRegistry.building_average_price != null && ukLandRegistry.building_average_price > 0
                          ? formatCurrency(ukLandRegistry.building_average_price, currencySymbol)
                          : (ukLandRegistry.latest_building_transaction != null && ukLandRegistry.latest_building_transaction.price > 0)
                            ? "No recent building transactions in the last 5 years."
                            : "No building transaction data available."}
                      </div>
                    </div>
                    <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-2 py-0.5 sm:px-2.5 sm:py-1">
                      <div className="text-[9px] uppercase tracking-wider text-emerald-400/90">Transactions in Building (Last 5 Years)</div>
                      <div className="mt-0.5 text-sm font-medium text-emerald-300">
                        {ukLandRegistry.transactions_in_building}
                      </div>
                      {(ukLandRegistry.transactions_in_building === 0 || ukLandRegistry.transactions_in_building == null) && ukLandRegistry.latest_building_transaction != null && ukLandRegistry.latest_building_transaction.price > 0 && (
                        <div className="mt-0.5 text-[10px] text-emerald-400/70">No recent building transactions in the last 5 years.</div>
                      )}
                    </div>
                    <div className="rounded-lg border border-amber-400/20 bg-amber-400/5 px-2 py-0.5 sm:px-2.5 sm:py-1">
                      <div className="flex items-center gap-1 text-[9px] uppercase tracking-wider text-amber-400/90">
                        <FileText className="size-3 shrink-0" aria-hidden />
                        Latest Building Transaction
                      </div>
                      <div className="mt-0.5 text-sm font-medium text-amber-200">
                        {ukLandRegistry.latest_building_transaction != null && ukLandRegistry.latest_building_transaction.price > 0
                          ? `${formatCurrency(ukLandRegistry.latest_building_transaction.price, currencySymbol)}${ukLandRegistry.latest_building_transaction.date ? ` · ${formatSaleDate(ukLandRegistry.latest_building_transaction.date)}` : ""}`
                          : "No building transaction data available."}
                      </div>
                      {ukLandRegistry.latest_building_transaction?.property_type && (
                        <div className="mt-0.5 text-[10px] text-amber-400/70">{ukLandRegistry.latest_building_transaction.property_type}</div>
                      )}
                    </div>
                  </>
                )}
                <div className="rounded-lg border border-zinc-500/20 bg-zinc-500/5 px-2 py-0.5 sm:px-2.5 sm:py-1">
                  <div className="text-[9px] uppercase tracking-wider text-zinc-400/90">
                    {ukLandRegistry.area_data_source === "HPI"
                      ? "Area average price (HPI)"
                      : ukLandRegistry.area_fallback_level === "street"
                        ? "Street-level average"
                        : ukLandRegistry.area_fallback_level === "locality"
                          ? "Locality average"
                          : ukLandRegistry.area_fallback_level === "outward_postcode"
                            ? "Outward postcode average"
                            : ukLandRegistry.area_fallback_level === "postcode_area"
                              ? "Postcode area average"
                              : "Average Area Price"}
                  </div>
                  <div className="mt-0.5 text-sm font-medium text-zinc-300">
                    {ukLandRegistry.average_area_price != null && ukLandRegistry.average_area_price > 0
                      ? formatCurrency(ukLandRegistry.average_area_price, currencySymbol)
                      : "No area transaction data available."}
                  </div>
                  {ukLandRegistry.average_area_price != null && ukLandRegistry.average_area_price > 0 && (
                    <div className="mt-0.5 text-[10px] text-zinc-500">
                      {ukLandRegistry.area_data_source === "HPI" ? (
                        <>
                          UK House Price Index · Local authority level
                          {ukLandRegistry.price_trend && (
                            <span className="ml-1">
                              · {ukLandRegistry.price_trend.change_1y_percent >= 0 ? "+" : ""}{ukLandRegistry.price_trend.change_1y_percent}% YoY
                            </span>
                          )}
                        </>
                      ) : (
                        <>
                          {ukLandRegistry.area_transaction_count} transactions (last 5 years)
                          {ukLandRegistry.area_fallback_level && ukLandRegistry.area_fallback_level !== "postcode" && ukLandRegistry.area_fallback_level !== "none" && (
                            <span> · {ukLandRegistry.area_fallback_level === "street" ? "Street-level" : ukLandRegistry.area_fallback_level === "locality" ? "Locality" : ukLandRegistry.area_fallback_level === "outward_postcode" ? "Outward postcode" : "Postcode area"} fallback</span>
                          )}
                        </>
                      )}
                    </div>
                  )}
                </div>
                {(ukLandRegistry.has_building_match === false) && (
                  <div className="rounded-lg border border-amber-400/20 bg-amber-400/5 px-2 py-0.5 sm:px-2.5 sm:py-1">
                    <div className="flex items-center gap-1 text-[9px] uppercase tracking-wider text-amber-400/90">
                      <FileText className="size-3 shrink-0" aria-hidden />
                      Latest nearby transaction
                    </div>
                    <div className="mt-0.5 text-sm font-medium text-amber-200">
                      {ukLandRegistry.latest_nearby_transaction != null && ukLandRegistry.latest_nearby_transaction.price > 0
                        ? `${formatCurrency(ukLandRegistry.latest_nearby_transaction.price, currencySymbol)}${ukLandRegistry.latest_nearby_transaction.date ? ` · ${formatSaleDate(ukLandRegistry.latest_nearby_transaction.date)}` : ""}`
                        : "No nearby transaction data available."}
                    </div>
                    {ukLandRegistry.latest_nearby_transaction?.property_type && (
                      <div className="mt-0.5 text-[10px] text-amber-400/70">{ukLandRegistry.latest_nearby_transaction.property_type}</div>
                    )}
                  </div>
                )}
              </div>
              <div className="pt-0.5 text-[10px] text-zinc-500">
                {ukLandRegistry.area_data_source === "HPI"
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
                    {formatCurrency(estimate.estimated_value, currencySymbol)}
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
                    ? formatCurrency(latestBuildingAmount, currencySymbol)
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
