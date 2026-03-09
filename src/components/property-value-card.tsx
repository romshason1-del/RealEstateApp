"use client";

import * as React from "react";
import { X, FileText, Sparkles, Building2, BadgeCheck, Bug } from "lucide-react";
import { HeartButton } from "@/components/heart-button";
import { calculatePropertyValue } from "@/lib/property-value";
import { usePropertyValueInsights } from "@/hooks/use-property-value-insights";
import { parseAddressFromFullString, parseUSAddressFromFullString } from "@/lib/address-parse";
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

type ParsedAddress = { city: string; street: string; houseNumber: string; state?: string; zip?: string; country?: string };

type DebugPanelProps = {
  address: string;
  parsed: ParsedAddress;
  canonical: { cityKey: string; streetKey: string; houseKey: string };
  insightsData: {
    debug?: {
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
      market_value_source?: string;
      price_per_m2_used?: number;
      property_size_used?: number;
      transactions_count_5y?: number;
      latest_transaction_amount?: number;
      fallback_level_used?: string;
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
        {d?.market_value_source != null && (
          <div><span className="text-zinc-500">Market value source:</span> {String(d.market_value_source)}</div>
        )}
        {d?.price_per_m2_used != null && (
          <div><span className="text-zinc-500">Price per m² used:</span> {d.price_per_m2_used}</div>
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
        {d?.fallback_level_used != null && (
          <div><span className="text-zinc-500">Fallback level used:</span> {String(d.fallback_level_used)}</div>
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
  const hasOfficialProvider = isIsrael || isUS;
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
  const hasMatch = hasPropertyData && (insightsData?.match_quality === "exact_building" || insightsData?.match_quality === "nearby_building");
  const isNearbyBuilding = insightsData?.match_quality === "nearby_building";
  const latest = insightsData?.latest_transaction;
  const estimate = insightsData?.current_estimated_value;
  const building = insightsData?.building_summary_last_3_years;
  const transactions5y = building?.transactions_count_last_5_years ?? building?.transactions_count_last_3_years ?? 0;
  const latestBuildingAmount = building?.latest_building_transaction_price ?? latest?.transaction_price ?? 0;

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
        "pointer-events-none absolute inset-x-4 bottom-4 z-10 flex justify-end transition-all duration-300 ease-out",
        mounted ? "translate-y-0 opacity-100" : "translate-y-4 opacity-0",
      ].join(" ")}
    >
      <div className="pointer-events-auto flex max-h-[70vh] w-full max-w-[360px] flex-col overflow-hidden rounded-2xl border border-amber-400/20 bg-black/85 shadow-2xl backdrop-blur-xl sm:max-w-[380px]">
        <div className="flex shrink-0 items-start justify-between gap-2 p-3 sm:p-4">
          <div className="min-w-0 flex-1">
            <div className="text-[10px] uppercase tracking-[0.2em] text-amber-400/90">Property Value</div>
            <div className="mt-1 truncate text-sm font-semibold text-white sm:text-base">{toEnglishDisplay(address)}</div>
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

        <div className="min-h-0 flex-1 overflow-y-auto rounded-b-2xl border-t border-amber-400/15 bg-amber-400/5 px-3 py-2.5 sm:px-4 sm:py-3">
          {debugMode && hasOfficialProvider ? (
            <DebugPanel
              address={address}
              parsed={parsedLocal}
              canonical={canonicalLocal}
              insightsData={insightsData}
              latest={latest}
              currencySymbol={currencySymbol}
            />
          ) : isLoading && hasOfficialProvider ? (
            <div className="py-2 text-sm text-amber-200/70">Loading official data…</div>
          ) : !hasOfficialProvider ? (
            <div className="space-y-1.5">
              <div className="text-[10px] uppercase tracking-[0.18em] text-amber-300/80">Estimated Value</div>
              <div className="text-lg font-bold text-amber-400 sm:text-xl">{formatCurrency(mockData.valueNumber, currencySymbol)}</div>
              <div className="text-xs text-zinc-400">
                {mockData.pricePerSqm.toLocaleString()} {currencySymbol}/ sqm
                <span className="ml-2 text-emerald-400">↑ {mockData.trendYoY >= 0 ? "+" : ""}{mockData.trendYoY.toFixed(1)}% YoY</span>
              </div>
            </div>
          ) : !hasPropertyData ? (
            <div className="space-y-1.5">
              <div className="flex items-center gap-2 text-amber-300/90">
                <FileText className="size-3.5 shrink-0 sm:size-4" aria-hidden />
                <span className="text-xs font-medium sm:text-sm">No property data found for this address</span>
              </div>
              <p className="text-[11px] text-zinc-400 sm:text-xs">
                {isUS
                  ? "No property record could be retrieved for this address."
                  : "We only show data when there is a high-confidence match for the exact address."}
              </p>
              {insightsData?.debug && (
                <details className="mt-2 text-[10px] text-zinc-500">
                  <summary>Debug info</summary>
                  <pre className="mt-1 overflow-auto rounded bg-black/30 p-2">
                    {JSON.stringify(sanitizeForDisplay(insightsData.debug), null, 2)}
                  </pre>
                </details>
              )}
            </div>
          ) : (
            <div className="space-y-2.5 sm:space-y-3">
              {/* Row 1: Market Value */}
              {estimate && (
                <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 px-2.5 py-1.5 sm:px-3 sm:py-2">
                  <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-violet-400/90">
                    <Sparkles className="size-3.5" aria-hidden />
                    Market Value
                  </div>
                  <div className="mt-0.5 text-base font-semibold text-violet-300 sm:text-lg">
                    {formatCurrency(estimate.estimated_value, currencySymbol)}
                  </div>
                  <div className="mt-0.5 text-[10px] text-violet-400/70">
                    {estimateIsStreetValue
                      ? "Estimated from provider data"
                      : estimateIsRent
                        ? "Estimated monthly rent from property data"
                        : "Estimated from provider data"}
                  </div>
                </div>
              )}

              {/* Row 2: Transactions in Building (Last 5 Years) */}
              <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-2.5 py-1.5 sm:px-3 sm:py-2">
                <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-emerald-400/90">
                  <Building2 className="size-3.5" aria-hidden />
                  Transactions in Building (Last 5 Years)
                </div>
                <div className="mt-0.5 text-sm font-medium text-emerald-300">
                  {transactions5y > 0 ? transactions5y : "0"}
                </div>
              </div>

              {/* Row 3: Latest Building Transaction */}
              <div className="rounded-lg border border-amber-400/20 bg-amber-400/5 px-2.5 py-1.5 sm:px-3 sm:py-2">
                <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-amber-400/90">
                  <FileText className="size-3.5" aria-hidden />
                  Latest Building Transaction
                </div>
                <div className="mt-0.5 text-sm font-medium text-amber-200">
                  {latestBuildingAmount > 0
                    ? formatCurrency(latestBuildingAmount, currencySymbol)
                    : "No recent building transaction available"}
                </div>
                {latest?.transaction_date && (
                  <div className="mt-0.5 text-[10px] text-amber-400/70">
                    {formatSaleDate(latest.transaction_date)}
                  </div>
                )}
              </div>

              <div className="flex shrink-0 items-center gap-2 pt-0.5">
                {isNearbyBuilding ? (
                  <span className="inline-flex items-center gap-1 rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-300">
                    {latestBuildingAmount > 0
                      ? "Based on the closest verified transaction on this street."
                      : "Estimated from provider data"}
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-emerald-400">
                    <BadgeCheck className="size-3" aria-hidden /> Exact Building Match
                  </span>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
