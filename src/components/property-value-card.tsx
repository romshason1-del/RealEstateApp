"use client";

import * as React from "react";
import { X, FileText, Sparkles, Building2, BadgeCheck, Bug } from "lucide-react";
import { HeartButton } from "@/components/heart-button";
import { calculatePropertyValue } from "@/lib/property-value";
import { usePropertyValueInsights } from "@/hooks/use-property-value-insights";
import { parseAddressFromFullString } from "@/lib/address-parse";
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

type DebugPanelProps = {
  address: string;
  parsed: { city: string; street: string; houseNumber: string };
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
    <div className="space-y-3 text-xs">
      <div className="flex items-center gap-2 text-amber-300/90">
        <Bug className="size-4 shrink-0" aria-hidden />
        <span className="font-medium uppercase tracking-wider">Debug Mode</span>
      </div>
      <div className="space-y-1.5 font-mono text-xs text-zinc-300">
        <div><span className="text-zinc-500">Input address:</span> {toEnglishDisplay(address)}</div>
        <div><span className="text-zinc-500">Parsed city:</span> {toEnglishDisplay(parsed.city)}</div>
        <div><span className="text-zinc-500">Parsed street:</span> {toEnglishDisplay(parsed.street)}</div>
        <div><span className="text-zinc-500">Parsed house number:</span> {toEnglishDisplay(parsed.houseNumber)}</div>
        <div><span className="text-zinc-500">Canonical city_key:</span> {apiCanon.city_key}</div>
        <div><span className="text-zinc-500">Canonical street_key:</span> {apiCanon.street_key}</div>
        <div><span className="text-zinc-500">Canonical house_key:</span> {apiCanon.house_key}</div>
        <div><span className="text-zinc-500">Records fetched:</span> {d?.records_fetched ?? "—"}</div>
        <div><span className="text-zinc-500">Records returned:</span> {d?.records_returned ?? "—"}</div>
        <div><span className="text-zinc-500">Candidate records:</span> {d?.records_after_filter ?? "—"}</div>
        {d?.api_status != null && (
          <div><span className="text-zinc-500">API status:</span> {d.api_status}</div>
        )}
        {d?.api_error && (
          <div><span className="text-zinc-500">API error:</span> <span className="text-amber-300/90">{d.api_error}</span></div>
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
        <div className="rounded border border-emerald-500/30 bg-emerald-500/5 p-2">
          <div className="text-[10px] uppercase tracking-wider text-emerald-400/90">Matched transaction</div>
          <div className="mt-1 space-y-0.5 text-zinc-300">
            <div>Date: {formatSaleDate(latest.transaction_date)}</div>
            <div>Price: {currencySymbol}{latest.transaction_price.toLocaleString()}</div>
            <div>Size: {latest.property_size} m²</div>
            <div>Price/m²: {currencySymbol}{latest.price_per_m2.toLocaleString()}</div>
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
  const mockData = React.useMemo(
    () => calculatePropertyValue(position.lat, position.lng, currencySymbol),
    [position.lat, position.lng, currencySymbol]
  );
  const { data: insightsData, isLoading } = usePropertyValueInsights(address, isIsrael, {
    latitude: position?.lat,
    longitude: position?.lng,
  });

  const [debugMode, setDebugMode] = React.useState(false);
  const hasMatch = insightsData?.address != null && (insightsData?.match_quality === "exact_building" || insightsData?.match_quality === "nearby_building");
  const isNearbyBuilding = insightsData?.match_quality === "nearby_building";
  const latest = insightsData?.latest_transaction;
  const estimate = insightsData?.current_estimated_value;
  const building = insightsData?.building_summary_last_3_years;

  const parsedLocal = React.useMemo(() => parseAddressFromFullString(address), [address]);
  const canonicalLocal = React.useMemo(
    () => toCanonicalAddress(parsedLocal.city, parsedLocal.street, parsedLocal.houseNumber),
    [parsedLocal]
  );

  const [mounted, setMounted] = React.useState(false);
  React.useEffect(() => {
    const id = requestAnimationFrame(() => setMounted(true));
    return () => cancelAnimationFrame(id);
  }, []);

  return (
    <div
      className={[
        "pointer-events-none absolute inset-x-4 bottom-4 z-10 flex justify-end transition-all duration-300 ease-out",
        mounted ? "translate-y-0 opacity-100" : "translate-y-4 opacity-0",
      ].join(" ")}
    >
      <div className="pointer-events-auto w-full max-w-[360px] rounded-2xl border border-amber-400/20 bg-black/85 p-4 shadow-2xl backdrop-blur-xl sm:max-w-[380px]">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="text-[10px] uppercase tracking-[0.2em] text-amber-400/90">Property Value</div>
            <div className="mt-1.5 truncate text-sm font-semibold text-white">{toEnglishDisplay(address)}</div>
          </div>
          <div className="flex shrink-0 items-center gap-1.5">
            {isIsrael && (
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

        <div className="mt-4 rounded-xl border border-amber-400/15 bg-amber-400/5 px-4 py-3">
          {debugMode && isIsrael ? (
            <DebugPanel
              address={address}
              parsed={parsedLocal}
              canonical={canonicalLocal}
              insightsData={insightsData}
              latest={latest}
              currencySymbol={currencySymbol}
            />
          ) : isLoading && isIsrael ? (
            <div className="text-sm text-amber-200/70">Loading official data…</div>
          ) : !isIsrael ? (
            <div className="space-y-2">
              <div className="text-[10px] uppercase tracking-[0.18em] text-amber-300/80">Estimated Value</div>
              <div className="text-2xl font-bold text-amber-400">{currencySymbol}{mockData.valueNumber.toLocaleString()}</div>
              <div className="text-xs text-zinc-400">
                {mockData.pricePerSqm.toLocaleString()} {currencySymbol}/ sqm
                <span className="ml-2 text-emerald-400">↑ {mockData.trendYoY >= 0 ? "+" : ""}{mockData.trendYoY.toFixed(1)}% YoY</span>
              </div>
            </div>
          ) : !hasMatch ? (
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-amber-300/90">
                <FileText className="size-4 shrink-0" aria-hidden />
                <span className="text-sm font-medium">No reliable official transaction found for this exact building</span>
              </div>
              <p className="text-xs text-zinc-400">
                We only show data when there is a high-confidence match for the exact address. Street-level or nearby data is not used.
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
            <div className="space-y-4">
              {/* 1. Latest Official Transaction */}
              <div>
                <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-[0.18em] text-amber-300/80">
                  <FileText className="size-3.5" aria-hidden />
                  Latest Official Transaction
                </div>
                {latest && (
                  <div className="mt-1.5 space-y-0.5 text-sm">
                    <div className="flex justify-between">
                      <span className="text-zinc-400">Date</span>
                      <span className="font-medium text-amber-200">{formatSaleDate(latest.transaction_date)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-400">Price</span>
                      <span className="font-semibold text-amber-400">{currencySymbol}{latest.transaction_price.toLocaleString()}</span>
                    </div>
                    {latest.property_size > 0 && (
                      <>
                        <div className="flex justify-between">
                          <span className="text-zinc-400">Size</span>
                          <span className="text-white">{latest.property_size} m²</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-zinc-400">Price/m²</span>
                          <span className="text-white">{currencySymbol}{latest.price_per_m2.toLocaleString()}</span>
                        </div>
                      </>
                    )}
                  </div>
                )}
              </div>

              {/* 2. Estimated Current Value */}
              {estimate && (
                <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 px-3 py-2">
                  <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-violet-400/90">
                    <Sparkles className="size-3.5" aria-hidden />
                    Estimated Current Value
                  </div>
                  <div className="mt-1 text-base font-semibold text-violet-300">
                    {currencySymbol}{estimate.estimated_value.toLocaleString()}
                  </div>
                  {estimate.estimated_price_per_m2 > 0 && (
                    <div className="text-xs text-violet-400/80">
                      {currencySymbol}{estimate.estimated_price_per_m2.toLocaleString()}/ m²
                    </div>
                  )}
                  <div className="mt-1 text-[10px] text-violet-400/70">
                    Estimate based on official transaction data
                  </div>
                </div>
              )}

              {/* 3. Building Activity (Last 3 Years) */}
              {building && building.transactions_count_last_3_years > 0 && (
                <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-3 py-2">
                  <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-emerald-400/90">
                    <Building2 className="size-3.5" aria-hidden />
                    Building Activity (Last 3 Years)
                  </div>
                  <div className="mt-1.5 space-y-0.5 text-sm">
                    <div className="flex justify-between">
                      <span className="text-zinc-400">Transactions</span>
                      <span className="font-medium text-emerald-300">{building.transactions_count_last_3_years}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-400">Latest building sale</span>
                      <span className="font-medium text-emerald-300">{currencySymbol}{building.latest_building_transaction_price.toLocaleString()}</span>
                    </div>
                    {building.average_apartment_value_today > 0 && (
                      <div className="flex justify-between">
                        <span className="text-zinc-400">Avg apartment value today</span>
                        <span className="font-medium text-emerald-300">{currencySymbol}{building.average_apartment_value_today.toLocaleString()}</span>
                      </div>
                    )}
                  </div>
                </div>
              )}

              <div className="flex items-center gap-2">
                {isNearbyBuilding ? (
                  <span className="inline-flex items-center gap-1 rounded-full border border-amber-500/30 bg-amber-500/10 px-2.5 py-0.5 text-[10px] font-medium text-amber-300">
                    Based on the closest verified transaction on this street.
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-emerald-400">
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
