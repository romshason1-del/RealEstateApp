"use client";

import * as React from "react";
import { X, TrendingUp, BadgeCheck } from "lucide-react";
import { HeartButton } from "@/components/heart-button";
import { calculatePropertyValue } from "@/lib/property-value";
import { useIsraelRealEstate } from "@/hooks/use-israel-real-estate";
import { formatSaleYear } from "@/lib/israel-real-estate";

const SQM_MIN = 20;
const SQM_MAX = 400;
const SQM_STEP = 5;

export type PropertyValueCardProps = {
  /** Property address */
  address: string;
  /** Map coordinates for value calculation */
  position: { lat: number; lng: number };
  /** Currency symbol (default $) */
  currencySymbol?: string;
  /** Country code - IL triggers real Israel Tax Authority data fetch */
  countryCode?: string;
  /** Called when user closes the card */
  onClose: () => void;
  /** Whether the property is saved to favorites */
  isSaved?: boolean;
  /** Toggle save callback */
  onToggleSave?: () => void;
};

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
  const { data: israelData, isLoading: israelLoading } = useIsraelRealEstate(address, isIsrael, mockData.areaSqm);

  const hasRealData = isIsrael && israelData && !israelData.error &&
    ((israelData.avgPrice != null && israelData.avgPrice > 0) || (israelData.lastSalePrice != null && israelData.lastSalePrice > 0));

  /** Street avg price per sqm - from API or mock */
  const pricePerSqm = React.useMemo(() => {
    if (hasRealData && israelData!.avgPricePerSqm != null && israelData!.avgPricePerSqm > 0) {
      return israelData!.avgPricePerSqm;
    }
    if (hasRealData && israelData!.avgPrice != null && israelData!.avgPrice > 0 && mockData.areaSqm > 0) {
      return Math.round(israelData!.avgPrice / mockData.areaSqm);
    }
    return mockData.pricePerSqm;
  }, [hasRealData, israelData, mockData.pricePerSqm, mockData.areaSqm]);

  const [mounted, setMounted] = React.useState(false);
  const [manualSqm, setManualSqm] = React.useState<number | null>(null);
  const [isPremium, setIsPremium] = React.useState(false);

  const effectiveSqm = manualSqm ?? mockData.areaSqm;
  const conditionMultiplier = isPremium ? 1.2 : 1;
  const tunerPrice = pricePerSqm * effectiveSqm * conditionMultiplier;
  const rawPrice = hasRealData || pricePerSqm > 0
    ? tunerPrice
    : mockData.valueNumber;
  const displayPrice = Number.isFinite(rawPrice) && rawPrice > 0 ? rawPrice : mockData.valueNumber;
  const formattedValue = `${Math.round(displayPrice).toLocaleString()} ${currencySymbol}`.trim();
  const lastSaleDate = hasRealData ? israelData!.lastSaleDate : null;
  const isCityFallback = hasRealData && israelData!.isCityFallback;

  React.useEffect(() => {
    setManualSqm(null);
    setIsPremium(false);
  }, [address]);
  React.useEffect(() => {
    const id = requestAnimationFrame(() => setMounted(true));
    return () => cancelAnimationFrame(id);
  }, []);

  const show = mounted;

  return (
    <div
      className={[
        "pointer-events-none absolute inset-x-4 bottom-4 z-10 flex justify-end transition-all duration-300 ease-out",
        show ? "translate-y-0 opacity-100" : "translate-y-4 opacity-0",
      ].join(" ")}
    >
      <div className="pointer-events-auto w-full max-w-[320px] rounded-2xl border border-amber-400/20 bg-black/85 p-4 shadow-2xl backdrop-blur-xl sm:max-w-[340px]">
        {/* Header */}
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="text-[10px] uppercase tracking-[0.2em] text-amber-400/90">
              Property Value
            </div>
            <div className="mt-1.5 truncate text-sm font-semibold text-white">
              {address}
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-1.5">
            {onToggleSave && (
              <HeartButton isSaved={isSaved} onToggle={onToggleSave} iconSize="size-3.5" />
            )}
            <button
              type="button"
              onClick={onClose}
              aria-label="Close"
              className="rounded-full border border-white/10 p-1.5 text-zinc-400 transition-colors hover:border-white/20 hover:text-white"
            >
              <X className="size-4" />
            </button>
          </div>
        </div>

        {/* Property Value - Two rows */}
        <div className="mt-4 rounded-xl border border-amber-400/15 bg-amber-400/5 px-4 py-3">
          {israelLoading && isIsrael ? (
            <div className="text-sm text-amber-200/70">Loading government data…</div>
          ) : (
            <>
              {/* Row 1 (main): Estimated Market Value - large */}
              <div className="text-[10px] uppercase tracking-[0.18em] text-amber-300/80">
                Estimated Market Value
              </div>
              <div className="mt-1.5 flex flex-wrap items-baseline gap-2">
                <span className="text-2xl font-bold tracking-tight text-amber-400 sm:text-[1.75rem]">
                  {formattedValue}
                </span>
                {!hasRealData && (
                  <span className="text-sm font-medium text-emerald-400">
                    ↑ {mockData.trendYoY >= 0 ? "+" : ""}{mockData.trendYoY.toFixed(1)}% vs last year
                  </span>
                )}
              </div>

              {/* Row 2 (secondary): Last Official Sale - smaller */}
              {hasRealData && israelData!.lastSalePrice != null && israelData!.lastSalePrice > 0 && (
                <div className="mt-2 text-xs text-zinc-400">
                  Last Official Sale: {currencySymbol}{israelData!.lastSalePrice.toLocaleString()} in {formatSaleYear(lastSaleDate)}
                  {isCityFallback && " (city average)"}
                </div>
              )}

              {hasRealData && (israelData!.transactionCount ?? 0) > 0 && (
                <div className="mt-1.5 text-[11px] text-zinc-500">
                  Based on {israelData!.transactionCount} recent transaction{(israelData!.transactionCount ?? 0) !== 1 ? "s" : ""} from the Tax Authority
                </div>
              )}

              {hasRealData && (israelData!.lastSaleOlderThan2Years || (israelData!.transactionCount ?? 0) > 0) && (
                <div className="mt-2 text-[10px] text-zinc-500 italic">
                  Estimate based on neighborhood trends
                </div>
              )}

              {/* Price Tuner */}
              <div className="mt-3 space-y-2 rounded-lg border border-white/5 bg-black/30 px-3 py-2">
                <div className="text-[10px] uppercase tracking-wider text-zinc-500">Price Tuner</div>
                <div className="flex items-center gap-3">
                  <label className="flex flex-1 items-center gap-2 text-xs">
                    <span className="shrink-0 text-zinc-400">Sqm</span>
                    <input
                      type="number"
                      min={SQM_MIN}
                      max={SQM_MAX}
                      step={SQM_STEP}
                      value={effectiveSqm}
                      onChange={(e) => {
                        const v = parseFloat(e.target.value);
                        setManualSqm(Number.isFinite(v) ? Math.max(SQM_MIN, Math.min(SQM_MAX, v)) : null);
                      }}
                      className="w-16 rounded border border-white/10 bg-white/5 px-2 py-1 text-right text-white focus:border-amber-400/50 focus:outline-none"
                    />
                  </label>
                  <label className="flex cursor-pointer items-center gap-2 text-xs text-zinc-400">
                    <input
                      type="checkbox"
                      checked={isPremium}
                      onChange={(e) => setIsPremium(e.target.checked)}
                      className="rounded border-white/20 bg-white/5 text-amber-500 focus:ring-amber-400/50"
                    />
                    <span>Premium/Renovated (+20%)</span>
                  </label>
                </div>
              </div>

              {!hasRealData && (
                <div className="mt-2 text-xs text-zinc-400">
                  {mockData.pricePerSqm.toLocaleString()} {currencySymbol}
                  <span className="ml-1 text-zinc-500">/ sqm</span>
                </div>
              )}

              <div className="mt-3 flex flex-wrap items-center gap-2">
                {hasRealData ? (
                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-emerald-400">
                    <BadgeCheck className="size-3" aria-hidden />
                    Official Data
                  </span>
                ) : mockData.isOfficial ? (
                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-emerald-400">
                    <BadgeCheck className="size-3" aria-hidden />
                    Official Data
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 rounded-full border border-amber-400/30 bg-amber-400/10 px-2.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-amber-300">
                    <BadgeCheck className="size-3" aria-hidden />
                    Verified
                  </span>
                )}
                {!hasRealData && (
                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-0.5 text-[10px] font-medium text-emerald-400">
                    <TrendingUp className="size-3" aria-hidden />
                    {mockData.trendYoY >= 0 ? "+" : ""}{mockData.trendYoY.toFixed(1)}% YoY
                  </span>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
