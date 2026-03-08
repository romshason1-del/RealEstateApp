"use client";

import * as React from "react";
import { X, TrendingUp, BadgeCheck, Sparkles } from "lucide-react";
import { HeartButton } from "@/components/heart-button";
import { calculatePropertyValue } from "@/lib/property-value";
import { useIsraelRealEstate } from "@/hooks/use-israel-real-estate";
import { formatSaleYear } from "@/lib/israel-real-estate";

export type PropertyValueCardProps = {
  address: string;
  position: { lat: number; lng: number };
  currencySymbol?: string;
  countryCode?: string;
  onClose: () => void;
  isSaved?: boolean;
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
  const { data: israelData, isLoading } = useIsraelRealEstate(address, isIsrael, mockData.areaSqm);

  const lastSalePrice = israelData?.lastSalePrice ?? null;
  const lastSaleDate = israelData?.lastSaleDate ?? null;
  const marketValue = israelData?.avgPrice ?? (israelData?.avgPricePerSqm != null ? Math.round(israelData.avgPricePerSqm * 100) : null);

  React.useEffect(() => {
    if (isIsrael && !isLoading && !lastSalePrice && !marketValue) {
      console.error("[PropertyValueCard] Cannot show 900k - REASON: lastSalePrice=", lastSalePrice, "marketValue=", marketValue, "error=", israelData?.error, "address=", address.slice(0, 50));
    }
  }, [isIsrael, isLoading, lastSalePrice, marketValue, israelData?.error, address]);

  const [mounted, setMounted] = React.useState(false);
  React.useEffect(() => {
    const id = requestAnimationFrame(() => setMounted(true));
    return () => cancelAnimationFrame(id);
  }, []);

  const mainPrice = lastSalePrice ?? marketValue ?? mockData.valueNumber;
  const showMainPrice = lastSalePrice != null || marketValue != null || !isIsrael;

  return (
    <div
      className={[
        "pointer-events-none absolute inset-x-4 bottom-4 z-10 flex justify-end transition-all duration-300 ease-out",
        mounted ? "translate-y-0 opacity-100" : "translate-y-4 opacity-0",
      ].join(" ")}
    >
      <div className="pointer-events-auto w-full max-w-[320px] rounded-2xl border border-amber-400/20 bg-black/85 p-4 shadow-2xl backdrop-blur-xl sm:max-w-[340px]">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="text-[10px] uppercase tracking-[0.2em] text-amber-400/90">Property Value</div>
            <div className="mt-1.5 truncate text-sm font-semibold text-white">{address}</div>
          </div>
          <div className="flex shrink-0 items-center gap-1.5">
            {onToggleSave && <HeartButton isSaved={isSaved} onToggle={onToggleSave} iconSize="size-3.5" />}
            <button type="button" onClick={onClose} aria-label="Close" className="rounded-full border border-white/10 p-1.5 text-zinc-400 transition-colors hover:border-white/20 hover:text-white">
              <X className="size-4" />
            </button>
          </div>
        </div>

        <div className="mt-4 rounded-xl border border-amber-400/15 bg-amber-400/5 px-4 py-3">
          {isLoading && isIsrael ? (
            <div className="text-sm text-amber-200/70">Loading…</div>
          ) : (
            <>
              <div className="text-[10px] uppercase tracking-[0.18em] text-amber-300/80">Last Deal on this Property</div>
              <div className="mt-1.5 flex flex-wrap items-baseline gap-2">
                <span className="text-2xl font-bold tracking-tight text-amber-400 sm:text-[1.75rem]">
                  {currencySymbol}{mainPrice.toLocaleString()}
                </span>
                {lastSalePrice != null && <span className="text-lg font-semibold text-zinc-400">({formatSaleYear(lastSaleDate)})</span>}
              </div>

              {marketValue != null && isIsrael && (
                <div className="mt-3 flex items-center gap-2 rounded-lg border border-violet-500/20 bg-violet-500/5 px-3 py-2">
                  <Sparkles className="size-4 shrink-0 text-violet-400" aria-hidden />
                  <div>
                    <div className="text-[10px] uppercase tracking-wider text-violet-400/90">Market Value</div>
                    <div className="text-base font-semibold text-violet-300">{currencySymbol}{marketValue.toLocaleString()}</div>
                  </div>
                </div>
              )}

              {!isIsrael && (
                <div className="mt-2 text-xs text-zinc-400">
                  {mockData.pricePerSqm.toLocaleString()} {currencySymbol}<span className="ml-1 text-zinc-500">/ sqm</span>
                  <span className="ml-2 text-emerald-400">↑ {mockData.trendYoY >= 0 ? "+" : ""}{mockData.trendYoY.toFixed(1)}% YoY</span>
                </div>
              )}

              <div className="mt-3 flex items-center gap-2">
                {showMainPrice ? (
                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-emerald-400">
                    <BadgeCheck className="size-3" aria-hidden /> Official Data
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 rounded-full border border-amber-400/30 bg-amber-400/10 px-2.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-amber-300">
                    <BadgeCheck className="size-3" aria-hidden /> Verified
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
