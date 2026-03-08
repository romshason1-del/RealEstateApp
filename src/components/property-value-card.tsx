"use client";

import * as React from "react";
import { X, TrendingUp, BadgeCheck } from "lucide-react";
import { HeartButton } from "@/components/heart-button";
import { calculatePropertyValue } from "@/lib/property-value";

export type PropertyValueCardProps = {
  /** Property address */
  address: string;
  /** Map coordinates for value calculation */
  position: { lat: number; lng: number };
  /** Currency symbol (default $) */
  currencySymbol?: string;
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
  onClose,
  isSaved = false,
  onToggleSave,
}: PropertyValueCardProps) {
  const data = React.useMemo(
    () => calculatePropertyValue(position.lat, position.lng, currencySymbol),
    [position.lat, position.lng, currencySymbol]
  );

  const [mounted, setMounted] = React.useState(false);
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

        {/* Estimated Market Value */}
        <div className="mt-4 rounded-xl border border-amber-400/15 bg-amber-400/5 px-4 py-3">
          <div className="text-[10px] uppercase tracking-[0.18em] text-amber-300/80">
            Estimated Market Value
          </div>
          <div className="mt-1.5 text-2xl font-bold tracking-tight text-amber-400 sm:text-[1.75rem]">
            {data.formattedValue}
          </div>

          {/* Price per Sqm */}
          <div className="mt-2 text-xs text-zinc-400">
            {data.currencySymbol}
            {data.pricePerSqm.toLocaleString()}
            <span className="ml-1 text-zinc-500">/ sqm</span>
          </div>

          {/* Badges row */}
          <div className="mt-3 flex flex-wrap items-center gap-2">
            {data.isOfficial ? (
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
            <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-0.5 text-[10px] font-medium text-emerald-400">
              <TrendingUp className="size-3" aria-hidden />
              {data.trendYoY >= 0 ? "+" : ""}
              {data.trendYoY.toFixed(1)}% YoY
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
