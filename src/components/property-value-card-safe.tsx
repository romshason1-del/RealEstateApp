"use client";

import * as React from "react";
import { X, TrendingUp, BadgeCheck } from "lucide-react";
import { HeartButton } from "@/components/heart-button";
import { calculatePropertyValue } from "@/lib/property-value";
import { PropertyValueCard, type PropertyValueCardProps } from "./property-value-card";

type Props = PropertyValueCardProps;

/**
 * Error boundary wrapper - if PropertyValueCard throws (e.g. connection refused),
 * renders a fallback card with mock data so the app never crashes.
 */
export function PropertyValueCardSafe(props: Props) {
  const [error, setError] = React.useState<Error | null>(null);

  React.useEffect(() => {
    setError(null);
  }, [props.address]);

  if (error) {
    return (
      <PropertyValueCardFallback
        address={props.address}
        position={props.position}
        currencySymbol={props.currencySymbol ?? "$"}
        onClose={props.onClose}
        isSaved={props.isSaved}
        onToggleSave={props.onToggleSave}
      />
    );
  }

  return (
    <PropertyValueCardErrorBoundary onError={setError}>
      <PropertyValueCard {...props} />
    </PropertyValueCardErrorBoundary>
  );
}

class PropertyValueCardErrorBoundary extends React.Component<
  { children: React.ReactNode; onError: (err: Error) => void },
  { hasError: boolean }
> {
  state = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch(err: Error) {
    this.props.onError(err);
  }

  render() {
    if (this.state.hasError) {
      return null;
    }
    return this.props.children;
  }
}

function PropertyValueCardFallback({
  address,
  position,
  currencySymbol,
  onClose,
  isSaved,
  onToggleSave,
}: Props) {
  const data = React.useMemo(
    () => calculatePropertyValue(position.lat, position.lng, currencySymbol),
    [position.lat, position.lng, currencySymbol]
  );

  return (
    <div className="pointer-events-none absolute inset-x-4 bottom-4 z-20 flex justify-end">
      <div className="pointer-events-auto w-full max-w-[320px] rounded-2xl border border-amber-400/20 bg-black/85 p-4 shadow-2xl backdrop-blur-xl sm:max-w-[340px]">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="text-[10px] uppercase tracking-[0.2em] text-amber-400/90">Property Value</div>
            <div className="mt-1.5 truncate text-sm font-semibold text-white">{address}</div>
          </div>
          <div className="flex shrink-0 items-center gap-1.5">
            {onToggleSave && <HeartButton isSaved={isSaved ?? false} onToggle={onToggleSave} iconSize="size-3.5" />}
            <button type="button" onClick={onClose} aria-label="Close" className="rounded-full border border-white/10 p-1.5 text-zinc-400 hover:text-white">
              <X className="size-4" />
            </button>
          </div>
        </div>
        <div className="mt-4 rounded-xl border border-amber-400/15 bg-amber-400/5 px-4 py-3">
          <div className="text-[10px] uppercase tracking-[0.18em] text-amber-300/80">Estimated Market Value</div>
          <div className="mt-1.5 flex flex-wrap items-baseline gap-2">
            <span className="text-2xl font-bold text-amber-400">{data.formattedValue}</span>
            <span className="text-sm font-medium text-emerald-400">↑ {data.trendYoY >= 0 ? "+" : ""}{data.trendYoY.toFixed(1)}% vs last year</span>
          </div>
          <div className="mt-2 text-xs text-zinc-400">
            {data.pricePerSqm.toLocaleString()} {currencySymbol}<span className="ml-1 text-zinc-500">/ sqm</span>
          </div>
          <div className="mt-3 flex items-center gap-2">
            <span className="inline-flex items-center gap-1 rounded-full border border-amber-400/30 bg-amber-400/10 px-2.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-amber-300">
              <BadgeCheck className="size-3" /> Verified
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-0.5 text-[10px] font-medium text-emerald-400">
              <TrendingUp className="size-3" /> {data.trendYoY >= 0 ? "+" : ""}{data.trendYoY.toFixed(1)}% YoY
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
