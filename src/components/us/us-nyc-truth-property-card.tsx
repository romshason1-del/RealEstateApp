"use client";

import * as React from "react";

export type UsNycTruthCardData = {
  /** Parsed address from main API payload (display-only formatting here). */
  address?: { city?: string; street?: string; house_number?: string };
  estimated_value?: number | null;
  latest_sale_price?: number | null;
  latest_sale_date?: string | null;
  price_per_sqft?: number | null;
  /** From truth-table `total_units` on the matched sale row only. */
  latest_sale_total_units?: number | null;
  property_result?: { value_level?: string };
  /** When true, UI may show apartment re-query (set by US pipeline only; no client heuristics). */
  supports_apartment_requery?: boolean;
};

/** UI-only: natural US line; does not affect matching/normalization elsewhere. */
export function formatNycCardDisplayAddress(addr: UsNycTruthCardData["address"]): string {
  if (!addr || typeof addr !== "object") return "";
  let hn = String(addr.house_number ?? "").trim();
  let street = String(addr.street ?? "").trim();
  const cityRaw = String(addr.city ?? "").trim();

  hn = hn.replace(/,\s*$/, "").trim();
  street = street.replace(/^\s*,\s*/, "").trim();

  street = expandSimpleStreetSuffix(street);

  const city = cityRaw || "Brooklyn";
  const core = [hn, street].filter(Boolean).join(" ");
  if (!core) return "";
  return `${core}, ${city}, NY`;
}

function expandSimpleStreetSuffix(street: string): string {
  const s = street.trim();
  if (!s) return s;
  const pairs: [RegExp, string][] = [
    [/\bSt\.?\s*$/i, "Street"],
    [/\bAve\.?\s*$/i, "Avenue"],
    [/\bRd\.?\s*$/i, "Road"],
  ];
  for (const [re, rep] of pairs) {
    if (re.test(s)) return s.replace(re, rep).replace(/\s+/g, " ").trim();
  }
  return s;
}

function formatCurrency(value: number, symbol: string): string {
  if (!Number.isFinite(value)) return `${symbol}0`;
  return `${symbol}${Math.round(value).toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
}

function formatNycSaleDate(dateStr: string | null | undefined): string {
  if (!dateStr?.trim()) return "";
  try {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return dateStr.trim();
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  } catch {
    return dateStr.trim();
  }
}

function formatPricePerSqFt(value: number, symbol: string): string {
  if (!Number.isFinite(value) || value <= 0) return "—";
  const rounded = value >= 100 ? Math.round(value) : Math.round(value * 10) / 10;
  return `${symbol}${rounded.toLocaleString("en-US")}/sq ft`;
}

function nycEstimatedSubtitle(valueLevel: string | undefined): string {
  switch (valueLevel) {
    case "building-level":
      return "Based on building transaction history";
    case "street-level":
      return "Based on recent sales on this street";
    case "area-level":
      return "Based on area market data";
    case "no_match":
      return "Map location found; no property record";
    case "property-level":
    default:
      return "Based on exact property transaction history";
  }
}

function saleIndicatesMultipleUnits(totalUnits: number | null | undefined): boolean {
  if (totalUnits == null || !Number.isFinite(totalUnits)) return false;
  return totalUnits > 1;
}

function nycTopMetadataLine(valueLevel: string | undefined): string {
  switch (valueLevel) {
    case "building-level":
      return "Official record — building / multi-unit context";
    case "street-level":
      return "Official record — street-level match";
    case "area-level":
      return "Official record — area context";
    case "no_match":
      return "NYC lookup — no exact record";
    case "property-level":
    default:
      return "Official record — property level";
  }
}

export type UsNycTruthPropertyCardProps = {
  data: UsNycTruthCardData;
  currencySymbol: string;
  /** Apartment prompt + CTA only when API sets `supports_apartment_requery`. */
  apartmentFlowEnabled: boolean;
  showApartmentInput?: boolean;
  apartmentDraft?: string;
  onApartmentDraftChange?: (value: string) => void;
  onApartmentSearch?: () => void;
  apartmentSearchInFlight?: boolean;
  submittedApartment?: string;
  onCheckAnotherApartment?: () => void;
};

const block =
  "rounded-md border border-amber-500/20 bg-zinc-950/90 px-2.5 py-2 sm:px-3 sm:py-2.5 shadow-sm shadow-black/40";

/**
 * NYC gold-layer truth only — US-only styling; no France imports or shared card.
 */
export function UsNycTruthPropertyCard({
  data,
  currencySymbol,
  apartmentFlowEnabled,
  showApartmentInput = false,
  apartmentDraft = "",
  onApartmentDraftChange,
  onApartmentSearch,
  apartmentSearchInFlight = false,
  submittedApartment,
  onCheckAnotherApartment,
}: UsNycTruthPropertyCardProps) {
  const ev = data.estimated_value;
  const price = data.latest_sale_price;
  const dateStr = formatNycSaleDate(data.latest_sale_date ?? null);
  const ppsf = data.price_per_sqft;
  const valueLevel = data.property_result?.value_level;
  const multiUnit = saleIndicatesMultipleUnits(data.latest_sale_total_units ?? null);
  const topLine = nycTopMetadataLine(valueLevel);
  const showConfidenceGreen = valueLevel === "property-level";

  const onAptKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      onApartmentSearch?.();
    }
  };

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 border-b border-amber-500/15 pb-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="size-1.5 shrink-0 rounded-full bg-emerald-500/90" aria-hidden />
          <span className="text-[9px] font-semibold uppercase tracking-[0.16em] text-amber-500/80">{topLine}</span>
        </div>
        {showConfidenceGreen ? (
          <span className="text-[9px] font-medium text-emerald-400/90">High match confidence</span>
        ) : (
          <span className="text-[9px] font-medium text-zinc-500">Conservative disclosure</span>
        )}
      </div>

      {apartmentFlowEnabled && showApartmentInput ? (
        <div className="rounded-md border border-amber-500/30 bg-black/55 px-2.5 py-2 sm:px-3">
          <div className="text-[11px] font-semibold tracking-tight text-amber-100/95">What&apos;s your apartment number?</div>
          <p className="mt-1 text-[9px] leading-snug text-zinc-500">
            Use the official unit designator from NYC records. Results depend on what the pipeline returns for that unit.
          </p>
          <div className="mt-2 flex flex-wrap gap-2">
            <input
              type="text"
              value={apartmentDraft}
              onChange={(e) => onApartmentDraftChange?.(e.target.value)}
              onKeyDown={onAptKeyDown}
              placeholder="e.g. 4B"
              disabled={apartmentSearchInFlight}
              className="min-w-[6rem] flex-1 rounded border border-amber-500/25 bg-black/60 px-2 py-1.5 text-[11px] text-zinc-100 placeholder:text-zinc-600 focus:border-amber-400/50 focus:outline-none focus:ring-1 focus:ring-amber-500/30 disabled:opacity-50"
              autoComplete="off"
            />
            <button
              type="button"
              disabled={apartmentSearchInFlight || !apartmentDraft.trim()}
              onClick={() => onApartmentSearch?.()}
              className="shrink-0 rounded border border-amber-500/40 bg-amber-500/15 px-3 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-amber-100 hover:bg-amber-500/25 disabled:pointer-events-none disabled:opacity-40"
            >
              {apartmentSearchInFlight ? "…" : "Apply"}
            </button>
          </div>
        </div>
      ) : null}

      {apartmentFlowEnabled && !showApartmentInput && (submittedApartment ?? "").trim() ? (
        <div className="text-[9px] text-zinc-500">
          Unit filter: <span className="font-medium text-amber-200/90">{(submittedApartment ?? "").trim()}</span>
        </div>
      ) : null}

      <div className={block}>
        <div className="text-[8px] font-semibold uppercase tracking-[0.14em] text-amber-400/85">Estimated value for this property</div>
        <div className="mt-1 text-lg font-semibold tracking-tight text-amber-100 sm:text-xl">
          {ev != null && ev > 0 ? formatCurrency(ev, currencySymbol) : "—"}
        </div>
        <div className="mt-1 text-[9px] leading-snug text-zinc-500">{nycEstimatedSubtitle(valueLevel)}</div>
      </div>

      <div className={block}>
        <div className="text-[8px] font-semibold uppercase tracking-[0.14em] text-amber-400/85">Last transaction</div>
        <div className="mt-1 text-[12px] font-medium leading-snug text-zinc-100">
          {price != null && price > 0 ? (
            <>
              {formatCurrency(price, currencySymbol)}
              {dateStr ? ` · ${dateStr}` : ""}
            </>
          ) : (
            "No recorded transaction in truth data"
          )}
        </div>
        {multiUnit ? (
          <div className="mt-1.5 border-t border-amber-500/10 pt-1.5 text-[9px] font-medium text-emerald-400/90">
            Transaction includes multiple units
          </div>
        ) : null}
      </div>

      <div className={block}>
        <div className="text-[8px] font-semibold uppercase tracking-[0.14em] text-amber-400/85">Price per ft²</div>
        <div className="mt-1 text-[12px] font-medium text-zinc-100">
          {ppsf != null && ppsf > 0 ? formatPricePerSqFt(ppsf, currencySymbol) : "—"}
        </div>
        <div className="mt-1 text-[9px] text-zinc-500">Per square foot (matched property record)</div>
      </div>

      <div className={block}>
        <div className="text-[8px] font-semibold uppercase tracking-[0.14em] text-amber-400/85">Local market context</div>
        <div className="mt-1 text-[10px] leading-snug text-zinc-300">NYC — borough-level demand is not computed in this view.</div>
        <div className="mt-1 text-[9px] leading-snug text-zinc-500">Broader market indicators are omitted until wired from official feeds.</div>
      </div>

      <div className={block}>
        <div className="text-[8px] font-semibold uppercase tracking-[0.14em] text-amber-400/85">Source &amp; confidence</div>
        <div className="mt-1 text-[10px] leading-snug text-zinc-300">Official NYC gold-layer truth (address match to the NYC API truth table).</div>
        <div className="mt-1 flex flex-wrap items-center gap-2">
          <span className="text-[9px] leading-snug text-zinc-500">
            Figures reflect the matched row only; we do not extrapolate or estimate beyond that record.
          </span>
          {showConfidenceGreen ? (
            <span className="rounded border border-emerald-500/25 bg-emerald-500/10 px-1.5 py-0.5 text-[8px] font-semibold uppercase tracking-wider text-emerald-400/95">
              Verified row
            </span>
          ) : null}
        </div>
      </div>

      {apartmentFlowEnabled ? (
        <button
          type="button"
          onClick={() => onCheckAnotherApartment?.()}
          disabled={apartmentSearchInFlight}
          className="mt-1 w-full rounded-md border border-amber-500/40 bg-amber-500/[0.08] py-2.5 text-[11px] font-semibold tracking-wide text-amber-100/95 transition-colors hover:bg-amber-500/15 disabled:pointer-events-none disabled:opacity-45"
        >
          Check another apartment
        </button>
      ) : null}
    </div>
  );
}
