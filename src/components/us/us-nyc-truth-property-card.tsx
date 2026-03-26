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
  /** ACRIS cross-check (main route); display-only. */
  acris_last_sale_price?: number | null;
  acris_last_sale_date?: string | null;
  acris_has_multiple_deeds?: boolean;
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
  if (!Number.isFinite(value) || value <= 0) return "Unavailable";
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
      return "No property record at this location";
    case "property-level":
    default:
      return "Based on this property's transaction history";
  }
}

function saleIndicatesMultipleUnits(totalUnits: number | null | undefined): boolean {
  if (totalUnits == null || !Number.isFinite(totalUnits)) return false;
  return totalUnits > 1;
}

/** Normalize dates to YYYY-MM-DD for strict equality (truth vs ACRIS). */
function nycSaleDateKeyForCompare(d: string | null | undefined): string {
  if (d == null || !String(d).trim()) return "";
  const t = String(d).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(t)) return t.slice(0, 10);
  const ms = Date.parse(t);
  return Number.isFinite(ms) ? new Date(ms).toISOString().slice(0, 10) : "";
}

function nycPricesMatchForAcris(a: number | null | undefined, b: number | null | undefined): boolean {
  if (a == null || b == null || !Number.isFinite(a) || !Number.isFinite(b)) return false;
  return Math.round(a) === Math.round(b);
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
  "rounded-md border border-amber-500/20 bg-zinc-950/90 px-2 py-1.5 sm:px-2.5 sm:py-1.5 shadow-sm shadow-black/40";

const badgeBase =
  "inline-flex items-center rounded-full border px-1.5 py-[2px] text-[9px] font-medium leading-none tracking-tight";

const sectionLabel = "text-[7px] font-semibold uppercase tracking-[0.12em] text-amber-400/85 leading-none";

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
  const isPropertyLevel = valueLevel === "property-level";

  const showAcrisVerified =
    nycPricesMatchForAcris(data.latest_sale_price, data.acris_last_sale_price) &&
    nycSaleDateKeyForCompare(data.latest_sale_date) !== "" &&
    nycSaleDateKeyForCompare(data.latest_sale_date) === nycSaleDateKeyForCompare(data.acris_last_sale_date);
  const showAcrisMultipleDeedsLine = data.acris_has_multiple_deeds === true;

  const onAptKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      onApartmentSearch?.();
    }
  };

  return (
    <div className="space-y-1">
      <div className="rounded-lg border border-zinc-700/50 bg-zinc-950/95 px-2 py-1.5 sm:px-2.5">
        <div className="text-[10px] font-semibold leading-tight tracking-tight text-zinc-100">NYC property record</div>
        <div className="mt-1 flex flex-wrap gap-1">
          {isPropertyLevel ? (
            <span className={`${badgeBase} border-[#C6A85B]/50 bg-[#C6A85B]/12 text-[#C6A85B]`}>Property-level</span>
          ) : null}
          <span className={`${badgeBase} border-[#C6A85B]/28 bg-black/40 text-zinc-200`}>Official record</span>
          <span
            className={`${badgeBase} border-emerald-500/30 bg-emerald-500/[0.08] text-emerald-400/95`}
          >
            Conservative
          </span>
          {showAcrisVerified ? (
            <span
              className={`${badgeBase} border-emerald-500/25 bg-emerald-500/[0.06] text-emerald-400/85`}
            >
              ACRIS verified
            </span>
          ) : null}
        </div>
        {showAcrisMultipleDeedsLine ? (
          <div className="mt-0.5 text-[8px] leading-tight text-zinc-500">Multiple recorded deeds found</div>
        ) : null}
      </div>

      {apartmentFlowEnabled && showApartmentInput ? (
        <div className="rounded-md border border-amber-500/30 bg-black/55 px-2 py-1.5 sm:px-2.5">
          <div className="text-[10px] font-semibold leading-tight tracking-tight text-amber-100/95">What&apos;s your apartment number?</div>
          <p className="mt-0.5 text-[8px] leading-tight text-zinc-500">
            Use the official unit designator from NYC records. Results depend on what the pipeline returns for that unit.
          </p>
          <div className="mt-1.5 flex flex-wrap gap-1.5">
            <input
              type="text"
              value={apartmentDraft}
              onChange={(e) => onApartmentDraftChange?.(e.target.value)}
              onKeyDown={onAptKeyDown}
              placeholder="e.g. 4B"
              disabled={apartmentSearchInFlight}
              className="min-w-[5rem] flex-1 rounded border border-amber-500/25 bg-black/60 px-1.5 py-1 text-[10px] leading-tight text-zinc-100 placeholder:text-zinc-600 focus:border-amber-400/50 focus:outline-none focus:ring-1 focus:ring-amber-500/30 disabled:opacity-50"
              autoComplete="off"
            />
            <button
              type="button"
              disabled={apartmentSearchInFlight || !apartmentDraft.trim()}
              onClick={() => onApartmentSearch?.()}
              className="shrink-0 rounded border border-amber-500/40 bg-amber-500/15 px-2 py-1 text-[9px] font-semibold uppercase tracking-wide text-amber-100 hover:bg-amber-500/25 disabled:pointer-events-none disabled:opacity-40"
            >
              {apartmentSearchInFlight ? "…" : "Apply"}
            </button>
          </div>
        </div>
      ) : null}

      {apartmentFlowEnabled && !showApartmentInput && (submittedApartment ?? "").trim() ? (
        <div className="text-[8px] leading-tight text-zinc-500">
          Unit filter: <span className="font-medium text-amber-200/90">{(submittedApartment ?? "").trim()}</span>
        </div>
      ) : null}

      <div className={block}>
        <div className={sectionLabel}>Estimated value for this property</div>
        <div className="mt-0.5 text-base font-semibold leading-tight tracking-tight text-amber-100 sm:text-lg">
          {ev != null && ev > 0 ? formatCurrency(ev, currencySymbol) : "Unavailable"}
        </div>
        <div className="mt-0.5 text-[8px] leading-tight text-zinc-500">{nycEstimatedSubtitle(valueLevel)}</div>
      </div>

      <div className={block}>
        <div className={sectionLabel}>Last transaction</div>
        <div className="mt-0.5 text-[11px] font-medium leading-tight text-zinc-100">
          {price != null && price > 0 ? (
            <>
              {formatCurrency(price, currencySymbol)}
              {dateStr ? ` · ${dateStr}` : ""}
            </>
          ) : (
            "No official sale recorded"
          )}
        </div>
        {multiUnit ? (
          <div className="mt-1 border-t border-amber-500/10 pt-1 text-[8px] font-medium leading-tight text-emerald-400/90">
            Transaction includes multiple units
          </div>
        ) : null}
      </div>

      <div className={block}>
        <div className={sectionLabel}>Price per ft²</div>
        <div className="mt-0.5 text-[11px] font-medium leading-tight text-zinc-100">
          {ppsf != null && ppsf > 0 ? formatPricePerSqFt(ppsf, currencySymbol) : "Unavailable"}
        </div>
        <div className="mt-0.5 text-[8px] leading-tight text-zinc-500">Per square foot</div>
      </div>

      <div className={block}>
        <div className={sectionLabel}>Local market context</div>
        <div className="mt-0.5 text-[9px] leading-tight text-zinc-300">Neighborhood demand and trends are not shown here.</div>
        <div className="mt-0.5 text-[8px] leading-tight text-zinc-500">Broader market context will appear when available.</div>
      </div>

      {apartmentFlowEnabled ? (
        <button
          type="button"
          onClick={() => onCheckAnotherApartment?.()}
          disabled={apartmentSearchInFlight}
          className="w-full rounded-md border border-amber-500/40 bg-amber-500/[0.08] py-1.5 text-[10px] font-semibold leading-tight tracking-wide text-amber-100/95 transition-colors hover:bg-amber-500/15 disabled:pointer-events-none disabled:opacity-45"
        >
          Check another apartment
        </button>
      ) : null}
    </div>
  );
}
