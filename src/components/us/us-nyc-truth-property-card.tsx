"use client";

import * as React from "react";

export type UsNycTruthCardData = {
  /** Top-level `/api/property-value` status (e.g. requires_unit). */
  status?: string;
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
  /** Main NYC API: deterministic multi-unit prompt (no client heuristics). */
  should_prompt_for_unit?: boolean;
  unit_classification?: string;
  unit_prompt_reason?: string;
  /** ACRIS cross-check (main route); display-only. */
  acris_last_sale_price?: number | null;
  acris_last_sale_date?: string | null;
  acris_has_multiple_deeds?: boolean;
  /** NYC `us_nyc_app_output_final_v4` — server-derived; do not recompute on the client. */
  nyc_display_hierarchy?: "EXACT" | "BUILDING" | "STREET" | "NONE";
  nyc_match_confidence?: "HIGH" | "LOW" | "NONE" | "MEDIUM";
  nyc_has_exact_transaction?: boolean;
  nyc_show_street_reference?: boolean;
  nyc_street_reference?: {
    price: number | null;
    date: string | null;
    source_address: string | null;
  } | null;
  nyc_show_search_another_cta?: boolean;
  /** Server-normalized neighborhood score / label from BigQuery row. */
  nyc_neighborhood_score?: string | null;
  /** Server-normalized building category label. */
  nyc_building_type_display?: string | null;
  /** Full address string sent to `/api/us/nyc-app-output` (user’s intended lookup line). */
  nyc_searched_address_line?: string | null;
  /** BigQuery matched row (`property_address` or `lookup_address`) when present. */
  nyc_matched_record_address?: string | null;
  /** True when `/api/us/nyc-app-output` matched a BigQuery row (adapter). */
  nyc_bq_row_matched?: boolean;
  /** Server `final_display_mode` from NYC row (e.g. ASK_APARTMENT). */
  nyc_final_display_mode?: string | null;
  /**
   * When the BigQuery row includes an optional verified source unit column (see adapter), non-null means
   * apartment-level source is available for display. Otherwise the UI must not claim unit-specific data.
   */
  nyc_verified_source_unit_for_data?: string | null;
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

function nycSubtitleFromHierarchy(data: UsNycTruthCardData): string {
  const h = data.nyc_display_hierarchy;
  if (h) {
    switch (h) {
      case "EXACT":
        return "Based on exact property match";
      case "BUILDING":
        return "Based on building transaction history";
      case "STREET":
        return "Based on recent sales on this street";
      case "NONE":
        return "No property record at this location";
      default:
        break;
    }
  }
  return nycEstimatedSubtitle(data.property_result?.value_level);
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
  /** Address sent to `/api/property-value` (for debug logs on unit apply). */
  addressForFetch?: string;
  /** Optional; falls back to `data.status`. */
  status?: string;
  /** When true, commercial-only messaging may apply (US pipeline). */
  isCommercial?: boolean;
  /** Apartment prompt + CTA only when main API sets `should_prompt_for_unit`. */
  apartmentFlowEnabled: boolean;
  showApartmentInput?: boolean;
  apartmentDraft?: string;
  onApartmentDraftChange?: (value: string) => void;
  onApartmentSearch?: () => void;
  apartmentSearchInFlight?: boolean;
  submittedApartment?: string;
  onCheckAnotherApartment?: () => void;
  /** Closes the card so the user can pick another address (NONE / no-data CTA). */
  onSearchAnotherAddress?: () => void;
};

const block =
  "rounded-md border border-amber-500/20 bg-zinc-950/90 px-2 py-1.5 sm:px-2.5 sm:py-1.5 shadow-sm shadow-black/40";

const badgeBase =
  "inline-flex items-center rounded-full border px-1.5 py-[2px] text-[9px] font-medium leading-none tracking-tight";

const sectionLabel = "text-[7px] font-semibold uppercase tracking-[0.12em] text-amber-400/85 leading-none";

/** iOS Safari zooms inputs with font-size under 16px; keep 16px on small screens, compact on sm+. Exported for legacy NYC unit row in property-value-card. */
export const NYC_APT_INPUT_CLASS =
  "min-w-[5rem] flex-1 rounded border border-amber-500/25 bg-black/60 px-1.5 py-2 text-[16px] leading-snug text-zinc-100 placeholder:text-zinc-600 focus:border-amber-400/50 focus:outline-none focus:ring-1 focus:ring-amber-500/30 disabled:opacity-50 sm:py-1 sm:text-[10px] touch-manipulation";

/** Dispatched while NYC apartment/unit inputs are focused so the map skips resize/recenter (see address-explorer). */
const NYC_APT_INPUT_FOCUS_EVENT = "streetiq-nyc-apartment-input-focus";

function emitNycApartmentInputFocus(active: boolean) {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new CustomEvent(NYC_APT_INPUT_FOCUS_EVENT, { detail: { active } }));
}

/**
 * NYC gold-layer truth only — US-only styling; no France imports or shared card.
 */
export function UsNycTruthPropertyCard(props: UsNycTruthPropertyCardProps) {
  const {
    data,
    currencySymbol,
    addressForFetch,
    status: statusProp,
    isCommercial: isCommercialProp,
    apartmentFlowEnabled,
    showApartmentInput = false,
    apartmentDraft = "",
    onApartmentDraftChange,
    onApartmentSearch,
    apartmentSearchInFlight = false,
    submittedApartment,
    onCheckAnotherApartment,
    onSearchAnotherAddress,
  } = props;
  /** Server flags only — never treat as empty-state when a unit is still required for the row. */
  const askApartmentMode =
    String(data.nyc_final_display_mode ?? "")
      .toUpperCase()
      .trim() === "ASK_APARTMENT";
  const apartmentRequiredFromApi =
    data.should_prompt_for_unit === true ||
    (data as { nyc_pending_unit_prompt?: boolean | null }).nyc_pending_unit_prompt === true ||
    data.status === "requires_unit" ||
    (data as { requires_apartment_number?: boolean }).requires_apartment_number === true ||
    askApartmentMode;

  const hasSubmittedUnit = !!(submittedApartment ?? "").trim();
  /** No valuation/metrics until the user applies a unit (parent sets `submittedApartment` after successful Apply). */
  const isApartmentGatedBeforeUnit = apartmentRequiredFromApi && !hasSubmittedUnit;

  const verifiedSourceRaw = data.nyc_verified_source_unit_for_data;
  const hasVerifiedSourceUnitInTable =
    typeof verifiedSourceRaw === "string" && verifiedSourceRaw.trim() !== "";
  /** After Apply: always show A or B when a unit was submitted (full card path). */
  const showPostApplyUnitScope = hasSubmittedUnit;

  const effectiveStatus = statusProp ?? data.status;
  const ev = data.estimated_value;
  /** Full NYC card when API says success or omitted status but value row exists. */
  const treatAsSuccessWithData =
    !apartmentRequiredFromApi &&
    (effectiveStatus === "success" ||
      ((effectiveStatus == null || effectiveStatus === "") && ev != null));
  const requiresUnitOnly = effectiveStatus === "requires_unit" && !treatAsSuccessWithData;
  const commercialPropertyOnly =
    isCommercialProp === true || effectiveStatus === "commercial_property";
  const showCommercialOnlyMessage =
    !treatAsSuccessWithData &&
    (commercialPropertyOnly ||
      ((effectiveStatus == null || effectiveStatus === "") &&
        !!(submittedApartment ?? "").trim() &&
        !apartmentSearchInFlight));

  React.useEffect(() => {
    if (process.env.NODE_ENV === "production") return;
    console.log("[NYC_TRUTH_CARD_UI_DEBUG]", {
      status: data.status,
      statusProp,
      nyc_bq_row_matched: data.nyc_bq_row_matched,
      nyc_display_hierarchy: data.nyc_display_hierarchy,
      nyc_match_confidence: data.nyc_match_confidence,
      should_prompt_for_unit: data.should_prompt_for_unit,
      nyc_final_display_mode: data.nyc_final_display_mode,
      requires_apartment_number: (data as { requires_apartment_number?: boolean }).requires_apartment_number,
      apartmentRequiredFromApi,
      isApartmentGatedBeforeUnit,
      entersNoRecordEmptyState:
        !apartmentRequiredFromApi &&
        (data.nyc_display_hierarchy === "NONE" || data.nyc_show_search_another_cta === true) &&
        !(
          data.nyc_bq_row_matched === true &&
          (data.nyc_display_hierarchy === "BUILDING" ||
            data.nyc_display_hierarchy === "EXACT" ||
            data.nyc_display_hierarchy === "STREET")
        ),
      rendersApartmentSheet: apartmentFlowEnabled && showApartmentInput,
    });
  }, [
    apartmentFlowEnabled,
    apartmentRequiredFromApi,
    data,
    showApartmentInput,
    statusProp,
    submittedApartment,
    isApartmentGatedBeforeUnit,
  ]);

  const price = data.latest_sale_price;
  const dateStr = formatNycSaleDate(data.latest_sale_date ?? null);
  const ppsf = data.price_per_sqft;
  const valueLevel = data.property_result?.value_level;
  const multiUnit = saleIndicatesMultipleUnits(data.latest_sale_total_units ?? null);
  const isPropertyLevel = valueLevel === "property-level";

  const isV4Style =
    data.nyc_display_hierarchy !== undefined ||
    data.nyc_match_confidence !== undefined ||
    data.nyc_has_exact_transaction !== undefined;
  const showLastTxSection = isV4Style
    ? data.nyc_has_exact_transaction === true && data.nyc_display_hierarchy !== "NONE"
    : true;

  const showLegacyAcrisStripe = !isV4Style;

  const showAcrisVerified =
    showLegacyAcrisStripe &&
    nycPricesMatchForAcris(data.latest_sale_price, data.acris_last_sale_price) &&
    nycSaleDateKeyForCompare(data.latest_sale_date) !== "" &&
    nycSaleDateKeyForCompare(data.latest_sale_date) === nycSaleDateKeyForCompare(data.acris_last_sale_date);
  const showAcrisMultipleDeedsLine =
    showLegacyAcrisStripe &&
    data.acris_has_multiple_deeds === true &&
    (data.acris_last_sale_price != null || data.acris_last_sale_date != null);

  const onAptKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      const unitValue = apartmentDraft.trim();
      console.log("[UNIT_SUBMIT] unit value:", unitValue);
      console.log("[UNIT_FETCH_TRIGGERED] address:", addressForFetch ?? "", "unit:", unitValue);
      onApartmentSearch?.();
    }
  };

  const nycAptInputDebug = (phase: "focus" | "change", detail?: Record<string, unknown>) => {
    if (process.env.NODE_ENV === "production") return;
    console.log("[NYC_APT_DEBUG] apartment input", phase, {
      addressForFetch: addressForFetch ?? "",
      submittedApartment: submittedApartment ?? "",
      ...detail,
    });
  };

  const nycAptBlurTimerRef = React.useRef<number | null>(null);
  const onNycAptInputFocus = (gated: boolean) => {
    if (nycAptBlurTimerRef.current != null) {
      window.clearTimeout(nycAptBlurTimerRef.current);
      nycAptBlurTimerRef.current = null;
    }
    emitNycApartmentInputFocus(true);
    nycAptInputDebug("focus", { gated });
  };
  const onNycAptInputBlur = () => {
    nycAptBlurTimerRef.current = window.setTimeout(() => {
      nycAptBlurTimerRef.current = null;
      const el = document.activeElement;
      if (el?.getAttribute("data-nyc-apartment-input") === "1") return;
      emitNycApartmentInputFocus(false);
    }, 120);
  };

  React.useEffect(() => {
    return () => {
      if (nycAptBlurTimerRef.current != null) window.clearTimeout(nycAptBlurTimerRef.current);
      emitNycApartmentInputFocus(false);
    };
  }, []);

  if (showCommercialOnlyMessage) {
    return (
      <div className="space-y-1">
        <div className={block}>
          <p className="mt-0.5 text-[8px] leading-tight text-zinc-500">
            🏢 Commercial Property — No residential data available
          </p>
        </div>
      </div>
    );
  }

  if (
    !apartmentRequiredFromApi &&
    (data.nyc_display_hierarchy === "NONE" || data.nyc_show_search_another_cta === true) &&
    !(
      data.nyc_bq_row_matched === true &&
      (data.nyc_display_hierarchy === "BUILDING" ||
        data.nyc_display_hierarchy === "EXACT" ||
        data.nyc_display_hierarchy === "STREET")
    )
  ) {
    return (
      <div className="space-y-1">
        {data.nyc_searched_address_line ? (
          <div className="rounded-md border border-zinc-600/40 bg-black/40 px-2 py-1 text-[8px] leading-tight text-zinc-400">
            <span className="font-semibold text-zinc-500">You searched</span> {data.nyc_searched_address_line}
          </div>
        ) : null}
        {data.nyc_matched_record_address ? (
          <div className="rounded-md border border-zinc-600/40 bg-black/40 px-2 py-1 text-[8px] leading-tight text-zinc-400">
            <span className="font-semibold text-zinc-500">NYC record (if any)</span> {data.nyc_matched_record_address}
          </div>
        ) : null}
        <div className={block}>
          <div className={sectionLabel}>No NYC property record</div>
          <p className="mt-0.5 text-[9px] text-zinc-400">
            We couldn&apos;t match this address in our NYC records. It may be outside coverage or need a small correction.
          </p>
        </div>
        {onSearchAnotherAddress ? (
          <button
            type="button"
            onClick={onSearchAnotherAddress}
            className="w-full rounded-md border border-amber-500/40 bg-amber-500/[0.08] py-1.5 text-[10px] font-semibold leading-tight tracking-wide text-amber-100/95 transition-colors hover:bg-amber-500/15"
          >
            Search another address
          </button>
        ) : null}
      </div>
    );
  }

  if (isApartmentGatedBeforeUnit) {
    return (
      <div className="space-y-1">
        <div className={block}>
          <div className={sectionLabel}>Multi-unit property</div>
          <p className="mt-0.5 text-[9px] text-zinc-400">
            This building has multiple units. A unit or apartment number is required before we show valuation, sales, or
            other property details.
          </p>
        </div>
        {apartmentFlowEnabled && showApartmentInput ? (
          <div className="rounded-md border border-amber-500/30 bg-black/55 px-2 py-1.5 sm:px-2.5">
            <div className="text-[10px] font-semibold leading-tight tracking-tight text-amber-100/95">
              Enter apartment / unit number
            </div>
            <p className="mt-0.5 text-[8px] leading-tight text-zinc-500">
              Results appear after you enter a unit and tap Apply.
            </p>
            <div className="mt-1.5 flex flex-wrap gap-1.5">
              <input
                type="text"
                data-nyc-apartment-input="1"
                value={apartmentDraft}
                onChange={(e) => {
                  nycAptInputDebug("change", { draftLen: e.target.value.length });
                  onApartmentDraftChange?.(e.target.value);
                }}
                onFocus={() => onNycAptInputFocus(true)}
                onBlur={onNycAptInputBlur}
                onKeyDown={onAptKeyDown}
                placeholder="e.g. 4B"
                disabled={apartmentSearchInFlight}
                className={NYC_APT_INPUT_CLASS}
                autoComplete="off"
              />
              <button
                type="button"
                disabled={apartmentSearchInFlight || !apartmentDraft.trim()}
                onClick={() => {
                  const unitValue = apartmentDraft.trim();
                  console.log("[UNIT_SUBMIT] unit value:", unitValue);
                  console.log("[UNIT_FETCH_TRIGGERED] address:", addressForFetch ?? "", "unit:", unitValue);
                  onApartmentSearch?.();
                }}
                className="shrink-0 self-stretch rounded border border-amber-500/40 bg-amber-500/15 px-2 py-2 text-[9px] font-semibold uppercase tracking-wide text-amber-100 hover:bg-amber-500/25 disabled:pointer-events-none disabled:opacity-40 sm:py-1"
              >
                {apartmentSearchInFlight ? "…" : "Apply"}
              </button>
            </div>
            {apartmentSearchInFlight ? (
              <p className="mt-1.5 text-[8px] leading-tight text-zinc-500">Searching…</p>
            ) : null}
          </div>
        ) : (
          <div className={block}>
            <p className="mt-0.5 text-[8px] leading-tight text-zinc-500">
              A unit number is required before results can be shown.
            </p>
          </div>
        )}
      </div>
    );
  }

  const showMatchedLine =
    (data.nyc_matched_record_address ?? "").trim() !== "" &&
    (data.nyc_searched_address_line ?? "").trim().toLowerCase() !==
      (data.nyc_matched_record_address ?? "").trim().toLowerCase();

  return (
    <div className="space-y-1">
      {data.nyc_searched_address_line ? (
        <div className="rounded-md border border-zinc-600/40 bg-black/40 px-2 py-1 text-[8px] leading-tight text-zinc-400">
          <span className="font-semibold text-zinc-500">You searched</span> {data.nyc_searched_address_line}
        </div>
      ) : null}
      {showMatchedLine ? (
        <div className="rounded-md border border-zinc-600/40 bg-black/40 px-2 py-1 text-[8px] leading-tight text-zinc-400">
          <span className="font-semibold text-zinc-500">Matched NYC record</span> {data.nyc_matched_record_address}
        </div>
      ) : null}
      {showPostApplyUnitScope ? (
        <div className="rounded-md border border-amber-500/30 bg-amber-500/[0.06] px-2 py-1 text-[8px] leading-tight text-zinc-300">
          <p>
            <span className="font-semibold text-zinc-500">Unit you entered</span> {(submittedApartment ?? "").trim()}
          </p>
          {hasVerifiedSourceUnitInTable ? (
            <>
              <p className="mt-0.5">
                <span className="font-semibold text-zinc-500">Data shown from apartment/unit</span>{" "}
                {String(verifiedSourceRaw).trim()}
              </p>
              <p className="mt-0.5">
                <span className="font-semibold text-zinc-500">Matched NYC record</span>{" "}
                {(data.nyc_matched_record_address ?? "").trim() || "—"}
              </p>
            </>
          ) : (
            <>
              <p className="mt-0.5 text-zinc-400">
                No verified source apartment is available in the final NYC table
              </p>
              <p className="mt-0.5">
                <span className="font-semibold text-zinc-500">Matched NYC record</span>{" "}
                {(data.nyc_matched_record_address ?? "").trim() || "—"}
              </p>
              <p className="mt-0.5 text-zinc-400">Showing building-level data only</p>
            </>
          )}
        </div>
      ) : null}
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
          <div className="text-[10px] font-semibold leading-tight tracking-tight text-amber-100/95">
            Enter apartment / unit number
          </div>
          <p className="mt-0.5 text-[8px] leading-tight text-zinc-500">
            Enter the official unit or lot designator. Unit-specific valuation is not applied until the backend supports it.
          </p>
          <div className="mt-1.5 flex flex-wrap gap-1.5">
            <input
              type="text"
              data-nyc-apartment-input="1"
              value={apartmentDraft}
              onChange={(e) => {
                nycAptInputDebug("change", { draftLen: e.target.value.length });
                onApartmentDraftChange?.(e.target.value);
              }}
              onFocus={() => onNycAptInputFocus(false)}
              onBlur={onNycAptInputBlur}
              onKeyDown={onAptKeyDown}
              placeholder="e.g. 4B"
              disabled={apartmentSearchInFlight}
              className={NYC_APT_INPUT_CLASS}
              autoComplete="off"
            />
            <button
              type="button"
              disabled={apartmentSearchInFlight || !apartmentDraft.trim()}
              onClick={() => {
                const unitValue = apartmentDraft.trim();
                console.log("[UNIT_SUBMIT] unit value:", unitValue);
                console.log("[UNIT_FETCH_TRIGGERED] address:", addressForFetch ?? "", "unit:", unitValue);
                onApartmentSearch?.();
              }}
              className="shrink-0 self-stretch rounded border border-amber-500/40 bg-amber-500/15 px-2 py-2 text-[9px] font-semibold uppercase tracking-wide text-amber-100 hover:bg-amber-500/25 disabled:pointer-events-none disabled:opacity-40 sm:py-1"
            >
              {apartmentSearchInFlight ? "…" : "Apply"}
            </button>
          </div>
          {apartmentSearchInFlight ? (
            <p className="mt-1.5 text-[8px] leading-tight text-zinc-500">Searching…</p>
          ) : null}
        </div>
      ) : null}

      {apartmentFlowEnabled && !showApartmentInput && (submittedApartment ?? "").trim() ? (
        <div className="text-[8px] leading-tight text-zinc-500">
          Apartment / lot saved: <span className="font-medium text-amber-200/90">{(submittedApartment ?? "").trim()}</span>
        </div>
      ) : null}

      {requiresUnitOnly ? (
        <div className={block}>
          <p className="mt-0.5 text-[8px] leading-tight text-zinc-500">
            Please enter a unit number to see specific valuation and sales history
          </p>
        </div>
      ) : null}
      <>
          <div className={block}>
            <div className={sectionLabel}>Estimated value for this property</div>
            <div className="mt-0.5 text-base font-semibold leading-tight tracking-tight text-amber-100 sm:text-lg">
              {ev != null && ev > 0 ? formatCurrency(ev, currencySymbol) : "Unavailable"}
            </div>
            <div className="mt-0.5 text-[8px] leading-tight text-zinc-500">{nycSubtitleFromHierarchy(data)}</div>
          </div>

          {showLastTxSection ? (
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
          ) : null}

          {data.nyc_show_street_reference === true && data.nyc_street_reference ? (
            <div className={block}>
              <div className={sectionLabel}>Street-level reference</div>
              <p className="mt-0.5 text-[8px] leading-tight text-zinc-500">
                Not this property&apos;s last transaction — a comparable sale on the street (confidence is low).
              </p>
              {data.nyc_street_reference.source_address ? (
                <p className="mt-0.5 text-[9px] text-zinc-400">
                  Source address: {data.nyc_street_reference.source_address}
                </p>
              ) : null}
              <div className="mt-0.5 text-[11px] font-medium leading-tight text-zinc-100">
                {data.nyc_street_reference.price != null && data.nyc_street_reference.price > 0 ? (
                  <>
                    {formatCurrency(data.nyc_street_reference.price, currencySymbol)}
                    {data.nyc_street_reference.date
                      ? ` · ${formatNycSaleDate(data.nyc_street_reference.date)}`
                      : ""}
                  </>
                ) : data.nyc_street_reference.date ? (
                  formatNycSaleDate(data.nyc_street_reference.date)
                ) : (
                  "—"
                )}
              </div>
            </div>
          ) : null}

          {isV4Style ? (
            <div className={block}>
              <div className={sectionLabel}>Neighborhood score</div>
              <div className="mt-0.5 text-[11px] font-medium leading-tight text-zinc-100">
                {data.nyc_neighborhood_score != null && String(data.nyc_neighborhood_score).trim() !== ""
                  ? String(data.nyc_neighborhood_score)
                  : "Unavailable"}
              </div>
            </div>
          ) : (
            <div className={block}>
              <div className={sectionLabel}>Local market context</div>
              <div className="mt-0.5 text-[9px] leading-tight text-zinc-300">
                Neighborhood demand and trends are not shown here.
              </div>
              <div className="mt-0.5 text-[8px] leading-tight text-zinc-500">
                Broader market context will appear when available.
              </div>
            </div>
          )}

          <div className={block}>
            <div className={sectionLabel}>Price per ft²</div>
            <div className="mt-0.5 text-[11px] font-medium leading-tight text-zinc-100">
              {ppsf != null && ppsf > 0 ? formatPricePerSqFt(ppsf, currencySymbol) : "Unavailable"}
            </div>
            <div className="mt-0.5 text-[8px] leading-tight text-zinc-500">Per square foot</div>
          </div>

          {isV4Style ? (
            <div className={block}>
              <div className={sectionLabel}>Building type</div>
              <div className="mt-0.5 text-[11px] font-medium leading-tight text-zinc-100">
                {data.nyc_building_type_display != null && String(data.nyc_building_type_display).trim() !== ""
                  ? String(data.nyc_building_type_display)
                  : "Unavailable"}
              </div>
            </div>
          ) : null}
        </>

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
