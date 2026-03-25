"use client";

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

/**
 * NYC gold-layer truth only — isolated from France card logic and copy.
 */
export function UsNycTruthPropertyCard({ data, currencySymbol }: { data: UsNycTruthCardData; currencySymbol: string }) {
  const ev = data.estimated_value;
  const price = data.latest_sale_price;
  const dateStr = formatNycSaleDate(data.latest_sale_date ?? null);
  const ppsf = data.price_per_sqft;
  const valueLevel = data.property_result?.value_level;
  const multiUnit = saleIndicatesMultipleUnits(data.latest_sale_total_units ?? null);
  const displayLine = formatNycCardDisplayAddress(data.address);

  const sectionClass =
    "rounded-lg border border-violet-500/15 bg-zinc-950/60 px-2 py-1.5 sm:px-2.5 sm:py-2";

  return (
    <div className="space-y-1.5">
      {displayLine ? (
        <div className="rounded-lg border border-zinc-600/30 bg-zinc-900/40 px-2 py-1 sm:px-2.5 sm:py-1.5">
          <div className="text-[8px] uppercase tracking-wider text-zinc-500">Address</div>
          <div className="mt-0.5 text-[11px] font-medium leading-snug text-zinc-100">{displayLine}</div>
        </div>
      ) : null}
      <div className={sectionClass}>
        <div className="text-[8px] uppercase tracking-wider text-violet-400/90">Estimated value for this property</div>
        <div className="mt-0.5 text-sm font-semibold text-violet-200">
          {ev != null && ev > 0 ? formatCurrency(ev, currencySymbol) : "—"}
        </div>
        <div className="mt-0.5 text-[9px] text-zinc-500">{nycEstimatedSubtitle(valueLevel)}</div>
      </div>

      <div className={sectionClass}>
        <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Last transaction</div>
        <div className="mt-0.5 text-[11px] font-medium text-zinc-200">
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
          <div className="mt-1 text-[9px] text-amber-400/95">Transaction includes multiple units</div>
        ) : null}
      </div>

      <div className={sectionClass}>
        <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Price per ft²</div>
        <div className="mt-0.5 text-[11px] font-medium text-zinc-200">
          {ppsf != null && ppsf > 0 ? formatPricePerSqFt(ppsf, currencySymbol) : "—"}
        </div>
        <div className="mt-0.5 text-[9px] text-zinc-500">Per square foot (matched property record)</div>
      </div>

      <div className={sectionClass}>
        <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Local market context</div>
        <div className="mt-0.5 text-[10px] text-zinc-300">NYC — borough-level demand is not computed in this view.</div>
        <div className="mt-0.5 text-[9px] text-zinc-500">Broader market indicators are omitted until wired from official feeds.</div>
      </div>

      <div className={sectionClass}>
        <div className="text-[8px] uppercase tracking-wider text-zinc-400/90">Source & confidence</div>
        <div className="mt-0.5 text-[10px] text-zinc-300">Official NYC gold-layer truth (exact address match).</div>
        <div className="mt-0.5 text-[9px] text-zinc-500">Conservative: values reflect the matched NYC API truth row only.</div>
      </div>
    </div>
  );
}
