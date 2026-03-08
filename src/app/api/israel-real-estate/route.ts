import { NextRequest, NextResponse } from "next/server";

const DATA_GOV_IL_BASE = "https://data.gov.il/api/3/action";
const NADLAN_RESOURCE_ID = "ad6680ef-5d46-4654-be8d-7301292a8e48";

/** Hebrew field names from Israel Tax Authority real estate dataset (מאגר עסקאות הנדל"ן) */
const SALE_PRICE_FIELDS = ["מחיר העסקה", "מחיר עסקה", "מחיר_העסקה", "מחיר_עסקה", "sale_price", "price"];
const SALE_DATE_FIELDS = ["תאריך העסקה", "תאריך עסקה", "תאריך_העסקה", "תאריך_עסקה", "DEALDATE", "sale_date", "date"];
const AREA_FIELDS = ["שטח", "שטח במ\"ר", "מ\"ר", "area", "sqm", "שטח_במ\"ר"];
const GUSH_FIELDS = ["גוש", "GUSH", "gush"];
const PARCEL_FIELDS = ["חלקה", "PARCEL", "parcel"];

function getFieldValue(record: Record<string, unknown>, fieldList: string[]): unknown {
  for (const f of fieldList) {
    const val = record[f];
    if (val != null && val !== "") return val;
  }
  for (const [key, val] of Object.entries(record)) {
    if (val != null && val !== "" && key.includes("מחיר") && key.includes("עסקה")) return val;
  }
  for (const [key, val] of Object.entries(record)) {
    if (val != null && val !== "" && key.includes("תאריך") && key.includes("עסקה")) return val;
  }
  return null;
}

function hasGushAndParcel(record: Record<string, unknown>): boolean {
  const gush = getFieldValue(record, GUSH_FIELDS);
  const parcel = getFieldValue(record, PARCEL_FIELDS);
  if (gush == null || gush === "" || parcel == null || parcel === "") return false;
  const gushStr = String(gush).trim();
  const parcelStr = String(parcel).trim();
  return gushStr.length > 0 && parcelStr.length > 0;
}

function getArea(record: Record<string, unknown>): number {
  for (const f of AREA_FIELDS) {
    const val = record[f];
    if (val != null && val !== "") return parseNumeric(val);
  }
  for (const [key, val] of Object.entries(record)) {
    if (val != null && val !== "" && (key.includes("שטח") || key.includes("area") || key.includes("sqm"))) return parseNumeric(val);
  }
  return 0;
}

function parseNumeric(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val)) return val;
  const s = String(val ?? "").replace(/[^\d.]/g, "");
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : 0;
}

/**
 * Normalize address for government DB search.
 * Removes St, Street, Ave, Avenue, רחוב etc. so Google format matches gov DB.
 */
function normalizeForSearch(text: string): string {
  return text
    .replace(/\b(St|Street|Str)\b\.?/gi, " ")
    .replace(/\b(Ave|Avenue|Av)\b\.?/gi, " ")
    .replace(/\b(Rd|Road)\b\.?/gi, " ")
    .replace(/\b(Blvd|Boulevard)\b\.?/gi, " ")
    .replace(/\b(Dr|Drive)\b\.?/gi, " ")
    .replace(/\b(Ln|Lane)\b\.?/gi, " ")
    .replace(/^\s*רחוב\s+/i, " ")
    .replace(/\s+רחוב\s+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Parse street and city from address string.
 * Handles formats like "123 Dizengoff St, Tel Aviv, Israel" or "רחוב דיזנגוף 123, תל אביב"
 * Strips "Israel" from end for better API search.
 */
function parseAddressParts(address: string): { street: string; city: string } {
  const trimmed = address.trim();
  let parts = trimmed.split(/[,،]/).map((p) => p.trim()).filter(Boolean);

  if (parts[parts.length - 1]?.match(/^(Israel|ישראל)$/i)) {
    parts = parts.slice(0, -1);
  }

  if (parts.length >= 2) {
    const city = normalizeForSearch(parts[parts.length - 1]);
    const street = normalizeForSearch(parts.slice(0, -1).join(" "));
    return { street, city };
  }
  if (parts.length === 1) {
    const cleaned = normalizeForSearch(parts[0]);
    return { street: cleaned, city: cleaned };
  }
  return { street: "", city: "" };
}

export async function GET(request: NextRequest) {
  console.log("[israel-real-estate] GET request received");
  const { searchParams } = new URL(request.url);
  const address = searchParams.get("address") ?? "";
  const street = searchParams.get("street") ?? "";
  const city = searchParams.get("city") ?? "";
  const propertyAreaSqm = parseFloat(searchParams.get("propertyAreaSqm") ?? "85");
  const limitParam = parseInt(searchParams.get("limit") ?? "50", 10);
  const limit = Number.isFinite(limitParam) ? Math.min(Math.max(limitParam, 1), 100) : 50;

  const now = new Date();
  const twoYearsAgo = new Date(now.getFullYear() - 2, now.getMonth(), now.getDate()).getTime();
  const twentyFourMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 24, now.getDate()).getTime();

  const { street: parsedStreet, city: parsedCity } = parseAddressParts(address);
  const searchStreet = street || parsedStreet;
  const searchCity = city || parsedCity;

  console.log("[israel-real-estate] Parsed:", { address, searchStreet, searchCity, limit, resourceId: NADLAN_RESOURCE_ID });

  if (!address.trim() && !searchStreet && !searchCity) {
    return NextResponse.json(
      { error: "Address, street, or city is required", transactions: [], avgPrice: null },
      { status: 400 }
    );
  }

  try {
    const searchTerms = [searchStreet, searchCity].filter(Boolean).join(" ");
    const cleanedAddress = address.replace(/\s*(Israel|ישראל)\s*$/i, "").trim();
    const q = searchTerms || cleanedAddress || "*";
    const sortParam = "תאריך_העסקה desc";

    const url = `${DATA_GOV_IL_BASE}/datastore_search?resource_id=${NADLAN_RESOURCE_ID}&q=${encodeURIComponent(q)}&limit=${limit}&sort=${encodeURIComponent(sortParam)}`;
    console.log("[israel-real-estate] Fetching data.gov.il:", { url: url.slice(0, 120) + "..." });
    const res = await fetch(url, {
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
      },
      signal: AbortSignal.timeout(15000),
    });

    if (!res.ok) {
      console.log("[israel-real-estate] data.gov.il responded:", res.status, res.statusText);
      return NextResponse.json({
        transactions: [],
        avgPrice: null,
        avgPricePerSqm: null,
        lastSaleDate: null,
        lastSalePrice: null,
        transactionCount: 0,
        source: "data.gov.il",
        error: `API returned ${res.status}`,
      });
    }

    const json = await res.json();
    if (json?.success === false) {
      const errMsg = json?.error?.message ?? "API request failed";
      return NextResponse.json({
        transactions: [],
        avgPrice: null,
        avgPricePerSqm: null,
        lastSaleDate: null,
        lastSalePrice: null,
        transactionCount: 0,
        source: "data.gov.il",
        error: errMsg,
      });
    }
    const records = json?.result?.records ?? [];
    console.log("[israel-real-estate] Records received:", records.length, "from data.gov.il");

    const withGushParcel = records.filter((r: Record<string, unknown>) => hasGushAndParcel(r));
    console.log("[israel-real-estate] After GUSH/PARCEL filter:", withGushParcel.length);

    const allTransactions = withGushParcel.map((r: Record<string, unknown>) => {
      const price = getFieldValue(r, SALE_PRICE_FIELDS);
      const date = getFieldValue(r, SALE_DATE_FIELDS);
      const area = getArea(r);
      return {
        price: parseNumeric(price),
        date: date != null ? String(date) : null,
        area,
      };
    }).filter((t: { price: number }) => t.price > 0);

    const sortedByDate = [...allTransactions].sort((a, b) => {
      const da = a.date ? new Date(a.date).getTime() : 0;
      const db = b.date ? new Date(b.date).getTime() : 0;
      return db - da;
    });

    const lastSale = sortedByDate[0];
    const lastSaleTimestamp = lastSale?.date ? new Date(lastSale.date).getTime() : 0;
    const lastSaleOlderThan2Years = lastSaleTimestamp > 0 && lastSaleTimestamp < twoYearsAgo;

    const inLast24Months = sortedByDate.filter((t: { date: string | null }) => {
      const ts = t.date ? new Date(t.date).getTime() : 0;
      return ts >= twentyFourMonthsAgo;
    });
    const top10Recent = inLast24Months.slice(0, 10);
    const withArea = top10Recent.filter((t: { area: number }) => t.area > 0);
    const avgPricePerSqmFrom24Mo = withArea.length > 0
      ? withArea.reduce((sum: number, t: { price: number; area: number }) => sum + t.price / t.area, 0) / withArea.length
      : top10Recent.length > 0
        ? top10Recent.reduce((sum: number, t: { price: number }) => sum + t.price, 0) / top10Recent.length / 85
        : null;
    const areaForEstimate = Number.isFinite(propertyAreaSqm) && propertyAreaSqm > 0 ? propertyAreaSqm : 85;
    const estimatedFrom24Mo = avgPricePerSqmFrom24Mo != null
      ? Math.round(avgPricePerSqmFrom24Mo * areaForEstimate)
      : null;

    const estimatedValue = lastSaleOlderThan2Years && estimatedFrom24Mo != null
      ? estimatedFrom24Mo
      : lastSale?.price ?? estimatedFrom24Mo;
    const avgPrice = estimatedValue ?? (sortedByDate.length > 0 ? Math.round(sortedByDate.reduce((s: number, t: { price: number }) => s + t.price, 0) / sortedByDate.length) : null);
    const avgPricePerSqm = avgPricePerSqmFrom24Mo;

    const transactions = sortedByDate.slice(0, 10).map((t: { price: number; date: string | null }) => ({ price: t.price, date: t.date }));

    if (transactions.length === 0 && searchCity) {
      console.log("[israel-real-estate] No street match, trying city fallback:", searchCity);
      const cityUrl = `${DATA_GOV_IL_BASE}/datastore_search?resource_id=${NADLAN_RESOURCE_ID}&q=${encodeURIComponent(searchCity)}&limit=${limit}&sort=${encodeURIComponent(sortParam)}`;
      const cityRes = await fetch(cityUrl, {
        headers: {
          Accept: "application/json",
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
        },
        signal: AbortSignal.timeout(15000),
      });

      if (cityRes.ok) {
        const cityJson = await cityRes.json();
        const cityRecords = cityJson?.result?.records ?? [];
        const cityWithGushParcel = cityRecords.filter((r: Record<string, unknown>) => hasGushAndParcel(r));
        const cityAll = cityWithGushParcel.map((r: Record<string, unknown>) => {
          const price = getFieldValue(r, SALE_PRICE_FIELDS);
          const date = getFieldValue(r, SALE_DATE_FIELDS);
          const area = getArea(r);
          return { price: parseNumeric(price), date: date != null ? String(date) : null, area };
        }).filter((t: { price: number }) => t.price > 0);

        const citySorted = [...cityAll].sort((a, b) => {
          const da = a.date ? new Date(a.date).getTime() : 0;
          const db = b.date ? new Date(b.date).getTime() : 0;
          return db - da;
        });
        const cityTop5 = citySorted.slice(0, 5);
        const cityWithArea = cityTop5.filter((t: { area: number }) => t.area > 0);
        const cityAvgPricePerSqm = cityWithArea.length > 0
          ? cityWithArea.reduce((sum: number, t: { price: number; area: number }) => sum + t.price / t.area, 0) / cityWithArea.length
          : cityTop5.length > 0
            ? cityTop5.reduce((sum: number, t: { price: number }) => sum + t.price, 0) / cityTop5.length / 85
            : null;
        const cityAreaForEstimate = Number.isFinite(propertyAreaSqm) && propertyAreaSqm > 0 ? propertyAreaSqm : 85;
        const cityEstimatedValue = cityAvgPricePerSqm != null ? Math.round(cityAvgPricePerSqm * cityAreaForEstimate) : null;
        const cityLastSale = cityTop5[0];
        const cityLastSaleTs = cityLastSale?.date ? new Date(cityLastSale.date).getTime() : 0;
        const cityLastSaleOld = cityLastSaleTs > 0 && cityLastSaleTs < twoYearsAgo;

        if (cityTop5.length > 0) {
          console.log("[israel-real-estate] City fallback success:", cityTop5.length, "transactions");
          return NextResponse.json({
            transactions: cityTop5.map((t: { price: number; date: string | null }) => ({ price: t.price, date: t.date })),
            avgPrice: (cityLastSaleOld && cityEstimatedValue != null) ? cityEstimatedValue : (cityLastSale?.price ?? cityEstimatedValue ?? Math.round(cityTop5.reduce((s: number, t: { price: number }) => s + t.price, 0) / cityTop5.length)),
            avgPricePerSqm: cityAvgPricePerSqm != null ? Math.round(cityAvgPricePerSqm) : null,
            lastSaleDate: cityLastSale?.date ?? null,
            lastSalePrice: cityLastSale?.price ?? null,
            lastSaleOlderThan2Years: cityLastSaleOld,
            transactionCount: cityTop5.length,
            source: "data.gov.il",
            isCityFallback: true,
          });
        }
      }
    }

    console.log("[israel-real-estate] Success:", { transactionsCount: transactions.length, avgPrice, avgPricePerSqm, lastSaleOlderThan2Years });
    return NextResponse.json({
      transactions,
      avgPrice,
      avgPricePerSqm: avgPricePerSqm != null ? Math.round(avgPricePerSqm) : null,
      lastSaleDate: lastSale?.date ?? null,
      lastSalePrice: lastSale?.price ?? null,
      lastSaleOlderThan2Years,
      transactionCount: transactions.length,
      source: "data.gov.il",
      isCityFallback: false,
    });
  } catch (err) {
    console.error("[israel-real-estate] API error:", err);
    return NextResponse.json({
      transactions: [],
      avgPrice: null,
      avgPricePerSqm: null,
      lastSaleDate: null,
      lastSalePrice: null,
      transactionCount: 0,
      source: "data.gov.il",
      error: err instanceof Error ? err.message : "Connection failed",
    });
  }
}
