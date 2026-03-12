import { NextRequest, NextResponse } from "next/server";

const DATA_GOV_IL_BASE = "https://data.gov.il/api/3/action";
const NADLAN_RESOURCE_ID = "ad6680ef-5d46-4654-be8d-7301292a8e48";

/** Hebrew field names from Israel Tax Authority real estate dataset (מאגר עסקאות הנדל"ן) */
const SALE_PRICE_FIELDS = ["מחיר העסקה", "מחיר עסקה", "מחיר_העסקה", "מחיר_עסקה", "sale_price", "price"];
const SALE_DATE_FIELDS = ["תאריך העסקה", "תאריך עסקה", "תאריך_העסקה", "תאריך_עסקה", "DEALDATE", "sale_date", "date"];
const AREA_FIELDS = ["GFA_QUANTITY", "GFA", "שטח", "שטח במ\"ר", "שטח_במ\"ר", "מ\"ר", "area", "sqm"];
const ADDRESS_FIELDS = ["כתובת", "street", "address", "רחוב"];
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

/** Extract GFA_QUANTITY or area from record - iterates all keys for robustness */
function getArea(record: Record<string, unknown>): number {
  for (const f of AREA_FIELDS) {
    const val = record[f];
    if (val != null && val !== "") {
      const n = parseNumeric(val);
      if (n > 0) return n;
    }
  }
  for (const [key, val] of Object.entries(record)) {
    if (val == null || val === "") continue;
    const k = String(key).toUpperCase();
    if (k.includes("GFA") || k.includes("QUANTITY") || k.includes("שטח") || k.includes("AREA") || k.includes("SQM") || key.includes("מ")) {
      const n = parseNumeric(val);
      if (n > 0) return n;
    }
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
 * Parse address into street, city, and house number.
 * Handles "123 Dizengoff St, Tel Aviv", "Dizengoff 123, Tel Aviv", "רחוב דיזנגוף 123, תל אביב"
 */
function parseAddressParts(address: string): { street: string; city: string; houseNumber: string } {
  const trimmed = address.trim();
  let parts = trimmed.split(/[,،]/).map((p) => p.trim()).filter(Boolean);

  if (parts[parts.length - 1]?.match(/^(Israel|ישראל)$/i)) {
    parts = parts.slice(0, -1);
  }

  const houseMatch = trimmed.match(/\b(\d+)\b/);
  const houseNumber = houseMatch ? houseMatch[1] : "";

  if (parts.length >= 2) {
    const city = normalizeForSearch(parts[parts.length - 1]);
    const streetPart = parts.slice(0, -1).join(" ");
    const street = normalizeForSearch(streetPart.replace(/\d+/g, " ").replace(/\s+/g, " ").trim());
    return { street: street || normalizeForSearch(streetPart), city, houseNumber };
  }
  if (parts.length === 1) {
    const cleaned = normalizeForSearch(parts[0].replace(/\d+/g, " ").replace(/\s+/g, " ").trim());
    return { street: cleaned || normalizeForSearch(parts[0]), city: cleaned, houseNumber };
  }
  return { street: "", city: "", houseNumber: "" };
}

/** Street name without house number for API search */
function getStreetName(street: string): string {
  return street.replace(/^\d+\s+/, "").replace(/\s+\d+$/, "").trim() || street;
}

function getAddressFromRecord(record: Record<string, unknown>): string {
  for (const f of ADDRESS_FIELDS) {
    const val = record[f];
    if (val != null && val !== "") return String(val).trim();
  }
  for (const [key, val] of Object.entries(record)) {
    if (val != null && val !== "" && (key.includes("כתובת") || key.includes("address") || key.includes("street"))) {
      return String(val).trim();
    }
  }
  return "";
}

function recordMatchesHouse(record: Record<string, unknown>, houseNumber: string): boolean {
  if (!houseNumber) return false;
  const addr = getAddressFromRecord(record);
  if (!addr) return false;
  return addr.includes(houseNumber) || new RegExp(`\\b${houseNumber}\\b`).test(addr);
}

export async function GET(request: NextRequest) {
  console.log("[israel-real-estate] GET request received");
  const { searchParams } = new URL(request.url);
  const address = searchParams.get("address") ?? "";
  const street = searchParams.get("street") ?? "";
  const city = searchParams.get("city") ?? "";
  const propertyAreaSqm = parseFloat(searchParams.get("propertyAreaSqm") ?? "100");
  const limitParam = parseInt(searchParams.get("limit") ?? "100", 10);
  const limit = Number.isFinite(limitParam) ? Math.min(Math.max(limitParam, 1), 200) : 100;


  const { street: parsedStreet, city: parsedCity, houseNumber } = parseAddressParts(address);
  const searchStreet = street || getStreetName(parsedStreet);
  const searchCity = city || parsedCity;

  console.log("[israel-real-estate] Fetch by street+city:", { searchStreet, searchCity, houseNumber, limit });

  if (!searchStreet && !searchCity) {
    return NextResponse.json(
      { error: "Street or city is required", transactions: [], avgPrice: null },
      { status: 400 }
    );
  }

  const fetchHeaders = {
    Accept: "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
  };
  const sortParam = "תאריך_העסקה desc";

  try {
    const q = [searchStreet, searchCity].filter(Boolean).join(" ");
    const url = `${DATA_GOV_IL_BASE}/datastore_search?resource_id=${NADLAN_RESOURCE_ID}&q=${encodeURIComponent(q)}&limit=${limit}&sort=${encodeURIComponent(sortParam)}`;
    const res = await fetch(url, { headers: fetchHeaders, signal: AbortSignal.timeout(15000) });
    if (!res.ok) throw new Error(`API returned ${res.status}`);
    const json = await res.json().catch(() => ({ success: false, error: { message: "Invalid JSON" } }));
    if (json?.success === false) throw new Error(json?.error?.message ?? "API request failed");

    const recordList = (json?.result?.records ?? []) as Record<string, unknown>[];
    console.log("[israel-real-estate] Records received:", recordList.length, "for street+city");

    const withGushParcel = recordList.filter((r: Record<string, unknown>) => hasGushAndParcel(r));
    console.log("[israel-real-estate] After GUSH/PARCEL filter:", withGushParcel.length);
    if (withGushParcel.length > 0) {
      const first = withGushParcel[0] as Record<string, unknown>;
      console.log("[israel-real-estate] First record keys:", Object.keys(first).join(", "));
      console.log("[israel-real-estate] First record GFA/area:", getArea(first));
    }

    const allTransactions = withGushParcel.map((r: Record<string, unknown>) => {
      const price = getFieldValue(r, SALE_PRICE_FIELDS);
      const date = getFieldValue(r, SALE_DATE_FIELDS);
      const area = getArea(r);
      return {
        price: parseNumeric(price),
        date: date != null ? String(date) : null,
        area,
        record: r,
      };
    }).filter((t: { price: number }) => t.price > 0);

    const sortedByDate = [...allTransactions].sort((a, b) => {
      const da = a.date ? new Date(a.date).getTime() : 0;
      const db = b.date ? new Date(b.date).getTime() : 0;
      return db - da;
    });

    const forThisHouse = houseNumber ? sortedByDate.filter((t) => recordMatchesHouse(t.record as Record<string, unknown>, houseNumber)) : [];
    const hasSpecificHouse = forThisHouse.length > 0;
    const lastSaleForHouse = forThisHouse[0];
    const lastSaleForStreet = sortedByDate[0];

    const top10Street = sortedByDate.slice(0, 10);
    const top20Street = sortedByDate.slice(0, 20);
    const withArea = top20Street.filter((t: { area: number }) => t.area > 0);
    const avgPricePerSqm = withArea.length > 0
      ? withArea.reduce((sum: number, t: { price: number; area: number }) => sum + t.price / t.area, 0) / withArea.length
      : top20Street.length > 0
        ? top20Street.reduce((sum: number, t: { price: number }) => sum + t.price, 0) / top20Street.length / 100
        : null;
    const streetAverage = top10Street.length > 0
      ? Math.round(top10Street.reduce((s: number, t: { price: number }) => s + t.price, 0) / top10Street.length)
      : null;
    const sqmForEstimate = propertyAreaSqm > 0 ? propertyAreaSqm : 100;
    const avgPrice = avgPricePerSqm != null ? Math.round(avgPricePerSqm * sqmForEstimate) : streetAverage;

    const lastSalePrice = hasSpecificHouse ? lastSaleForHouse?.price : (lastSaleForStreet?.price ?? streetAverage);
    const lastSaleDate = hasSpecificHouse ? lastSaleForHouse?.date : lastSaleForStreet?.date ?? null;

    const transactions = sortedByDate.slice(0, 20).map((t: { price: number; date: string | null }) => ({ price: t.price, date: t.date }));

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
        const cityTop10 = citySorted.slice(0, 10);
        const cityTop20 = citySorted.slice(0, 20);
        const cityWithArea = cityTop20.filter((t: { area: number }) => t.area > 0);
        const cityAvgPricePerSqm = cityWithArea.length > 0
          ? cityWithArea.reduce((sum: number, t: { price: number; area: number }) => sum + t.price / t.area, 0) / cityWithArea.length
          : cityTop20.length > 0
            ? cityTop20.reduce((sum: number, t: { price: number }) => sum + t.price, 0) / cityTop20.length / 100
            : null;
        const cityLastSale = cityTop20[0];
        const citySqmForEstimate = propertyAreaSqm > 0 ? propertyAreaSqm : 100;
        const cityNeighborhoodEstimate = cityAvgPricePerSqm != null ? Math.round(cityAvgPricePerSqm * citySqmForEstimate) : null;
        if (cityTop20.length > 0) {
          const cityStreetAvg = cityTop10.length > 0
            ? Math.round(cityTop10.reduce((s: number, t: { price: number }) => s + t.price, 0) / cityTop10.length)
            : null;
          const cityAvgPrice = cityAvgPricePerSqm != null ? Math.round(cityAvgPricePerSqm * citySqmForEstimate) : cityStreetAvg;
          console.log("[israel-real-estate] City fallback success:", cityTop20.length, "transactions");
          return NextResponse.json({
            transactions: cityTop20.map((t: { price: number; date: string | null }) => ({ price: t.price, date: t.date })),
            avgPrice: cityAvgPrice,
            avgPricePerSqm: cityAvgPricePerSqm != null ? Math.round(cityAvgPricePerSqm) : null,
            lastSaleDate: cityLastSale?.date ?? null,
            lastSalePrice: cityLastSale?.price ?? cityStreetAvg ?? null,
            streetAverage: cityStreetAvg,
            hasSpecificHouse: false,
            transactionCount: cityTop20.length,
            source: "data.gov.il",
            isCityFallback: true,
          });
        }
      }
    }

    console.log("[israel-real-estate] Success:", { transactionsCount: transactions.length, hasSpecificHouse, lastSalePrice, streetAverage });
    return NextResponse.json({
      transactions,
      avgPrice,
      avgPricePerSqm: avgPricePerSqm != null ? Math.round(avgPricePerSqm) : null,
      lastSaleDate,
      lastSalePrice,
      streetAverage,
      hasSpecificHouse,
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
