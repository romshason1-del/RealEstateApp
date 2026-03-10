# StreetIQ System Audit Report

**Date:** March 6, 2025  
**Scope:** Full data pipeline from address input to property data output  
**Goal:** Verify system health before integrating additional real estate data sources

---

## Executive Summary

The StreetIQ application has a well-structured data pipeline with provider-based architecture. The UK Land Registry integration is robust with geocoding fallback, tiered fallbacks, and relaxed filtering for edge cases. Several issues were identified that should be addressed before adding new data sources.

**Overall Health: GOOD** — Core pipeline works; minor fixes recommended.

---

## 1. Address Input Pipeline

### 1.1 Parsing Logic

| Parser | File | Status |
|--------|------|--------|
| `parseAddressFromFullString` | `address-parse.ts` | ✅ General (IL-style) |
| `parseUSAddressFromFullString` | `address-parse.ts` | ✅ US format |
| `parseUKAddressFromFullString` | `address-parse.ts` | ⚠️ See issues |

**UK Parser Behavior:**
- Extracts house number, street, city, postcode
- Postcode regex: `/\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b/i` — requires **full** UK postcode (outward + inward, e.g. `SW1A 2AA`, `W11 3QY`)
- **Issue:** Partial postcodes like `W11` (outward only) are **not** extracted. Address "8 Holland Park, London W11" yields `postcode: ""`.
- **Mitigation:** Geocoding in UK provider resolves postcode when missing; street+city still parsed correctly.

### 1.2 Normalization

- **UK Land Registry:** `STREET_ABBREVS` in `uk-land-registry-provider.ts` — St, Rd, Ave, Ln, Dr, Pl, Ct, etc. ✅
- **Israel:** `address-canonical.ts` — Hebrew/English, city aliases, street aliases. UK not covered (expected).

### 1.3 Validation Flow

- **Route** (`app/api/property-value/route.ts`): Parses `address` param, validates `postcode OR (street && city)` for UK.
- **Client** (`property-value-api.ts`): Same check before calling API — can reject before server geocode runs.
- **Provider** (`uk-land-registry-provider.ts`): Accepts `fullAddress` for geocoding when postcode missing.

**Issue:** Addresses like `"London W11"` (city + area only) may fail validation because `parseUKAddressFromFullString` gives `city: ""`, `street: "London W11"`. `hasStreetAndCity` = false → 400 before geocoding.

**Recommendation:** Allow UK when `addressParam.length >= 5` even if parsing yields no street+city, so server can attempt geocoding.

---

## 2. Geocoding

### 2.1 Integration

| Source | Location | When Used |
|--------|----------|-----------|
| **Google Geocoding** | `address-explorer.tsx` (client) | Map search, Places autocomplete, reverse geocode |
| **Google Geocoding** | `uk-land-registry-provider.ts` (server) | Postcode resolution when missing |
| **Nominatim** | `uk-land-registry-provider.ts` (server) | Fallback when no Google API key |

### 2.2 UK Provider Geocoding

- **Flow:** `resolvePostcodeFromAddress()` → Google (if key) → Nominatim (fallback)
- **Timeouts:** 8s each
- **User-Agent:** `StreetIQ-PropertyValue/1.0` for Nominatim (ToS compliant)
- **Postcode validation:** `UK_POSTCODE_REGEX` ensures full format before use

**Status:** ✅ Correctly implemented.

### 2.3 Client-Side Geocoding

- Uses `window.google.maps.Geocoder` for place selection and reverse geocode
- No Nominatim fallback on client (Google Maps required for map)
- Error handling: "Google Maps geocoding is unavailable right now."

---

## 3. Land Registry Data

### 3.1 SPARQL Endpoint

- **URL:** `http://landregistry.data.gov.uk/landregistry/query`
- **Method:** POST, `application/x-www-form-urlencoded`
- **Timeout:** 15s
- **Verified:** Endpoint responds with valid transaction data (amount, date)

### 3.2 Query Modes

| Mode | When | Query |
|------|------|-------|
| `exact` | Has postcode | Exact postcode match |
| `street` | No postcode, street+city | Street + town CONTAINS |
| `locality` | Town only | Town CONTAINS |
| `outward_postcode` | Fallback | Postcode prefix (e.g. W11) |
| `postcode_area` | Fallback | Area prefix (e.g. W) |

### 3.3 Transaction Filtering

- **Strict:** `amount > 0`, `isValidTransaction(category)`, deduplication
- **Relaxed:** `amount > 0` only (used when strict yields 0 but raw bindings exist)
- **Excluded categories:** additional, transfer, lease extension, repossession, power of sale

**Status:** ✅ Correct. Zero prices filtered. Relaxed mode used for area fallback when appropriate.

---

## 4. Property Matching

### 4.1 Building Match Logic

- **Exact:** `matchesBuildingExact()` — paon + street + town, strict
- **Fuzzy:** `matchesBuildingFuzzy()` — tolerant street match
- **Preference:** Exact over fuzzy

### 4.2 Fallback Layers (in order)

1. **Primary query** (exact postcode, or street+town, or locality)
2. **Bindings empty** → street → locality → outward_postcode → postcode_area
3. **Items empty (filtered)** → same fallback chain
4. **Items still empty, bindings exist** → relaxed filtering for area metrics

**Status:** ✅ Fallback chain correctly implemented.

---

## 5. Data Validation

### 5.1 Duplicates

- **Deduplication key:** `paon|saon|addrStreet|dateStr|amount`
- **Status:** ✅ Duplicates filtered in `processBindingsToItems`

### 5.2 Invalid Prices

- **Zero/negative:** `amount <= 0` filtered ✅
- **Outliers:** No statistical outlier detection (e.g. IQR, z-score). Single extreme transaction can skew area average.
- **Recommendation:** Consider capping or flagging prices outside e.g. £10k–£50M for area metrics.

### 5.3 Category Filtering

- Invalid categories excluded in strict mode ✅
- Relaxed mode includes all categories for area when strict yields 0 ✅

---

## 6. API Performance

| Operation | Timeout | Location |
|-----------|---------|----------|
| Geocoding (Google) | 8s | uk-land-registry-provider |
| Geocoding (Nominatim) | 8s | uk-land-registry-provider |
| Land Registry SPARQL | 15s | uk-land-registry-provider |
| Property value fetch (client) | 20s | property-value-api.ts |
| Cache TTL | 5 min | property-value-api, route |

**Recommendation:** Add response-time logging (e.g. `Date.now()` before/after) in development to monitor latency. No server-side timing metrics observed.

---

## 7. Frontend Integration

### 7.1 Property Value Card

- **UK section:** Building Average Price, Transactions in Building, Latest Building Transaction, Average Area Price
- **Fallback messaging:** "No recent building transactions in the last 5 years." when historical exists ✅
- **Error state:** "No UK Land Registry transaction found." when no match ✅

### 7.2 Map Location

- `selectedBuilding` holds `position` (lat/lng) and `address`
- Property card receives `position` and `address` from `getPropertyInsight`
- **Status:** ✅ Map and card wired correctly

### 7.3 Error Handling

- Loading: "Loading official data…"
- No data: "No UK Land Registry transaction found." with debug JSON when available
- **Status:** ✅ Appropriate messaging

---

## 8. Debug Panel

### 8.1 UK-Specific Fields Displayed

| Field | Source | Status |
|-------|--------|--------|
| Normalized postcode | `debug.normalized_postcode` | ✅ |
| Postcode query mode | `debug.postcode_query_executed` | ✅ |
| Postcode query URL | `debug.postcode_query_url` | ✅ |
| Postcode raw result count | `debug.postcode_query_raw_result_count` | ✅ |
| Postcode results | `debug.postcode_results_count` | ✅ |
| Exact building matches | `debug.exact_building_matches_count` | ✅ |
| Fuzzy building matches | `debug.fuzzy_building_matches_count` | ✅ |
| Address match mode | `debug.address_match_mode` | ✅ |

### 8.2 Missing in Debug

- **Fallback level used:** Debug panel shows `d?.fallback_level_used`, but UK provider does not set it. `area_fallback_level` lives in `uk_land_registry`, not `debug`.
- **Recommendation:** Add `fallback_level_used: areaFallbackLevel` to `ukDebug` so Debug Panel displays it consistently.

---

## Detected Issues Summary

| # | Severity | Issue | Location |
|---|----------|-------|----------|
| 1 | Low | UK parser does not extract partial postcodes (e.g. "W11") | `address-parse.ts` |
| 2 | Low | Addresses like "London W11" may fail validation before geocoding | Route + property-value-api |
| 3 | Low | Debug Panel "Fallback level used" empty for UK | uk-land-registry-provider |
| 4 | Medium | No outlier detection for transaction prices | uk-land-registry-provider |
| 5 | Low | No response-time metrics for performance monitoring | API route |

---

## Suggested Fixes

### Fix 1: UK partial postcode extraction (optional)

Extend `parseUKAddressFromFullString` to also match outward-only patterns like `W11`, `SW1`, `E14`:

```ts
const outwardOnlyRe = /\b([A-Z]{1,2}\d{1,2}[A-Z]?)\b/i;
// Use when full postcode not found
```

### Fix 2: Allow geocodeable UK addresses

In route validation and `property-value-api.ts`, for UK allow when `addressParam.trim().length >= 5` even if `!hasStreetAndCity`, so server can attempt geocoding.

### Fix 3: Debug Panel fallback level for UK

In `uk-land-registry-provider.ts`, add to `ukDebug`:

```ts
fallback_level_used: areaFallbackLevel,
```

### Fix 4: Outlier handling (future)

Consider filtering or capping prices outside a reasonable range (e.g. £10k–£50M) for area averages to reduce skew from data errors.

---

## Areas for Data Reliability Before New Datasets

1. **Validation consistency:** Align client and server validation so geocoding is always attempted when appropriate.
2. **Debug completeness:** Ensure all providers populate `fallback_level_used` (or equivalent) for Debug Panel.
3. **Performance observability:** Add timing logs or metrics for geocoding and provider calls.
4. **Outlier handling:** Define and implement price bounds for area-level metrics.

---

## Conclusion

The StreetIQ UK Land Registry pipeline is well implemented with geocoding, fallbacks, and relaxed filtering. The main gaps are validation edge cases for minimal addresses, Debug Panel consistency, and optional outlier handling. Addressing the suggested fixes will improve robustness before integrating additional data sources.
