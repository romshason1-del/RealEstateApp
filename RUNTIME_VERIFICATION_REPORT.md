# Runtime Verification Report – UK Property Data Fix

## Production Access Limitation

**Production URL:** `https://real-estate-oykqqs4n0-romshason1-5829s-projects.vercel.app` (and similar deployment URLs)

**Finding:** Vercel Deployment Protection is enabled. Unauthenticated requests to the API return an HTML auth page (200) instead of JSON. This leads to:
1. `fetch()` receiving HTML
2. `res.json()` failing or returning invalid data
3. `insightsData` = `{ message: "Invalid response", error: "PARSE_ERROR" }`
4. No `uk_land_registry` in the response
5. UI shows "No property data found for this address"

## Root Cause

Two failure paths:

1. **API returns 404 (pre-fix deploy):** Response has `message: "no transaction found"` but no `uk_land_registry` → UI shows "No property data found".
2. **Deployment protection / parse error:** API returns HTML or invalid JSON → parse error → no `uk_land_registry` → same UI.

## Fixes Applied

### Fix 1: API route (`src/app/api/property-value/route.ts`) – already committed

For UK "no transaction found", return 200 with a minimal `uk_land_registry` instead of 404.

### Fix 2: Frontend fallback (`src/components/property-value-card.tsx`)

When `isUK && insightsData?.message === "no transaction found"` and `uk_land_registry` is missing, derive a minimal `ukLandRegistry` on the client:

```ts
ukLandRegistry =
  ukLandRegistryRaw ??
  (isUK && insightsData?.message === "no transaction found"
    ? { has_building_match: false, average_area_price: null, area_transaction_count: 0, ... }
    : undefined);
```

This covers:
- Old API still returning 404
- Any case where the API returns "no transaction found" without `uk_land_registry`

## Exact UI Condition Fixed

**Before:** `!hasPropertyData && !ukLandRegistry` → "No property data found for this address"

**After:** With the fallback, `ukLandRegistry` is set when `message === "no transaction found"`, so the UK section renders instead of the no-data block.

## Files Changed

1. `src/components/property-value-card.tsx` – added `ukLandRegistry` fallback for UK no-match responses

## Verification Steps

### 1. Local verification (no deployment protection)

```bash
npm run dev
# In another terminal:
node -e "
fetch('http://localhost:3000/api/property-value?address=221B%20Baker%20St%2C%20London%20NW1%206XE%2C%20UK&countryCode=UK')
  .then(r => r.json())
  .then(d => console.log(JSON.stringify({
    status: 'ok',
    has_uk_land_registry: !!d.uk_land_registry,
    has_building_match: d.uk_land_registry?.has_building_match,
    fallback_level_used: d.uk_land_registry?.fallback_level_used,
    average_area_price: d.uk_land_registry?.average_area_price,
    area_transaction_count: d.uk_land_registry?.area_transaction_count,
  }, null, 2)));
"
```

### 2. Production verification (requires auth or bypass)

If deployment protection is enabled, either:
- Use `vercel curl /api/property-value?address=...&countryCode=UK --deployment <url>`
- Or disable deployment protection for the production domain
- Or use a bypass token as per Vercel docs

### 3. Deploy and test

```bash
git add src/components/property-value-card.tsx
git commit -m "fix: frontend fallback for UK no-match when API returns 404"
git push
```

After deploy, hard refresh (Ctrl+Shift+R) to avoid cached JS.

## Redeploy / Cache

- **Redeploy:** Yes, to ship the frontend fallback.
- **Hard refresh:** Yes, to avoid cached client bundle.
- **Server cache:** In-memory; cleared on redeploy.
- **Client cache:** 5 min TTL; hard refresh clears it.

## Expected Result After Fix

For "221B Baker St, London NW1 6XE, UK":

- **If API returns 200 with `uk_land_registry`:** UK section with area insights.
- **If API returns 404 with `message: "no transaction found"`:** UK section with "Area insights – no exact building match" and "No area transaction data available." (via frontend fallback).
- **If fetch fails or returns invalid JSON:** Still shows "No property data found" (unchanged).
