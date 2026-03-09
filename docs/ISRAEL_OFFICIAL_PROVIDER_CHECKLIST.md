# Israel Official Provider – Integration Checklist

Integration guide for connecting the Israel Official Provider to the real Tax Authority / government API.

---

## 1. Required Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PROPERTY_PROVIDER_ISRAEL` | Yes | Set to `official` to enable this provider |
| `ISRAEL_TAX_API_BASE_URL` | Yes | Base URL (e.g. `https://api.tax.gov.il`) |
| `ISRAEL_TAX_API_KEY` | Conditional | API key (if using API key auth) |
| `ISRAEL_TAX_API_CLIENT_ID` | Conditional | OAuth2 client ID (if using OAuth) |
| `ISRAEL_TAX_API_CLIENT_SECRET` | Conditional | OAuth2 client secret (if using OAuth) |
| `ISRAEL_TAX_API_ENDPOINT_TRANSACTIONS` | No | Endpoint path (e.g. `/v1/transactions`). Default: `/transactions` |
| `ISRAEL_TAX_API_TIMEOUT_MS` | No | Request timeout in ms. Default: `15000` |
| `ISRAEL_TAX_API_RETRIES` | No | Number of retries on 5xx. Default: `2` |

**Auth:** Provide either `ISRAEL_TAX_API_KEY` or both `ISRAEL_TAX_API_CLIENT_ID` and `ISRAEL_TAX_API_CLIENT_SECRET`.

---

## 2. Authentication Options

| Option | Env Variables | Implementation |
|--------|---------------|----------------|
| **API Key** | `ISRAEL_TAX_API_KEY` | Sent as `Authorization: Bearer {key}` and `X-API-Key: {key}` |
| **OAuth2 Client Credentials** | `ISRAEL_TAX_API_CLIENT_ID`, `ISRAEL_TAX_API_CLIENT_SECRET` | Placeholder: add token endpoint call and use Bearer token |

**TODO for OAuth:** Implement token fetch from `{baseUrl}/oauth/token` (or env-configured path) before each request; cache token until expiry.

---

## 3. Expected Request Inputs

The provider sends the following in the request body (POST) or as query params (if API uses GET):

| Field | Type | Description |
|-------|------|-------------|
| `city` | string | City / locality name |
| `street` | string | Street name |
| `houseNumber` | string | Building / house number |
| `latitude` | number? | Optional coordinates |
| `longitude` | number? | Optional coordinates |

**Note:** If the real API uses GET, update `buildRequestInit` in `israel-official-api.ts` to use query parameters instead of a JSON body.

---

## 4. Expected Response Fields

The response mapper supports multiple shapes. Adapt `RawApiTransaction` and `mapResponseToInsights` when the real API schema is known.

**Current mapping (flexible):**

| UI Field | API Field (candidates) |
|----------|------------------------|
| `transaction_date` | `date`, `transactionDate` |
| `transaction_price` | `price`, `salePrice`, `amount` |
| `property_size` | `area`, `propertySize`, `sqm` |

**Response structure:** Expect one of:

- `{ transactions: [...] }`
- `{ data: [...] }`
- `{ results: [...] }`
- Raw array `[...]`

**Sorting:** Transactions are assumed newest-first. If not, sort by date before mapping.

---

## 5. Mapping to Property Value UI

| UI Section | Provider Output | Source |
|------------|-----------------|--------|
| **Latest Official Transaction** | `latest_transaction` | First transaction in API response |
| **Estimated Current Value** | `current_estimated_value` | Derived from latest price and size |
| **Building Activity (Last 3 Years)** | `building_summary_last_3_years` | Transactions filtered to last 3 years |

---

## 6. Validation Checklist Before Going Live

### Configuration

- [ ] `PROPERTY_PROVIDER_ISRAEL=official` is set
- [ ] `ISRAEL_TAX_API_BASE_URL` is correct and reachable
- [ ] Auth credentials (API key or OAuth) are set and valid
- [ ] `ISRAEL_TAX_API_ENDPOINT_TRANSACTIONS` matches the real endpoint path (if different from default)

### Authentication

- [ ] API key or OAuth credentials are tested
- [ ] OAuth token endpoint and flow are implemented (if used)
- [ ] Token refresh is handled when expired

### Request / Response

- [ ] Request method (GET/POST) matches the API
- [ ] Request body/query format matches the API spec
- [ ] Response mapper is updated for the real API schema
- [ ] Field names in `RawApiTransaction` match the API

### Error Handling

- [ ] 401/403 mapped to user-friendly auth error message
- [ ] 404 mapped to "no transaction found"
- [ ] 5xx triggers retry (already implemented)
- [ ] Timeout and network errors are handled (already implemented)

### Testing

- [ ] Test with a known address that has transactions
- [ ] Test with an address that has no transactions
- [ ] Test timeout behavior (e.g. slow API)
- [ ] Test with invalid or expired credentials

### Security

- [ ] API keys and secrets are not logged
- [ ] Credentials are only used server-side (no exposure to client)

---

## Files Changed

| File | Change |
|------|--------|
| `src/lib/property-value-providers/config.ts` | Added env helpers, timeout, retries, endpoint path |
| `src/lib/property-value-providers/israel-official-api.ts` | **NEW** – Request builder, auth, response/error mappers, timeout, retry |
| `src/lib/property-value-providers/israel-official-provider.ts` | Uses `fetchIsraelOfficialTransactions` when configured |
| `.env.example` | Added `ISRAEL_TAX_API_ENDPOINT_TRANSACTIONS`, `TIMEOUT_MS`, `RETRIES` |
| `docs/ISRAEL_OFFICIAL_PROVIDER_CHECKLIST.md` | **NEW** – This checklist |
