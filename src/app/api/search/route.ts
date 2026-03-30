/**
 * Alias for the main property-value API (`/api/property-value`).
 * US / NYC searches that hit this route use the same stack as `/api/us/property-value`:
 * BigQuery precomputed cards plus `us_nyc_address_master_v1` candidate hints (see `us-nyc-api-truth.ts`).
 *
 * Query params match `/api/property-value` (e.g. `countryCode=US`, `address`, `unit_or_lot`).
 */

export { GET } from "../property-value/route";
