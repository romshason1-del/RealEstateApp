/**
 * Alias for the main property-value API (`/api/property-value`).
 * US / NYC: same delegation as `/api/property-value` (US → `/api/us/nyc-app-output`, v4 table `us_nyc_app_output_final_v4`).
 * `/api/us/property-value` also forwards to that route (legacy URL compatibility only).
 *
 * Query params match `/api/property-value` (e.g. `countryCode=US`, `address`, `unit_or_lot`).
 */

export { GET } from "../property-value/route";
