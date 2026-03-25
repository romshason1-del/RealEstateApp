/**
 * NYC Open Data / Socrata ACRIS dataset identifiers (US only).
 * @see https://data.cityofnewyork.us/City-Government/ACRIS-Real-Property-Legals/8h5j-fqxa
 * @see https://data.cityofnewyork.us/City-Government/ACRIS-Real-Property-Master/bnx9-e6tj
 */

export const ACRIS_SOCRATA_ORIGIN = "https://data.cityofnewyork.us";

/** ACRIS – Real Property Legals */
export const ACRIS_LEGALS_DATASET_ID = "8h5j-fqxa";

/** ACRIS – Real Property Master */
export const ACRIS_MASTER_DATASET_ID = "bnx9-e6tj";

export function acrisLegalsResourcePath(): string {
  return `/resource/${ACRIS_LEGALS_DATASET_ID}.json`;
}

export function acrisMasterResourcePath(): string {
  return `/resource/${ACRIS_MASTER_DATASET_ID}.json`;
}

export function acrisLegalsResourceUrl(): string {
  return `${ACRIS_SOCRATA_ORIGIN}${acrisLegalsResourcePath()}`;
}

export function acrisMasterResourceUrl(): string {
  return `${ACRIS_SOCRATA_ORIGIN}${acrisMasterResourcePath()}`;
}

/** Optional Socrata app token (higher rate limits). Not required for read-only public data. */
export function acrisSocrataAppToken(): string | undefined {
  const t = process.env.NYC_SOCRATA_APP_TOKEN ?? process.env.SOCRATA_APP_TOKEN;
  return typeof t === "string" && t.trim() ? t.trim() : undefined;
}

/** Default row cap per request (Socrata defaults to 1000 if omitted). */
export const ACRIS_DEFAULT_LIMIT = 5000;
