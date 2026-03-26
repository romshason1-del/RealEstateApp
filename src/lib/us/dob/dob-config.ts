/**
 * NYC Open Data — DOB Job Application Filings (US only).
 * @see https://data.cityofnewyork.us/Housing-Development/DOB-Job-Application-Filings/ic3t-wcy2
 */

export const DOB_SOCRATA_ORIGIN = "https://data.cityofnewyork.us";

/** Dataset identifier (Socrata resource id). */
export const DOB_JOB_FILINGS_DATASET_ID = "ic3t-wcy2";

export function dobJobFilingsResourcePath(): string {
  return `/resource/${DOB_JOB_FILINGS_DATASET_ID}.json`;
}

export function dobJobFilingsResourceUrl(): string {
  return `${DOB_SOCRATA_ORIGIN}${dobJobFilingsResourcePath()}`;
}

/** Optional Socrata app token (higher rate limits). Same env vars as ACRIS. */
export function dobSocrataAppToken(): string | undefined {
  const t = process.env.NYC_SOCRATA_APP_TOKEN ?? process.env.SOCRATA_APP_TOKEN;
  return typeof t === "string" && t.trim() ? t.trim() : undefined;
}

/** Default row cap for address filings queries. */
export const DOB_FILINGS_DEFAULT_LIMIT = 50;
