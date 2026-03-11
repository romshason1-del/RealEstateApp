/**
 * UK Postcode Provider
 * Uses postcodes.io (free, ONS/Ordnance Survey data) for address→area matching.
 * Returns LSOA, MSOA, LAD for Census and HPI lookups.
 */

const POSTCODES_IO_URL = "https://api.postcodes.io/postcodes";
const TIMEOUT_MS = 3000;

export type UKPostcodeResult = {
  postcode: string;
  lsoa: string;
  msoa: string;
  adminDistrict: string;
  adminDistrictCode: string;
  region: string;
} | null;

/**
 * Lookup postcode via postcodes.io. Returns ONS geography for address-to-area matching.
 */
export async function lookupUKPostcode(postcode: string): Promise<UKPostcodeResult | null> {
  const pc = (postcode ?? "").trim().replace(/\s+/g, " ").toUpperCase();
  if (!pc || pc.length < 5) return null;

  try {
    const res = await fetch(`${POSTCODES_IO_URL}/${encodeURIComponent(pc)}`, {
      signal: AbortSignal.timeout(TIMEOUT_MS),
      headers: { Accept: "application/json" },
    });
    if (!res.ok) return null;

    const data = (await res.json()) as { status?: number; result?: Record<string, unknown> };
    const r = data?.result;
    if (!r || typeof r !== "object") return null;

    const lsoa = (r.lsoa ?? r.lsoa21 ?? "") as string;
    const msoa = (r.msoa ?? r.msoa21 ?? "") as string;
    const adminDistrict = (r.admin_district ?? "") as string;
    const codes = r.codes as Record<string, string> | undefined;
    const adminDistrictCode = (codes?.admin_district ?? codes?.lau2 ?? "") as string;
    const region = (r.region ?? r.european_electoral_region ?? "") as string;

    return {
      postcode: pc,
      lsoa: String(lsoa || ""),
      msoa: String(msoa || ""),
      adminDistrict: String(adminDistrict || ""),
      adminDistrictCode: String(adminDistrictCode || ""),
      region: String(region || ""),
    };
  } catch {
    return null;
  }
}
