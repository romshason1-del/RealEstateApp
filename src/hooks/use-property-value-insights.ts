"use client";

import * as React from "react";
import { fetchPropertyValueInsights, type PropertyValueInsightsResponse } from "@/lib/property-value-api";

export type UsePropertyValueInsightsOptions = {
  latitude?: number;
  longitude?: number;
  countryCode?: string;
  /** UK only: raw typed input (preserves Flat/Unit) */
  rawInputAddress?: string;
  /** UK only: Google formatted_address from selected suggestion */
  selectedFormattedAddress?: string;
};

/** Countries that have an official property data provider. FR temporarily disabled: DVF source (api.cquest.org) returns 502. Re-enable by adding "FR" when stable. */
const PROVIDER_COUNTRIES = ["IL", "US", "UK", "GB", "IT"];

export function usePropertyValueInsights(
  address: string,
  countryCode: string,
  options?: UsePropertyValueInsightsOptions
) {
  const [data, setData] = React.useState<PropertyValueInsightsResponse | null>(null);
  const [isLoading, setIsLoading] = React.useState(false);
  const hasProvider = PROVIDER_COUNTRIES.includes((countryCode ?? "").toUpperCase());

  const requestIdRef = React.useRef(0);

  React.useEffect(() => {
    if (!address.trim() || !hasProvider) {
      setData(null);
      setIsLoading(false);
      return;
    }

    const thisRequestId = ++requestIdRef.current;
    setIsLoading(true);

    const fetchOpts = {
      countryCode: countryCode || "IL",
      ...(options?.latitude != null &&
      options?.longitude != null &&
      Number.isFinite(options.latitude) &&
      Number.isFinite(options.longitude)
        ? { latitude: options.latitude, longitude: options.longitude }
        : {}),
      ...(options?.rawInputAddress != null ? { rawInputAddress: options.rawInputAddress } : {}),
      ...(options?.selectedFormattedAddress != null ? { selectedFormattedAddress: options.selectedFormattedAddress } : {}),
    };

    fetchPropertyValueInsights(address, fetchOpts)
      .then((res) => {
        if (thisRequestId === requestIdRef.current) setData(res);
      })
      .catch((err) => {
        if (thisRequestId === requestIdRef.current) {
          console.error("[usePropertyValueInsights]", err);
          setData({ message: "Failed to fetch", error: String(err) });
        }
      })
      .finally(() => {
        if (thisRequestId === requestIdRef.current) setIsLoading(false);
      });

    return () => {};
  }, [address, countryCode, hasProvider, options?.latitude, options?.longitude, options?.rawInputAddress, options?.selectedFormattedAddress]);

  return { data, isLoading };
}
