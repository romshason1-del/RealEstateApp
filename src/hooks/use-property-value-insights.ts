"use client";

import * as React from "react";
import { fetchPropertyValueInsights, type PropertyValueInsightsResponse } from "@/lib/property-value-api";

export type UsePropertyValueInsightsOptions = {
  latitude?: number;
  longitude?: number;
  countryCode?: string;
};

/** Countries that have an official property data provider */
const PROVIDER_COUNTRIES = ["IL", "US", "UK", "GB"];

export function usePropertyValueInsights(
  address: string,
  countryCode: string,
  options?: UsePropertyValueInsightsOptions
) {
  const [data, setData] = React.useState<PropertyValueInsightsResponse | null>(null);
  const [isLoading, setIsLoading] = React.useState(false);
  const hasProvider = PROVIDER_COUNTRIES.includes((countryCode ?? "").toUpperCase());

  React.useEffect(() => {
    if (!address.trim() || !hasProvider) {
      setData(null);
      setIsLoading(false);
      return;
    }

    let cancelled = false;
    setIsLoading(true);

    const fetchOpts = {
      countryCode: countryCode || "IL",
      ...(options?.latitude != null &&
      options?.longitude != null &&
      Number.isFinite(options.latitude) &&
      Number.isFinite(options.longitude)
        ? { latitude: options.latitude, longitude: options.longitude }
        : {}),
    };

    fetchPropertyValueInsights(address, fetchOpts)
      .then((res) => {
        if (!cancelled) setData(res);
      })
      .catch((err) => {
        if (!cancelled) {
          console.error("[usePropertyValueInsights]", err);
          setData({ message: "Failed to fetch", error: String(err) });
        }
      })
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => { cancelled = true; };
  }, [address, countryCode, hasProvider, options?.latitude, options?.longitude]);

  return { data, isLoading };
}
