"use client";

import * as React from "react";
import { fetchPropertyValueInsights, type PropertyValueInsightsResponse } from "@/lib/property-value-api";

export type UsePropertyValueInsightsOptions = {
  latitude?: number;
  longitude?: number;
};

export function usePropertyValueInsights(
  address: string,
  isIsrael: boolean,
  options?: UsePropertyValueInsightsOptions
) {
  const [data, setData] = React.useState<PropertyValueInsightsResponse | null>(null);
  const [isLoading, setIsLoading] = React.useState(false);

  React.useEffect(() => {
    if (!address.trim() || !isIsrael) {
      setData(null);
      setIsLoading(false);
      return;
    }

    let cancelled = false;
    setIsLoading(true);

    const fetchOpts = {
      countryCode: "IL",
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
  }, [address, isIsrael, options?.latitude, options?.longitude]);

  return { data, isLoading };
}
