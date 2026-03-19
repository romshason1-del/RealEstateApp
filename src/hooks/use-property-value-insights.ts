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
  /** France: apartment/lot number for multi-unit buildings */
  aptNumber?: string;
  /** France: postcode from Google address_components (avoids "Postcode required") */
  postcode?: string;
  /** Increment to force refetch (e.g. when user clicks Search for apartment) */
  refetchTrigger?: number;
};

/** Countries that have an official property data provider. FR uses properties_france (DVF import) + optional DVF API fallback. */
const PROVIDER_COUNTRIES = ["IL", "US", "UK", "GB", "IT", "FR"];

export function usePropertyValueInsights(
  address: string,
  countryCode: string,
  options?: UsePropertyValueInsightsOptions
) {
  const [data, setData] = React.useState<PropertyValueInsightsResponse | null>(null);
  const [isLoading, setIsLoading] = React.useState(false);
  const hasProvider = PROVIDER_COUNTRIES.includes((countryCode ?? "").toUpperCase());
  const requestIdRef = React.useRef(0);
  const optionsRef = React.useRef(options);
  optionsRef.current = options;
  const abortRef = React.useRef<AbortController | null>(null);

  const doFetch = React.useCallback(() => {
    if (!address.trim() || !hasProvider) return;
    const opts = optionsRef.current;
    const thisRequestId = ++requestIdRef.current;
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    setIsLoading(true);
    const fetchOpts = {
      countryCode: countryCode || "IL",
      ...(opts?.latitude != null && opts?.longitude != null && Number.isFinite(opts.latitude) && Number.isFinite(opts.longitude)
        ? { latitude: opts.latitude, longitude: opts.longitude }
        : {}),
      ...(opts?.rawInputAddress != null ? { rawInputAddress: opts.rawInputAddress } : {}),
      ...(opts?.selectedFormattedAddress != null ? { selectedFormattedAddress: opts.selectedFormattedAddress } : {}),
      ...((opts?.aptNumber ?? "").toString().trim() ? { aptNumber: (opts?.aptNumber ?? "").toString().trim() } : {}),
      ...(opts?.postcode != null ? { postcode: opts.postcode } : {}),
      signal: abortRef.current.signal,
    };
    fetchPropertyValueInsights(address, fetchOpts)
      .then((res) => {
        if (thisRequestId === requestIdRef.current) setData(res);
      })
      .catch((err) => {
        if (thisRequestId === requestIdRef.current) {
          if ((err as any)?.name !== "AbortError") console.error("[usePropertyValueInsights]", err);
          setData({ message: "Failed to fetch", error: String(err) });
        }
      })
      .finally(() => {
        if (thisRequestId === requestIdRef.current) setIsLoading(false);
      });
  }, [address, countryCode, hasProvider]);

  React.useEffect(() => {
    if (!address.trim() || !hasProvider) {
      abortRef.current?.abort();
      abortRef.current = null;
      setData(null);
      setIsLoading(false);
      return;
    }
    doFetch();
  }, [address, countryCode, hasProvider, options?.latitude, options?.longitude, options?.rawInputAddress, options?.selectedFormattedAddress, options?.aptNumber, options?.postcode, options?.refetchTrigger, doFetch]);

  React.useEffect(() => {
    return () => {
      abortRef.current?.abort();
      abortRef.current = null;
    };
  }, []);

  const refetch = React.useCallback(() => {
    doFetch();
  }, [doFetch]);

  return { data, isLoading, refetch };
}
