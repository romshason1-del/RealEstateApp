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
  /** US NYC: optional unit/lot — passed as `unit_or_lot` to `/api/us/nyc-app-output` (same as Apply flow). */
  unitOrLot?: string;
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
    const isFR = (countryCode ?? "").toUpperCase() === "FR";
    if (isFR) setData(null);
    setIsLoading(true);
    const rawForFrance = (opts?.rawInputAddress || (isFR ? address : "")).trim();
    const cc = (countryCode ?? "").toUpperCase();
    const fetchOpts = {
      countryCode: countryCode || "IL",
      ...(opts?.latitude != null && opts?.longitude != null && Number.isFinite(opts.latitude) && Number.isFinite(opts.longitude)
        ? { latitude: opts.latitude, longitude: opts.longitude }
        : {}),
      ...(isFR && rawForFrance ? { rawInputAddress: rawForFrance } : opts?.rawInputAddress != null ? { rawInputAddress: opts.rawInputAddress } : {}),
      ...(opts?.selectedFormattedAddress != null ? { selectedFormattedAddress: opts.selectedFormattedAddress } : {}),
      ...((opts?.aptNumber ?? "").toString().trim() ? { aptNumber: (opts?.aptNumber ?? "").toString().trim() } : {}),
      ...(opts?.postcode != null ? { postcode: opts.postcode } : {}),
      ...(cc === "US" && (opts?.unitOrLot ?? "").trim() ? { unitOrLot: (opts?.unitOrLot ?? "").trim() } : {}),
      signal: abortRef.current.signal,
    };
    const aptSent = (fetchOpts as { aptNumber?: string }).aptNumber ?? "";
    if ((countryCode ?? "").toUpperCase() === "FR") {
      console.log("[FR_LOT_FETCH] starting_new_request", {
        requestId: thisRequestId,
        aptNumber: (opts?.aptNumber ?? "").toString().trim() || null,
        refetchTrigger: opts?.refetchTrigger ?? null,
        address,
      });
    }
    fetchPropertyValueInsights(address, fetchOpts)
      .then((res) => {
        if (thisRequestId === requestIdRef.current) {
          if (isFR && aptSent) {
            const rd = (res as { fr_runtime_debug?: Record<string, unknown> })?.fr_runtime_debug;
            console.log("[FR_LOT_DEBUG]", {
              requestId: thisRequestId,
              raw_apt_number_param: rd?.raw_apt_number_param,
              submitted_lot: rd?.submitted_lot,
              fr_lot_submitted: rd?.fr_lot_submitted,
            });
          }
          setData(res);
        }
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
  }, [address, countryCode, hasProvider, options?.latitude, options?.longitude, options?.rawInputAddress, options?.selectedFormattedAddress, options?.aptNumber, options?.postcode, options?.refetchTrigger, options?.unitOrLot, doFetch]);

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
