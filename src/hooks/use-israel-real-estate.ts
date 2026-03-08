"use client";

import * as React from "react";
import { fetchIsraelRealEstate, type IsraelRealEstateResponse } from "@/lib/israel-real-estate";

export function useIsraelRealEstate(address: string, isIsrael: boolean, propertyAreaSqm?: number) {
  const [data, setData] = React.useState<IsraelRealEstateResponse | null>(null);
  const [isLoading, setIsLoading] = React.useState(false);

  React.useEffect(() => {
    if (!address.trim() || !isIsrael) {
      setData(null);
      setIsLoading(false);
      return;
    }

    let cancelled = false;
    setIsLoading(true);

    fetchIsraelRealEstate(address, propertyAreaSqm)
      .then((res) => {
        if (!cancelled) {
          setData(res);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          console.error("[useIsraelRealEstate] Cannot show 900k - REASON: fetch threw", err);
          setData((prev) => prev);
        }
      })
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => { cancelled = true; };
  }, [address, isIsrael, propertyAreaSqm]);

  return { data, isLoading };
}
