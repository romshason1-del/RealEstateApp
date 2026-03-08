"use client";

import * as React from "react";
import { fetchIsraelRealEstate, type IsraelRealEstateResponse } from "@/lib/israel-real-estate";

export function useIsraelRealEstate(address: string, isIsrael: boolean, propertyAreaSqm?: number) {
  const [data, setData] = React.useState<IsraelRealEstateResponse | null>(null);
  const [isLoading, setIsLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!address.trim() || !isIsrael) {
      setData(null);
      setError(null);
      setIsLoading(false);
      return;
    }

    let cancelled = false;
    setIsLoading(true);
    setError(null);

    fetchIsraelRealEstate(address, propertyAreaSqm)
      .then((res) => {
        if (!cancelled) {
          setData(res);
          if (res.error) setError(res.error);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Connection failed");
          setData({ transactions: [], avgPrice: null, avgPricePerSqm: null, lastSaleDate: null, lastSalePrice: null, transactionCount: 0, source: "data.gov.il", error: "Connection failed" });
        }
      })
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => { cancelled = true; };
  }, [address, isIsrael, propertyAreaSqm]);

  return { data, isLoading, error };
}
