/**
 * US Property Value Orchestrator
 * Priority: RentCast (when configured) → Zillow Research → Redfin Data Center
 * When both Zillow and Redfin exist in cache, uses weighted consensus.
 */

import type { PropertyDataProvider } from "./provider-interface";
import type {
  PropertyValueInput,
  PropertyValueInsightsResult,
  PropertyValueInsightsSuccess,
  PropertyValueInsightsNoMatch,
  PropertyValueInsightsError,
} from "./types";
import { UnitedStatesRentcastProvider } from "./us-rentcast-provider";
import { UnitedStatesMockProvider } from "./us-mock-provider";
import { lookupAsync, loadFromFile } from "./us-market-data-cache";
import { isUSMockEnabled, isUSRentcastConfigured } from "./config";

const rentcast = new UnitedStatesRentcastProvider();
const usMock = new UnitedStatesMockProvider();

function buildMarketFallback(
  input: PropertyValueInput,
  cached: { estimated_area_price?: number; median_sale_price?: number; median_price_per_sqft?: number; market_trend_yoy?: number; inventory_signal?: number; days_on_market?: number; sources: string[] }
): PropertyValueInsightsSuccess {
  const city = (input.city ?? "").trim() || "Unknown";
  const street = (input.street ?? "").trim() || "Unknown";
  const price = cached.estimated_area_price ?? cached.median_sale_price ?? 0;
  const dataSources: ("RentCast" | "Zillow" | "Redfin")[] = cached.sources.includes("zillow")
    ? cached.sources.includes("redfin")
      ? ["Zillow", "Redfin"]
      : ["Zillow"]
    : cached.sources.includes("redfin")
      ? ["Redfin"]
      : [];
  const confidence: "high" | "medium" | "low" =
    dataSources.length >= 2 ? "medium" : dataSources.length === 1 ? "low" : "low";

  return {
    address: { city, street, house_number: (input.houseNumber ?? "").trim() },
    match_quality: "no_reliable_match",
    latest_transaction: { transaction_date: "", transaction_price: 0, property_size: 0, price_per_m2: 0 },
    current_estimated_value:
      price > 0
        ? {
            estimated_value: price,
            estimated_price_per_m2: 0,
            estimation_method: "Area-level market data from Zillow Research and/or Redfin Data Center. Not a property appraisal.",
            value_type: "sale",
          }
        : null,
    building_summary_last_3_years: null,
    market_value_source: "none",
    source: "us-orchestrator",
    avm_value: price > 0 ? price : undefined,
    estimated_area_price: price > 0 ? price : null,
    median_sale_price: cached.median_sale_price ?? null,
    median_price_per_sqft: cached.median_price_per_sqft ?? null,
    market_trend: cached.market_trend_yoy != null ? { change_1y_percent: cached.market_trend_yoy } : null,
    inventory_signal: cached.inventory_signal ?? null,
    days_on_market: cached.days_on_market ?? null,
    data_sources: dataSources.length > 0 ? [...dataSources] : undefined,
    us_match_confidence: confidence,
  };
}

export class USOrchestratorProvider implements PropertyDataProvider {
  readonly id = "us-orchestrator";
  readonly name = "United States (RentCast + Zillow + Redfin)";

  async getInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult> {
    if (isUSMockEnabled()) {
      return usMock.getInsights(input);
    }

    await loadFromFile().catch(() => {});

    const zip = (input.zip ?? "").trim();
    const city = (input.city ?? "").trim();
    const state = (input.state ?? "").trim();
    const cached = await lookupAsync(zip, city, state);

    if (isUSRentcastConfigured()) {
      const rentcastResult = await rentcast.getInsights(input);

      if ("error" in rentcastResult && (rentcastResult as PropertyValueInsightsError).error === "UNIT_REQUIRED") {
        return rentcastResult;
      }

      const hasRentcastData =
        rentcastResult &&
        !("error" in rentcastResult) &&
        "avm_value" in rentcastResult &&
        (rentcastResult as PropertyValueInsightsSuccess).avm_value != null &&
        (rentcastResult as PropertyValueInsightsSuccess).avm_value! > 0;

      if (hasRentcastData) {
        const success = rentcastResult as PropertyValueInsightsSuccess;
        const sources: ("RentCast" | "Zillow" | "Redfin")[] = ["RentCast"];
        if (cached?.sources.includes("zillow")) sources.push("Zillow");
        if (cached?.sources.includes("redfin")) sources.push("Redfin");
        return {
          ...success,
          data_sources: sources,
          us_match_confidence: "high",
          estimated_area_price: cached?.estimated_area_price ?? cached?.median_sale_price ?? success.estimated_area_price,
          median_sale_price: cached?.median_sale_price ?? success.median_sale_price,
          median_price_per_sqft: cached?.median_price_per_sqft ?? success.median_price_per_sqft,
          market_trend: cached?.market_trend_yoy != null ? { change_1y_percent: cached.market_trend_yoy } : success.market_trend,
          inventory_signal: cached?.inventory_signal ?? success.inventory_signal,
          days_on_market: cached?.days_on_market ?? success.days_on_market,
        };
      }

      if (cached && ((cached.estimated_area_price ?? cached.median_sale_price) ?? 0) > 0) {
        return buildMarketFallback(input, cached);
      }

      return rentcastResult;
    }

    if (cached && ((cached.estimated_area_price ?? cached.median_sale_price) ?? 0) > 0) {
      return buildMarketFallback(input, cached);
    }

    return {
      message: "No property data found for this address.",
      error: "NO_MATCH",
    };
  }
}
