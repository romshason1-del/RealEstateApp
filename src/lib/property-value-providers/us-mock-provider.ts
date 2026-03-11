/**
 * US Mock Provider
 * Returns stable, internally consistent sample US property data for development/testing.
 * All values derived from a single base property model.
 */

import type { PropertyDataProvider } from "./provider-interface";
import type {
  PropertyValueInput,
  PropertyValueInsightsResult,
  PropertyValueInsightsSuccess,
} from "./types";

/** Simple stable hash from string to number */
function hashString(s: string): number {
  let h = 0;
  const str = s.trim().toLowerCase();
  for (let i = 0; i < str.length; i++) {
    const c = str.charCodeAt(i);
    h = (h << 5) - h + c;
    h = h & h;
  }
  return Math.abs(h);
}

/** Deterministic number in range [min, max] from seed */
function seeded(seed: number, min: number, max: number): number {
  const t = ((seed % 10000) + 10000) / 10000;
  return Math.round(min + t * (max - min));
}

function formatDate(d: Date): string {
  return d.toLocaleDateString("en-US", { month: "short", year: "numeric" });
}

export class UnitedStatesMockProvider implements PropertyDataProvider {
  readonly id = "us-mock";
  readonly name = "United States (Mock)";

  async getInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult> {
    const fullAddr = (input.fullAddress ?? "").trim() || `${input.street ?? ""} ${input.houseNumber ?? ""}, ${input.city ?? ""}, ${input.state ?? ""} ${input.zip ?? ""}`.trim();
    const seed = hashString(fullAddr || "default");

    const city = (input.city ?? "").trim() || "Miami";
    const street = (input.street ?? "").trim() || "Ocean Drive";
    const houseNumber = (input.houseNumber ?? "").trim() || "1500";

    // Single source of truth: base property model
    const base_market_value = seeded(seed, 350000, 750000);
    const area_price_growth_1y = seeded(seed + 1, 20, 80) / 10; // 2.0% to 8.0%
    const property_size = seeded(seed + 2, 1200, 3200);
    const rent_yield = 0.04 + seeded(seed + 3, 0, 25) / 1000; // 4.0% to 6.5% annual

    // Derived: Estimated Market Value = base
    const avmValue = base_market_value;
    const pricePerSqft = Math.round((avmValue / property_size) * 100) / 100;

    // Last Sale: ~1 year ago, before area growth. last_sale = current_value / (1 + growth)
    const lastSalePrice = Math.round(base_market_value / (1 + area_price_growth_1y / 100));
    const lastSaleDate = formatDate(new Date(Date.now() - 380 * 24 * 60 * 60 * 1000)); // ~1 year ago

    // Sales History: build backwards from last sale. i=0 is most recent (1 year ago).
    const historyCount = seeded(seed + 8, 2, 5);
    const growthFactor = 1 + area_price_growth_1y / 100;
    const salesHistory = Array.from({ length: historyCount }, (_, i) => {
      const yearsAgo = i + 1;
      const price = Math.round(lastSalePrice / Math.pow(growthFactor, i));
      const daysAgo = 365 * yearsAgo + seeded(seed + 10 + i, 0, 90);
      return {
        date: formatDate(new Date(Date.now() - daysAgo * 24 * 60 * 60 * 1000)),
        price,
      };
    }).sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    // Ensure last sale matches latest in sales history
    const latestFromHistory = salesHistory[0];
    const effectiveLastSalePrice = latestFromHistory?.price ?? lastSalePrice;
    const effectiveLastSaleDate = latestFromHistory?.date ?? lastSaleDate;

    // Estimated Rent: market value * annual rent yield / 12
    const avmRent = Math.round((base_market_value * rent_yield) / 12);

    // Nearby Comps: similar range to market value (95%–105%)
    const compCount = seeded(seed + 30, 4, 10);
    const compVariance = 0.95 + (seed % 11) / 100; // 0.95 to 1.05
    const avgCompPrice = Math.round(base_market_value * compVariance);
    const avgCompPricePerSqft = Math.round((avgCompPrice / property_size) * 100) / 100;

    // FHFA trend: consistent with area_price_growth_1y
    const hpiIndex = 350 + (seed % 150);
    const change_1y_percent = area_price_growth_1y;

    // Neighborhood stats: area median in similar range
    const medianHomeValue = Math.round(base_market_value * (0.92 + (seed % 9) / 100));
    const medianIncome = seeded(seed + 41, 65000, 120000);
    const population = seeded(seed + 42, 15000, 85000);

    const beds = seeded(seed + 5, 2, 5);
    const baths = seeded(seed + 6, 2, 4);
    const yearBuilt = seeded(seed + 7, 1985, 2020);
    const propertyTypes = ["Single Family", "Condo", "Townhouse", "Multi-Family"];
    const propertyType = propertyTypes[seed % propertyTypes.length];

    const propertySizeM2 = Math.round(property_size * 0.0929 * 10) / 10;
    const pricePerM2 = property_size > 0 ? Math.round((effectiveLastSalePrice / property_size / 0.0929) * 100) / 100 : 0;

    const result: PropertyValueInsightsSuccess = {
      address: { city, street, house_number: houseNumber },
      match_quality: "exact_building",
      latest_transaction: {
        transaction_date: effectiveLastSaleDate,
        transaction_price: effectiveLastSalePrice,
        property_size: propertySizeM2,
        price_per_m2: pricePerM2,
      },
      current_estimated_value: {
        estimated_value: avmValue,
        estimated_price_per_m2: Math.round((avmValue / property_size / 0.0929) * 100) / 100,
        estimation_method: "Mock data for development. Not from live provider.",
        value_type: "sale",
      },
      building_summary_last_3_years: {
        transactions_count_last_3_years: salesHistory.filter((s) => new Date(s.date).getTime() >= Date.now() - 3 * 365.25 * 24 * 60 * 60 * 1000).length,
        transactions_count_last_5_years: salesHistory.length,
        latest_building_transaction_price: effectiveLastSalePrice,
        average_apartment_value_today: Math.round(salesHistory.reduce((a, s) => a + s.price, 0) / salesHistory.length),
      },
      market_value_source: "avm",
      fallback_level: "exact_property",
      avm_value: avmValue,
      avm_rent: avmRent,
      last_sale: { price: effectiveLastSalePrice, date: effectiveLastSaleDate },
      sales_history: salesHistory,
      nearby_comps: {
        avg_price: avgCompPrice,
        avg_price_per_sqft: avgCompPricePerSqft,
        count: compCount,
      },
      property_details: {
        beds,
        baths,
        sqft: property_size,
        year_built: yearBuilt,
        property_type: propertyType,
      },
      unit_required: false,
      explanation: "Mock data for development. Not from live provider.",
      source: "mock",
      debug: {
        active_provider_id: "us-mock",
        provider_configured: true,
        property_found: true,
        avm_value_found: true,
        avm_rent_found: true,
        sales_history_found: true,
        comps_found: true,
        market_value_source: "avm",
      },
    };

    (result as Record<string, unknown>).neighborhood_stats = {
      median_home_value: medianHomeValue,
      median_household_income: medianIncome,
      population,
      median_rent: 1850,
      population_growth_percent: 3.4,
      income_growth_percent: 5.1,
    };

    (result as Record<string, unknown>).market_trend = {
      hpi_index: hpiIndex,
      change_1y_percent,
      latest_date: "2024-10",
    };

    (result as Record<string, unknown>).data_sources = ["RentCast", "Zillow", "Redfin"];
    (result as Record<string, unknown>).us_match_confidence = "high";

    return result;
  }
}
