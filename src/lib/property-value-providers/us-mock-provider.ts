/**
 * US Mock Provider
 * Returns stable sample US property data for development/testing.
 * Does not call RentCast. Same UI structure as production.
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

    const avmValue = seeded(seed, 320000, 850000);
    const sqft = seeded(seed + 1, 1200, 3200);
    const pricePerSqft = Math.round(avmValue / sqft);
    const lastSalePrice = seeded(seed + 2, 280000, avmValue);
    const lastSaleDate = formatDate(new Date(Date.now() - seeded(seed + 3, 90, 800) * 24 * 60 * 60 * 1000));
    const avmRent = seeded(seed + 4, 2200, 6500);

    const beds = seeded(seed + 5, 2, 5);
    const baths = seeded(seed + 6, 2, 4);
    const yearBuilt = seeded(seed + 7, 1985, 2020);
    const propertyTypes = ["Single Family", "Condo", "Townhouse", "Multi-Family"];
    const propertyType = propertyTypes[seed % propertyTypes.length];

    const historyCount = seeded(seed + 8, 2, 6);
    const salesHistory = Array.from({ length: historyCount }, (_, i) => {
      const daysAgo = (i + 1) * seeded(seed + 9 + i, 180, 540);
      const price = seeded(seed + 20 + i, 200000, lastSalePrice);
      return {
        date: formatDate(new Date(Date.now() - daysAgo * 24 * 60 * 60 * 1000)),
        price,
      };
    }).sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    const compCount = seeded(seed + 30, 4, 12);
    const avgCompPrice = seeded(seed + 31, avmValue * 0.85, avmValue * 1.15);
    const avgCompPricePerSqft = seeded(seed + 32, pricePerSqft - 20, pricePerSqft + 30);

    const propertySizeM2 = Math.round(sqft * 0.0929 * 10) / 10;
    const pricePerM2 = sqft > 0 ? Math.round((lastSalePrice / sqft / 0.0929) * 100) / 100 : 0;

    const result: PropertyValueInsightsSuccess = {
      address: { city, street, house_number: houseNumber },
      match_quality: "exact_building",
      latest_transaction: {
        transaction_date: lastSaleDate,
        transaction_price: lastSalePrice,
        property_size: propertySizeM2,
        price_per_m2: pricePerM2,
      },
      current_estimated_value: {
        estimated_value: avmValue,
        estimated_price_per_m2: Math.round((avmValue / sqft / 0.0929) * 100) / 100,
        estimation_method: "Mock data for development. Not from live provider.",
        value_type: "sale",
      },
      building_summary_last_3_years: {
        transactions_count_last_3_years: salesHistory.filter((s) => new Date(s.date).getTime() >= Date.now() - 3 * 365.25 * 24 * 60 * 60 * 1000).length,
        transactions_count_last_5_years: salesHistory.length,
        latest_building_transaction_price: lastSalePrice,
        average_apartment_value_today: Math.round(salesHistory.reduce((a, s) => a + s.price, 0) / salesHistory.length),
      },
      market_value_source: "avm",
      fallback_level: "exact_property",
      avm_value: avmValue,
      avm_rent: avmRent,
      last_sale: { price: lastSalePrice, date: lastSaleDate },
      sales_history: salesHistory,
      nearby_comps: {
        avg_price: avgCompPrice,
        avg_price_per_sqft: avgCompPricePerSqft,
        count: compCount,
      },
      property_details: {
        beds,
        baths,
        sqft,
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
      median_home_value: seeded(seed + 40, 350000, 750000),
      median_household_income: seeded(seed + 41, 65000, 120000),
      population: seeded(seed + 42, 15000, 85000),
    };

    return result;
  }
}
