/**
 * Mock Provider
 * Returns sample data for development and testing.
 * Keeps the same UI structure as the official provider.
 */

import type { PropertyDataProvider } from "./provider-interface";
import type { PropertyValueInput, PropertyValueInsightsResult } from "./types";

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

export class MockProvider implements PropertyDataProvider {
  readonly id = "mock";
  readonly name = "Mock (Development)";

  async getInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult> {
    const city = (input.city ?? "").trim() || "Tel Aviv";
    const street = (input.street ?? "").trim() || "Rothschild";
    const houseNumber = (input.houseNumber ?? "").trim() || "10";

    const salePrice = 3_250_000;
    const propertySize = 85;
    const pricePerM2 = Math.round(salePrice / propertySize);
    const transactionDate = formatDate(new Date(Date.now() - 180 * 24 * 60 * 60 * 1000));

    return {
      address: { city, street, house_number: houseNumber },
      match_quality: "exact_building",
      latest_transaction: {
        transaction_date: transactionDate,
        transaction_price: salePrice,
        property_size: propertySize,
        price_per_m2: pricePerM2,
      },
      current_estimated_value: {
        estimated_value: salePrice,
        estimated_price_per_m2: pricePerM2,
        estimation_method: "Based on mock data for development. This is NOT an official appraisal.",
      },
      building_summary_last_3_years: {
        transactions_count_last_3_years: 3,
        latest_building_transaction_price: salePrice,
        average_apartment_value_today: 3_100_000,
      },
      explanation: `Mock match for ${city}, ${street} ${houseNumber}. 3 sample transaction(s) for this building.`,
      source: "mock",
    };
  }
}
