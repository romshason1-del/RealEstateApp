/**
 * Property Value Provider Interface
 * Implementations fetch transaction data from various sources.
 */

import type { PropertyValueInput, PropertyValueInsightsResult } from "./types";

export interface PropertyDataProvider {
  /** Provider identifier (e.g. "israel-official", "mock") */
  readonly id: string;

  /** Display name for UI/debug */
  readonly name: string;

  /**
   * Fetch property value insights for the given address.
   * Returns success with transaction data, or error/no-match message.
   */
  getInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult>;
}
