/**
 * Deterministic NYC building vs unit classification (US only).
 * Uses existing truth layers only: BigQuery `us_nyc_api_truth`, DOB filings, ACRIS deed history.
 * No UI, routes, or France.
 */

import { fetchAcrisNycTruthDeedHistory, type AcrisTruthDeed } from "./acris/acris-truth";
import { fetchDobNycBuildingInsights } from "./dob/dob-truth";
import { queryUSNYCApiTruthByAddress } from "./us-nyc-api-truth";

export type NycUnitClassification = "multi_unit_building" | "single_property" | "unknown";

export type NycAddressUnitClassificationResult = {
  classification: NycUnitClassification;
  should_prompt_for_unit: boolean;
  reason: string;
  dob_existing_units: number | null;
  dob_proposed_units: number | null;
  latest_sale_total_units: number | null;
  acris_has_multiple_deeds: boolean;
};

function distinctBblCount(deeds: readonly AcrisTruthDeed[]): number {
  const keys = new Set(deeds.map((d) => `${d.borough}|${d.block}|${d.lot}`));
  return keys.size;
}

/**
 * Multiple DEED rows at the same street address with more than one tax lot (borough/block/lot).
 * Deterministic signal for multi-parcel / unit-level indexing in ACRIS (conservative vs same-lot resale).
 */
function acrisMultiLotEvidence(deeds: readonly AcrisTruthDeed[]): boolean {
  return deeds.length > 1 && distinctBblCount(deeds) >= 2;
}

/**
 * Classify an NYC address for single vs multi-unit using DOB, truth `total_units`, and ACRIS only.
 *
 * Multi-unit if any:
 * - DOB `existing_units` > 1
 * - DOB `proposed_units` > 1
 * - Truth `latest_sale_total_units` > 1
 * - ACRIS: more than one deed **and** ≥2 distinct BBLs (same-address multi-lot)
 *
 * Single-property if not multi-unit and:
 * - DOB `existing_units` === 1 and proposed is not > 1, OR
 * - Truth row present with `latest_sale_total_units` === 1
 *
 * Unknown otherwise.
 */
export async function classifyNycAddressUnitType(params: {
  fullAddress: string;
  houseNumber: string;
  streetName: string;
  signal?: AbortSignal;
}): Promise<NycAddressUnitClassificationResult> {
  const { fullAddress, houseNumber, streetName, signal } = params;

  const [truth, dob, acris] = await Promise.all([
    queryUSNYCApiTruthByAddress(fullAddress),
    fetchDobNycBuildingInsights({ houseNumber, streetName, signal }),
    fetchAcrisNycTruthDeedHistory({ streetNumber: houseNumber, streetName, signal }),
  ]);

  let dob_existing_units: number | null = null;
  let dob_proposed_units: number | null = null;
  if (dob.success) {
    dob_existing_units = dob.existing_units;
    dob_proposed_units = dob.proposed_units;
  }

  const latest_sale_total_units = truth.latest_sale_total_units;

  let acris_has_multiple_deeds = false;
  let acrisMultiLot = false;
  if (acris.success) {
    acris_has_multiple_deeds = acris.has_multiple_deeds;
    acrisMultiLot = acrisMultiLotEvidence(acris.deeds);
  }

  const base: Pick<
    NycAddressUnitClassificationResult,
    "dob_existing_units" | "dob_proposed_units" | "latest_sale_total_units" | "acris_has_multiple_deeds"
  > = {
    dob_existing_units,
    dob_proposed_units,
    latest_sale_total_units,
    acris_has_multiple_deeds,
  };

  if (dob_existing_units != null && dob_existing_units > 1) {
    return {
      ...base,
      classification: "multi_unit_building",
      should_prompt_for_unit: true,
      reason: "dob_existing_units_gt_1",
    };
  }
  if (dob_proposed_units != null && dob_proposed_units > 1) {
    return {
      ...base,
      classification: "multi_unit_building",
      should_prompt_for_unit: true,
      reason: "dob_proposed_units_gt_1",
    };
  }
  if (latest_sale_total_units != null && latest_sale_total_units > 1) {
    return {
      ...base,
      classification: "multi_unit_building",
      should_prompt_for_unit: true,
      reason: "latest_sale_total_units_gt_1",
    };
  }
  if (acrisMultiLot) {
    return {
      ...base,
      classification: "multi_unit_building",
      should_prompt_for_unit: true,
      reason: "acris_multi_lot_deeds",
    };
  }

  const dobSingle =
    dob_existing_units === 1 && (dob_proposed_units == null || dob_proposed_units <= 1);
  const truthSingle =
    truth.has_truth_property_row === true && latest_sale_total_units === 1;

  if (dobSingle) {
    return {
      ...base,
      classification: "single_property",
      should_prompt_for_unit: false,
      reason: "dob_existing_units_eq_1",
    };
  }
  if (truthSingle) {
    return {
      ...base,
      classification: "single_property",
      should_prompt_for_unit: false,
      reason: "truth_latest_sale_total_units_eq_1",
    };
  }

  return {
    ...base,
    classification: "unknown",
    should_prompt_for_unit: false,
    reason: "insufficient_evidence",
  };
}

/**
 * True only when {@link classifyNycAddressUnitType} would return `multi_unit_building`
 * (deterministic mirror of `should_prompt_for_unit` on the result).
 */
export function getNycApartmentPromptEligibility(result: NycAddressUnitClassificationResult): boolean {
  return result.classification === "multi_unit_building";
}
