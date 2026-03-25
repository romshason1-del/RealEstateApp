/**
 * NYC ingestion — shared TypeScript types for scripts (no runtime data, no France imports).
 */

/** Provenance for any NYC raw row. */
export type NYCIngestionProvenance = {
  ingestion_batch_id: string;
  ingestion_ts_iso: string;
  source_file_name: string;
  source_row_number?: number;
};

/** Identifiers for joining NYC tax lots (normalized layer). */
export type NYCBBLParts = {
  borough_code: number;
  block: number;
  lot: number;
  bbl: string;
};
