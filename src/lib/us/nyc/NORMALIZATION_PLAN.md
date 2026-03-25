# NYC normalization plan (raw → normalized)

**No valuation.** **No France.** Converts staged raw JSON (or flat raw) into typed normalized tables.

## Principles

1. **Idempotent batches:** Every load carries `ingestion_batch_id`; normalized tables can be rebuilt from raw per batch or full-refresh per vintage.
2. **Types:** Dates → `DATE`; money → `NUMERIC` (precision TBD); integers → `INT64`; free text → `STRING`.
3. **BBL:** Single canonical string + integer components. Rules:
   - Borough: 1–5 (Manhattan, Bronx, Brooklyn, Queens, Staten Island).
   - Block: 5-digit zero-padded in BBL string (confirm NYC standard for your ETL).
   - Lot: 4-digit zero-padded in BBL string (confirm against NYC documentation).
4. **Sale price:** Store **dollar amount** as `NUMERIC(38, 2)` unless team standardizes integer cents — **document in DDL**.
5. **Invalid / placeholder sales:** Rolling Sales uses `0` or non-market markers — **do not drop in Phase 1** unless explicitly required; later `us_nyc_transaction_truth` applies rules.

## Sales: `us_nyc_raw_sales` → `us_nyc_sales_normalized`

1. Extract fields from `raw_record` (or read flat columns).
2. Parse `SALE DATE` to `DATE` (handle `MM/DD/YYYY` or source format).
3. Parse `SALE PRICE` to `NUMERIC`; strip commas if present in string source.
4. Cast `BOROUGH`, `BLOCK`, `LOT` to integers; validate ranges.
5. Compute `bbl` from components.
6. Generate `sale_id` stable hash (e.g. `FARM_FINGERPRINT` or `SHA256` of key fields) — **define in SQL**.
7. Insert with `ingestion_batch_id`.

## PLUTO: `us_nyc_raw_pluto` → `us_nyc_pluto_normalized`

1. Extract from `raw_record` (or shapefile row).
2. Map borough: source may be name or code — normalize to `borough_code` 1–5.
3. Cast numeric fields; handle empty strings as NULL.
4. Compute or validate `bbl`.
5. Set `source_vintage` from batch metadata.
6. Generate `pluto_row_id` (surrogate).

## SQL location (future)

Batch SQL or dbt models should live under **`scripts/us/nyc/sql/`** or a dedicated **`etl/us/nyc/`** repo area — **add when implementing** (still US-only).

## Testing (future)

- Row count: raw vs normalized.
- Null rate on `bbl`, `sale_date`, `sale_price` for sales.
- Sample joins sales ↔ pluto on `bbl`.
