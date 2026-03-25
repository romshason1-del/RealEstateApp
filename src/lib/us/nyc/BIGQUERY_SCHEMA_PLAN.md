# NYC BigQuery table plan

**Dataset (suggested):** `us_nyc` (configurable).  
**Convention:** Prefix `us_nyc_` for all NYC warehouse objects. **No shared tables with France.**

---

## RAW TABLES

### `us_nyc_raw_sales`

Purpose: Store **verbatim** Rolling Sales rows as ingested (plus provenance columns).

| Column | Type | Notes |
|--------|------|--------|
| `ingestion_batch_id` | STRING | UUID or batch label per load |
| `ingestion_ts` | TIMESTAMP | When row was loaded |
| `source_file_name` | STRING | Original file name |
| `source_row_number` | INT64 | 1-based row index in file (if available) |
| `raw_record` | JSON | **Entire** source row as JSON (flexible for column drift) |
| *optional* | | If team prefers flat raw: mirror **all** DOF CSV columns as STRING (add in DDL when format is fixed) |

**Partitioning (recommended):** `DATE(ingestion_ts)` or `tax_year` extracted into a dedicated column once raw shape is known.  
**Clustering (optional):** `borough_code`, `block`, `lot` if extracted to top-level columns in a later raw DDL revision.

---

### `us_nyc_raw_pluto`

Purpose: Store **verbatim** MapPLUTO/PLUTO attributes per release.

| Column | Type | Notes |
|--------|------|--------|
| `ingestion_batch_id` | STRING | Per vintage load |
| `ingestion_ts` | TIMESTAMP | Load time |
| `source_vintage` | STRING | e.g. `24v2` / label from distributor |
| `source_file_name` | STRING | |
| `raw_record` | JSON | Full attribute record as JSON, **or** flat STRING columns mirroring shapefile/dbf field names |

**Note:** If source is geospatial, geometry may live in raw as WKT in JSON, or in a dedicated `GEOGRAPHY` column in a follow-up DDL — **not required for foundation doc.**

---

## NORMALIZED TABLES

### `us_nyc_sales_normalized`

Purpose: Typed, join-ready sales facts keyed to tax lot and time.

| Column | Type | Description |
|--------|------|-------------|
| `sale_id` | STRING | Surrogate: hash or UUID from (borough, block, lot, sale_date, sale_price, address line) — **define in normalization plan** |
| `borough_code` | INT64 | 1–5 Manhattan…Staten Island |
| `block` | INT64 | Normalized block |
| `lot` | INT64 | Normalized lot |
| `bbl` | STRING | Canonical `borough_code` + zero-padded block/lot per NYC rules |
| `sale_date` | DATE | |
| `sale_price` | NUMERIC | **Unit documented in NORMALIZATION_PLAN** (dollars) |
| `address_normalized` | STRING | Optional standardized address |
| `building_class_at_sale` | STRING | From source |
| `tax_class_at_sale` | STRING | From source |
| `residential_units` | INT64 | nullable |
| `commercial_units` | INT64 | nullable |
| `land_square_feet` | NUMERIC | nullable |
| `gross_square_feet` | NUMERIC | nullable |
| `year_built` | INT64 | nullable (often from sales file; may defer to PLUTO) |
| `zip_code` | STRING | nullable |
| `ingestion_batch_id` | STRING | Traceability |

---

### `us_nyc_pluto_normalized`

Purpose: One row per tax lot (current PLUTO vintage).

| Column | Type | Description |
|--------|------|-------------|
| `pluto_row_id` | STRING | Surrogate |
| `borough_code` | INT64 | |
| `block` | INT64 | |
| `lot` | INT64 | |
| `bbl` | STRING | |
| `bldgclass` | STRING | Building class code |
| `landuse` | STRING | Land use |
| `lotarea` | NUMERIC | |
| `bldgarea` | NUMERIC | |
| `numfloors` | NUMERIC | nullable |
| `unitsres` | INT64 | nullable |
| `unitstotal` | INT64 | nullable |
| `yearbuilt` | INT64 | nullable |
| `yearalter1` | INT64 | nullable |
| `yearalter2` | INT64 | nullable |
| `zipcode` | STRING | nullable |
| `address` | STRING | nullable (field name per vintage) |
| `source_vintage` | STRING | PLUTO release |
| `ingestion_batch_id` | STRING | |

*Adjust exact PLUTO field set to match downloaded vintage; this is the **target normalized shape**.*

---

## HELPER / TRUTH TABLES (spec only — empty in Phase 1)

| Table | Purpose (future) |
|-------|------------------|
| `us_nyc_property_latest_facts` | Lot-level rollups: latest known sale, areas, class, flags |
| `us_nyc_building_unit_flags` | Indicators for multi-unit / condo / etc. (from PLUTO + rules) |
| `us_nyc_transaction_truth` | Curated transaction lineage (dedupe, outliers, data-quality flags) |
| `us_nyc_display_context_truth` | UI-ready labels (e.g. street vs lot vs building) — **no UI in this phase** |

**Phase 1:** Create tables optionally with empty schema stub, or **document only** and create DDL when logic exists.

---

## DDL delivery

Executable `CREATE TABLE` statements can be added under `scripts/us/nyc/ddl/` in a follow-up task once BigQuery dataset location and partitioning policy are fixed.
