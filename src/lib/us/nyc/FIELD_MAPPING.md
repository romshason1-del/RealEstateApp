# NYC source → warehouse field mapping

Maps **NYC Department of Finance Rolling Sales** and **MapPLUTO/PLUTO** public fields to **`us_nyc_raw_*`** and **`us_nyc_*_normalized`**.

> Source column names vary by file vintage. Treat names below as **canonical NYC Open Data / DOF CSV headers** — **verify against the file you download** before locking ETL.

---

## 1) NYC DOF Rolling Sales → `us_nyc_raw_sales` / `us_nyc_sales_normalized`

### Typical Rolling Sales CSV columns (DOF)

| Source column (typical) | Target: `raw_record` JSON key | Target: `us_nyc_sales_normalized` |
|------------------------|-------------------------------|-----------------------------------|
| BOROUGH | `BOROUGH` | `borough_code` (INT, 1–5) |
| BLOCK | `BLOCK` | `block` (INT) |
| LOT | `LOT` | `lot` (INT) |
| EASEMENT | `EASEMENT` | optional string / ignore if empty |
| BUILDING CLASS CATEGORY | `BUILDING CLASS CATEGORY` | optional metadata |
| TAX CLASS AT PRESENT | `TAX CLASS AT PRESENT` | optional |
| BUILDING CLASS AT PRESENT | `BUILDING CLASS AT PRESENT` | optional |
| ADDRESS | `ADDRESS` | `address_normalized` |
| APARTMENT NUMBER | `APARTMENT NUMBER` | optional (unit) |
| ZIP CODE | `ZIP CODE` | `zip_code` |
| RESIDENTIAL UNITS | `RESIDENTIAL UNITS` | `residential_units` |
| COMMERCIAL UNITS | `COMMERCIAL UNITS` | `commercial_units` |
| LAND SQUARE FEET | `LAND SQUARE FEET` | `land_square_feet` |
| GROSS SQUARE FEET | `GROSS SQUARE FEET` | `gross_square_feet` |
| YEAR BUILT | `YEAR BUILT` | `year_built` |
| TAX CLASS AT TIME OF SALE | `TAX CLASS AT TIME OF SALE` | `tax_class_at_sale` |
| BUILDING CLASS AT TIME OF SALE | `BUILDING CLASS AT TIME OF SALE` | `building_class_at_sale` |
| SALE PRICE | `SALE PRICE` | `sale_price` |
| SALE DATE | `SALE DATE` | `sale_date` (parse to DATE) |

**Raw layer:** Entire row stored under `raw_record` plus `ingestion_batch_id`, `ingestion_ts`, `source_file_name`, `source_row_number`.

**Derived:** `bbl` computed from borough/block/lot per NYC BBL rules in normalization SQL.

**Non–arms-length / invalid sales:** `$0` and institutional codes — **flag in later** `us_nyc_transaction_truth`; not in Phase 1 heuristics.

---

## 2) MapPLUTO / PLUTO → `us_nyc_raw_pluto` / `us_nyc_pluto_normalized`

### Typical MapPLUTO attributes (names vary slightly by vintage)

| Source field (typical) | Target: `raw_record` | Target: `us_nyc_pluto_normalized` |
|------------------------|----------------------|-------------------------------------|
| Borough | `Borough` or `BOROUGH` | `borough_code` |
| Block | `Block` | `block` |
| Lot | `Lot` | `lot` |
| BBL | `BBL` if present | `bbl` (validate vs computed) |
| BldgClass | `BldgClass` | `bldgclass` |
| LandUse | `LandUse` | `landuse` |
| LotArea | `LotArea` | `lotarea` |
| BldgArea | `BldgArea` | `bldgarea` |
| NumFloors | `NumFloors` | `numfloors` |
| UnitsRes | `UnitsRes` | `unitsres` |
| UnitsTotal | `UnitsTotal` | `unitstotal` |
| YearBuilt | `YearBuilt` | `yearbuilt` |
| YearAlter1 | `YearAlter1` | `yearalter1` |
| YearAlter2 | `YearAlter2` | `yearalter2` |
| ZipCode | `ZipCode` | `zipcode` |
| Address | `Address` | `address` |

**Raw layer:** Full record in `raw_record` + `source_vintage`, `ingestion_batch_id`, `ingestion_ts`.

**Geometry:** If using shapefile/geo package, store geometry in raw as WKT or load to a **separate** geo table later; **not** required for scalar foundation.

---

## Join key

- **Primary:** `bbl` (and components `borough_code`, `block`, `lot`).
- **Sales ↔ PLUTO:** join `us_nyc_sales_normalized.bbl` to `us_nyc_pluto_normalized.bbl` for lot attributes at time of analysis (vintage choice documented per release).
