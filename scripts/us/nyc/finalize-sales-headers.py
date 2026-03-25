"""
Build us_nyc_sales_FINAL.csv with canonical headers from us_nyc_sales_clean.csv.

Handles merged NYC Rolling Sales Excel exports:
- Standard block: col0 = BOROUGH
- Shifted blocks: col1 = NEIGHBORHOOD, BOROUGH column index varies (21–24) per file

US/NYC only — does not touch France.
"""
import csv

import pandas as pd

INPUT = "data/us/nyc/raw/sales/us_nyc_sales_clean.csv"
OUTPUT = "data/us/nyc/raw/sales/us_nyc_sales_FINAL.csv"

FINAL_COLS = [
    "BOROUGH",
    "NEIGHBORHOOD",
    "BUILDING_CLASS_CATEGORY",
    "TAX_CLASS_PRESENT",
    "BLOCK",
    "LOT",
    "EASE_MENT",
    "BUILDING_CLASS_PRESENT",
    "ADDRESS",
    "APARTMENT_NUMBER",
    "ZIP_CODE",
    "RESIDENTIAL_UNITS",
    "COMMERCIAL_UNITS",
    "TOTAL_UNITS",
    "LAND_SQUARE_FEET",
    "GROSS_SQUARE_FEET",
    "YEAR_BUILT",
    "TAX_CLASS_AT_TIME_OF_SALE",
    "BUILDING_CLASS_AT_TIME_OF_SALE",
    "SALE_PRICE",
    "SALE_DATE",
]


def is_disclaimer_row(cells: list[str]) -> bool:
    blob = ",".join(cells)
    return (
        "For sales prior to the Final Roll" in blob
        or "Building Class Category is based on Building Class at Time of Sale" in blob
        or "Note: Condominium and cooperative sales" in blob
    )


def is_std_header(cells: list[str]) -> bool:
    return len(cells) > 1 and cells[0].strip() == "BOROUGH" and cells[1].strip() == "NEIGHBORHOOD"


def find_shifted_borough_col(cells: list[str]) -> int | None:
    if len(cells) < 3 or cells[1].strip() != "NEIGHBORHOOD":
        return None
    for i, v in enumerate(cells):
        if v.strip() == "BOROUGH":
            return i
    return None


def parse_borough_cell(s: str) -> str | None:
    """Accept 1–5 or Excel-style 1.0, 2.0, … in borough column."""
    t = str(s).strip()
    if not t:
        return None
    try:
        n = int(float(t.replace(",", "")))
        if 1 <= n <= 5:
            return str(n)
    except ValueError:
        pass
    return None


rows_out: list[list[str]] = []
layout: str | int | None = None  # "std" or int borough column index

with open(INPUT, newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    for cells in reader:
        if is_disclaimer_row(cells):
            continue

        if is_std_header(cells):
            layout = "std"
            continue

        bor_col = find_shifted_borough_col(cells)
        if bor_col is not None:
            layout = bor_col
            continue

        if layout is None:
            continue

        # pad for safe indexing
        if len(cells) < 25:
            cells = cells + [""] * (25 - len(cells))

        if layout == "std":
            b = parse_borough_cell(cells[0])
            if b is None:
                continue
            rec = [b] + cells[1:21]
        else:
            bor_col = int(layout)
            if bor_col >= len(cells):
                continue
            b = parse_borough_cell(cells[bor_col])
            if b is None:
                continue
            # cols 1..20 = NEIGHBORHOOD .. SALE_DATE; col bor_col = BOROUGH
            rec = [b] + cells[1:21]

        rows_out.append(rec[:21])

df = pd.DataFrame(rows_out, columns=FINAL_COLS)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"Wrote {OUTPUT} rows={len(df)} cols={len(df.columns)}")
