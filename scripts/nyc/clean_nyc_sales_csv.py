#!/usr/bin/env python3
"""
Normalize NYC rolling/annual sales CSVs to a single schema for BigQuery load.

Reads raw exports (preamble + header + data), detects the BOROUGH header row,
maps columns to a fixed schema, drops junk/repeated headers, writes UTF-8 CSV.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


TARGET_COLS = [
    "borough",
    "neighborhood",
    "building_class_category",
    "tax_class_present",
    "block",
    "lot",
    "ease_ment",
    "building_class_present",
    "address",
    "apartment_number",
    "zip_code",
    "residential_units",
    "commercial_units",
    "total_units",
    "land_square_feet",
    "gross_square_feet",
    "year_built",
    "tax_class_sale",
    "building_class_sale",
    "sale_price",
    "sale_date",
    "source_file",
]


def normalize_header_cell(h: str) -> str:
    h = h.replace("\n", " ").replace("\r", " ")
    return re.sub(r"\s+", " ", h.strip()).upper()


def header_to_target(header: str) -> str | None:
    """Map one header cell to a target column name, or None if unknown."""
    u = normalize_header_cell(header)
    if u == "BOROUGH":
        return "borough"
    if u == "NEIGHBORHOOD":
        return "neighborhood"
    if u == "BUILDING CLASS CATEGORY":
        return "building_class_category"
    if u == "BLOCK":
        return "block"
    if u == "LOT":
        return "lot"
    if u in ("EASE-MENT", "EASEMENT"):
        return "ease_ment"
    if u == "ADDRESS":
        return "address"
    if u == "APARTMENT NUMBER":
        return "apartment_number"
    if u == "ZIP CODE":
        return "zip_code"
    if u == "RESIDENTIAL UNITS":
        return "residential_units"
    if u == "COMMERCIAL UNITS":
        return "commercial_units"
    if u == "TOTAL UNITS":
        return "total_units"
    if u == "LAND SQUARE FEET":
        return "land_square_feet"
    if u == "GROSS SQUARE FEET":
        return "gross_square_feet"
    if u == "YEAR BUILT":
        return "year_built"
    if u == "SALE PRICE":
        return "sale_price"
    if u == "SALE DATE":
        return "sale_date"
    if u == "TAX CLASS AT TIME OF SALE":
        return "tax_class_sale"
    if u == "BUILDING CLASS AT TIME OF SALE":
        return "building_class_sale"
    if u == "TAX CLASS AT PRESENT":
        return "tax_class_present"
    if u == "BUILDING CLASS AT PRESENT":
        return "building_class_present"
    if "TAX CLASS AS OF FINAL ROLL" in u:
        return "tax_class_present"
    if "BUILDING CLASS AS OF FINAL ROLL" in u:
        return "building_class_present"
    return None


def is_header_row(row: list[str]) -> bool:
    if not row:
        return False
    first = row[0].strip().upper()
    return first == "BOROUGH" or first.startswith("BOROUGH")


def is_probably_data_row(row: list[str]) -> bool:
    if not row or not row[0].strip():
        return False
    if is_header_row(row):
        return False
    return row[0].strip() in ("1", "2", "3", "4", "5")


def row_all_empty(row: list[str]) -> bool:
    return not row or all(not (c or "").strip() for c in row)


def process_file(
    path: Path,
) -> tuple[list[list[str]], int, int, list[str], bool, str | None, list[str]]:
    """
    Returns (output_rows_as_lists, rows_kept, rows_dropped, unknown_headers, unusual, note, header_cells_norm).
    output_rows exclude header; each row is values in TARGET_COLS order (strings).
    """
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        all_rows = list(reader)

    total_lines = len(all_rows)
    header_idx: int | None = None
    for i, row in enumerate(all_rows):
        if is_header_row(row):
            header_idx = i
            break

    unusual = False
    note: str | None = None
    if header_idx is None:
        unusual = True
        return [], 0, total_lines, [], True, "no BOROUGH header row found", []

    header_row = all_rows[header_idx]
    header_cells_norm = [normalize_header_cell(c) for c in header_row]
    col_map: dict[int, str] = {}
    unknown_headers: list[str] = []
    for j, cell in enumerate(header_row):
        t = header_to_target(cell)
        if t is None:
            un = normalize_header_cell(cell)
            if un:
                unknown_headers.append(un)
        else:
            col_map[j] = t

    if unknown_headers:
        unusual = True

    # Detect duplicate targets (should not happen)
    seen_t: dict[str, int] = {}
    for j, t in col_map.items():
        if t in seen_t:
            unusual = True
            note = f"duplicate target column {t} at indices {seen_t[t]} and {j}"
        seen_t[t] = j

    out_rows: list[list[str]] = []

    for row in all_rows[header_idx + 1 :]:
        if row_all_empty(row):
            continue
        if is_header_row(row):
            continue
        if not is_probably_data_row(row):
            continue

        # Pad row to max index
        max_idx = max(col_map.keys()) if col_map else -1
        extended = list(row)
        while len(extended) <= max_idx:
            extended.append("")

        rec = {t: "" for t in TARGET_COLS if t != "source_file"}
        for j, t in col_map.items():
            if j < len(extended):
                rec[t] = extended[j] if extended[j] is not None else ""
            else:
                rec[t] = ""

        ordered = [rec[c] for c in TARGET_COLS if c != "source_file"]
        ordered.append(path.name)
        out_rows.append(ordered)

    rows_kept = len(out_rows)
    # All lines not emitted as data rows (includes preamble, header, blanks, junk).
    rows_dropped = total_lines - rows_kept

    return out_rows, rows_kept, rows_dropped, unknown_headers, unusual, note, header_cells_norm


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("output_csv", type=Path)
    parser.add_argument("report_txt", type=Path)
    args = parser.parse_args()

    indir = args.input_dir.resolve()
    out_csv = args.output_csv.resolve()
    report_path = args.report_txt.resolve()

    if not indir.is_dir():
        print(f"Not a directory: {indir}", file=sys.stderr)
        return 1

    files = sorted(indir.glob("*.csv"))
    if not files:
        print(f"No CSV in {indir}", file=sys.stderr)
        return 1

    all_unknown: set[str] = set()
    all_header_cells: set[str] = set()
    unusual_files: list[str] = []
    notes: list[str] = []
    total_kept = 0
    total_dropped = 0
    processed = 0

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(
            fout,
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        writer.writerow(TARGET_COLS)

        for path in files:
            rows, kept, dropped, unknowns, unusual, note, header_norm = process_file(path)
            for h in header_norm:
                all_header_cells.add(h)
            for u in unknowns:
                all_unknown.add(u)
            if unusual:
                unusual_files.append(path.name)
                if note:
                    notes.append(f"{path.name}: {note}")
            for row in rows:
                writer.writerow(row)
            total_kept += kept
            total_dropped += dropped
            processed += 1

    report_lines = [
        "NYC sales CSV normalization report",
        "====================================",
        f"Source directory: {indir}",
        f"Output CSV: {out_csv}",
        "",
        f"Source files processed: {processed}",
        f"Total data rows kept (written): {total_kept}",
        f"Total rows dropped (preamble + junk + non-data): {total_dropped}",
        "",
        "Distinct unknown header tokens (should be empty if all columns mapped):",
    ]
    for u in sorted(all_unknown):
        report_lines.append(f"  - {u}")
    if not all_unknown:
        report_lines.append("  (none)")

    report_lines.extend(
        [
            "",
            "Detected column names (normalized) across all source files:",
        ]
    )
    for h in sorted(all_header_cells):
        report_lines.append(f"  - {h}")

    report_lines.extend(
        [
            "",
            "Files flagged as unusual structure:",
        ]
    )
    if unusual_files:
        for n in unusual_files:
            report_lines.append(f"  - {n}")
    else:
        report_lines.append("  (none)")

    if notes:
        report_lines.extend(["", "Notes:"])
        report_lines.extend(f"  {n}" for n in notes)

    report_lines.extend(
        [
            "",
            "Target schema column order:",
        ]
    )
    report_lines.extend(f"  {i+1}. {c}" for i, c in enumerate(TARGET_COLS))

    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(f"processed: {processed}")
    print(f"rows_kept: {total_kept}")
    print(f"rows_dropped: {total_dropped}")
    print(f"output: {out_csv}")
    print(f"report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
