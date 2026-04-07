#!/usr/bin/env python3
"""
Convert all .xls and .xlsx files under a source tree to UTF-8 CSV with the same base names.

- .xls: pandas with engine='calamine' (legacy BIFF).
- .xlsx: pandas with engine='openpyxl'.
- Writes CSV with encoding utf-8 (no BOM).
- Does not add an index column; first row is the header from the first sheet.

Usage:
  python convert_nyc_sales_xls_to_csv.py <source_dir> <output_dir>

Example:
  python convert_nyc_sales_xls_to_csv.py ./extracted_folder ./nyc_sales_csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _read_excel(path: Path):
    import pandas as pd

    ext = path.suffix.lower()
    if ext == ".xls":
        return pd.read_excel(path, engine="calamine", dtype=None)
    if ext == ".xlsx":
        return pd.read_excel(path, engine="openpyxl", dtype=None)
    raise ValueError(f"Unsupported extension: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert .xls / .xlsx files to UTF-8 CSV.")
    parser.add_argument("source_dir", type=Path, help="Root folder to search for spreadsheets")
    parser.add_argument("output_dir", type=Path, help="Output folder (created if missing)")
    args = parser.parse_args()

    source = args.source_dir.resolve()
    out_root = args.output_dir.resolve()

    if not source.is_dir():
        print(f"Source is not a directory: {source}", file=sys.stderr)
        return 1

    try:
        import pandas  # noqa: F401 — dependency check before batch read
    except ImportError:
        print("pandas is required: pip install pandas python-calamine openpyxl", file=sys.stderr)
        return 1

    xls_files = sorted(source.rglob("*.xls"))
    xlsx_files = sorted(source.rglob("*.xlsx"))
    all_files = sorted(xls_files + xlsx_files, key=lambda p: p.as_posix().lower())

    if not all_files:
        print(f"No .xls or .xlsx files under {source}", file=sys.stderr)
        return 1

    out_root.mkdir(parents=True, exist_ok=True)
    ok = 0
    errors: list[tuple[Path, str]] = []

    for path in all_files:
        stem = path.stem
        out_path = out_root / f"{stem}.csv"
        try:
            df = _read_excel(path)
            df.to_csv(
                out_path,
                index=False,
                encoding="utf-8",
                lineterminator="\n",
            )
            ok += 1
        except Exception as ex:  # noqa: BLE001 — report per-file and continue
            errors.append((path, str(ex)))

    print(f"Converted: {ok} file(s) -> {out_root}")
    if errors:
        print(f"Failed: {len(errors)}", file=sys.stderr)
        for p, msg in errors:
            print(f"  {p}: {msg}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
