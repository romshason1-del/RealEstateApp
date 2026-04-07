#!/usr/bin/env python3
"""
Merge all .csv files in a folder into one UTF-8 CSV with a single header row
and a source_file column (original filename).

Reads every cell as text (dtype=str) to avoid pandas retyping numbers/dates.

Usage:
  python merge_nyc_sales_csv.py <input_folder> <output_csv_path>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge NYC sales CSVs with source_file column.")
    parser.add_argument(
        "input_folder",
        type=Path,
        help="Folder containing .csv files",
    )
    parser.add_argument(
        "output_csv",
        type=Path,
        help="Path for merged output (UTF-8)",
    )
    args = parser.parse_args()

    folder = args.input_folder.resolve()
    out_path = args.output_csv.resolve()

    if not folder.is_dir():
        print(f"Not a directory: {folder}", file=sys.stderr)
        return 1

    try:
        import pandas as pd
    except ImportError:
        print("pandas is required: pip install pandas", file=sys.stderr)
        return 1

    files = sorted(folder.glob("*.csv"))
    if not files:
        print(f"No .csv files in {folder}", file=sys.stderr)
        return 1

    frames = []
    for path in files:
        df = pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)
        df["source_file"] = path.name
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True, sort=False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(
        out_path,
        index=False,
        encoding="utf-8",
        lineterminator="\n",
    )

    n_sources = len(files)
    n_data_rows = len(merged)
    print(f"source_files: {n_sources}")
    print(f"data_rows: {n_data_rows}")
    print(f"output: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
