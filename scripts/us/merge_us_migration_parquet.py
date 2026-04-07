#!/usr/bin/env python3
"""
Merge split BigQuery parquet exports into one file per NYC table.

Recursively finds *.parquet under --input-dir, groups by table name prefix,
concatenates with PyArrow (schema-preserving), writes to --output-dir.

Does not modify cell values — only row order from sorted source files.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Exact output filenames (without .parquet), in report order.
KNOWN_TABLES: tuple[str, ...] = (
    "us_nyc_acris_master_clean_v2",
    "us_nyc_acris_joined_clean_v2",
    "us_nyc_acris_property_full_v1",
    "us_nyc_acris_joined_full_v1",
    "us_nyc_latest_sale_by_bbl_v4",
)

# Longest prefix first for match_table (handles shared substrings safely).
MATCH_ORDER: tuple[str, ...] = tuple(sorted(KNOWN_TABLES, key=len, reverse=True))


def match_table(stem: str) -> str | None:
    """Return canonical table prefix for a parquet filename stem, or None."""
    for prefix in MATCH_ORDER:
        if stem == prefix:
            return prefix
        if stem.startswith(prefix + "-") or stem.startswith(prefix + "_"):
            return prefix
    return None


def resolve_table(path: Path, root: Path) -> str | None:
    """
    Map a parquet path to a known table:
    1) Filename stem (e.g. table-000000000000.parquet)
    2) Any ancestor folder name under root (e.g. .../us_nyc_acris_master_clean_v2/part-000.parquet)
    """
    t = match_table(path.stem)
    if t:
        return t
    cur: Path = path.parent
    while True:
        try:
            cur.relative_to(root)
        except ValueError:
            break
        if cur.name in KNOWN_TABLES:
            return cur.name
        m = match_table(cur.name)
        if m:
            return m
        if cur == root:
            break
        nxt = cur.parent
        if nxt == cur:
            break
        cur = nxt
    return None


def collect_parquet_parts(root: Path) -> tuple[dict[str, list[Path]], list[Path]]:
    """Map table prefix -> sorted list of part paths."""
    buckets: dict[str, list[Path]] = {p: [] for p in KNOWN_TABLES}
    unmatched: list[Path] = []

    for path in root.rglob("*.parquet"):
        t = resolve_table(path, root)
        if t is None:
            unmatched.append(path)
            continue
        buckets[t].append(path)

    for t in buckets:
        buckets[t].sort(key=lambda p: p.as_posix().lower())
    return buckets, unmatched


def merge_table(parts: list[Path], out_path: Path) -> tuple[int, int]:
    """
    Returns (num_parts, total_rows).
    Raises if schemas are incompatible.
    """
    if not parts:
        return 0, 0
    tables = [pq.read_table(p) for p in parts]
    merged = pa.concat_tables(tables)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(merged, out_path, compression="snappy")
    return len(parts), merged.num_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge split NYC parquet exports.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(r"C:\Users\ADMIN\RealEstateApp\us_migration"),
        help="Root folder containing BigQuery parquet exports (recursive)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(r"C:\Users\ADMIN\RealEstateApp\us_migration_merged"),
        help="Folder for merged parquet files + report",
    )
    args = parser.parse_args()

    root = args.input_dir.resolve()
    out_dir = args.output_dir.resolve()

    if not root.is_dir():
        print(f"Input directory does not exist: {root}", file=sys.stderr)
        print("Create it and place your .parquet exports inside, or pass --input-dir.", file=sys.stderr)
        return 1

    buckets, unmatched = collect_parquet_parts(root)
    out_dir.mkdir(parents=True, exist_ok=True)

    report_lines: list[str] = [
        "US NYC migration parquet merge report",
        "======================================",
        f"Input root: {root}",
        f"Output dir: {out_dir}",
        "",
    ]

    results: list[tuple[str, int, int, str | None]] = []

    for prefix in KNOWN_TABLES:
        parts = buckets[prefix]
        out_name = f"{prefix}.parquet"
        out_path = out_dir / out_name
        err: str | None = None
        n_parts = 0
        n_rows = 0
        try:
            if parts:
                n_parts, n_rows = merge_table(parts, out_path)
            else:
                err = "no matching source parquet files found; output file not written"
        except Exception as ex:  # noqa: BLE001
            err = f"merge failed: {ex}"
        results.append((out_name, n_parts, n_rows, err))

    report_lines.append("Per-table results (exact output filenames)")
    report_lines.append("-------------------------------------------")
    for out_name, n_parts, n_rows, err in results:
        report_lines.append(f"File: {out_name}")
        report_lines.append(f"  Source parts merged: {n_parts}")
        report_lines.append(f"  Total rows: {n_rows}")
        if err:
            report_lines.append(f"  Note: {err}")
        report_lines.append("")

    if unmatched:
        report_lines.append("Parquet files not matched to any known table (skipped):")
        for p in sorted(unmatched, key=lambda x: x.as_posix().lower()):
            report_lines.append(f"  - {p}")
        report_lines.append("")

    report_lines.append(
        "Matching: (1) filename stem equals table name, or starts with table name + '-' or '_'; "
        "(2) any parent folder under the input root is named exactly like a table "
        "(e.g. .../us_nyc_acris_master_clean_v2/part-00000.parquet)."
    )

    report_path = out_dir / "merge_report.txt"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print("\n".join(report_lines))
    print(f"\nReport written: {report_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
