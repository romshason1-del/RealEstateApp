#!/usr/bin/env python3
"""
Temporary Python script to import DVF (Demandes de Valeurs Foncières) data
into Supabase properties_france table.

Uses .env.local for NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.

Usage:
  pip install python-dotenv supabase
  python scripts/import_france_dvf.py [path-to-file]

Default file: src/app/ValeursFoncieres-2024.txt

DVF columns (pipe-delimited, 0-indexed):
  8: Date mutation (DD/MM/YYYY)
  9: Nature mutation (Vente only)
  10: Valeur fonciere (price)
  11: No voie (house number)
  15: Voie (street name)
  16: Code postal
  17: Commune
  24: 1er lot (apartment/lot number)
  36: Type local
  38: Surface reelle bati
  39: Nombre pieces principales
"""

import os
import re
import sys
from pathlib import Path

# Load .env.local
def load_env():
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent / ".env.local"
        load_dotenv(env_path)
    except ImportError:
        env_path = Path(__file__).resolve().parent.parent / ".env.local"
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ[k.strip()] = v.strip().strip('"').strip("'")


def parse_french_number(s: str) -> float:
    if not s or not s.strip():
        return float("nan")
    normalized = s.replace(" ", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return float("nan")


def parse_french_date(s: str) -> str | None:
    if not s or not s.strip():
        return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s.strip())
    if not m:
        return None
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"


def build_address(commune: str, voie: str, no_voie: str, code_postal: str) -> str:
    parts = []
    if no_voie and no_voie.strip():
        parts.append(no_voie.strip())
    if voie and voie.strip():
        parts.append(voie.strip())
    if code_postal and code_postal.strip() and commune and commune.strip():
        parts.append(f"{code_postal.strip()} {commune.strip()}")
    elif commune and commune.strip():
        parts.append(commune.strip())
    return ", ".join(parts).strip() or "unknown"


def process_line(cols: list[str]) -> dict | None:
    if len(cols) < 25:
        return None

    nature = (cols[9] or "").strip()
    if nature != "Vente":
        return None

    price = parse_french_number(cols[10] or "")
    if not (price and price > 0):
        return None

    commune = (cols[17] or "").strip()
    voie = (cols[15] or "").strip()
    no_voie = (cols[11] or "").strip()
    code_postal = (cols[16] or "").strip()
    lot_number = (cols[24] or "").strip() or ""

    if not commune and not voie:
        return None

    addr = build_address(commune, voie, no_voie, code_postal)
    if not addr or addr == "unknown":
        return None

    date_str = parse_french_date(cols[8] or "")
    price_fmt = f"{int(price):,}".replace(",", " ")
    sale_info = f"{price_fmt} € · {date_str}" if date_str else f"{price_fmt} €"

    surface_str = (cols[38] or "").strip()
    surface = parse_french_number(surface_str) if surface_str else float("nan")
    surface_reelle_bati = surface if (surface and surface > 0) else None

    type_local = (cols[36] or "").strip() or None

    nb_pieces_str = (cols[39] or "").strip()
    try:
        nb_pieces = int(nb_pieces_str) if nb_pieces_str else None
    except ValueError:
        nb_pieces = None
    if nb_pieces is not None and nb_pieces < 0:
        nb_pieces = None

    return {
        "address": addr,
        "lot_number": lot_number,
        "current_value": price,
        "last_sale_info": sale_info,
        "street_avg_price": None,
        "neighborhood_quality": None,
        "code_postal": code_postal or None,
        "commune": commune or None,
        "voie": voie or None,
        "type_local": type_local,
        "surface_reelle_bati": surface_reelle_bati,
        "date_mutation": date_str,
        "numero_voie": no_voie or None,
        "nombre_pieces_principales": nb_pieces,
    }


def main():
    load_env()

    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    if not url or not key:
        print("Error: Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env.local")
        sys.exit(1)

    try:
        from supabase import create_client
    except ImportError:
        print("Error: Install dependencies: pip install python-dotenv supabase")
        sys.exit(1)

    client = create_client(url, key)

    base = Path(__file__).resolve().parent.parent
    src_app = base / "src" / "app" / "ValeursFoncieres-2024.txt"
    root_file = base / "ValeursFoncieres-2024.txt"
    default_path = src_app if src_app.exists() else root_file

    file_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_path
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        print("Usage: python scripts/import_france_dvf.py [path-to-ValeursFoncieres-2024.txt]")
        sys.exit(1)

    print(f"Importing from {file_path}")
    print(f"File size: {file_path.stat().st_size / 1024 / 1024:.1f} MB\n")

    batch: dict[str, dict] = {}
    batch_size = 100
    total_processed = 0
    total_skipped = 0
    total_upserted = 0
    line_num = 0

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            line_num += 1
            cols = [c.strip() for c in line.split("|")]

            if line_num == 1:
                print(f"Columns: {len(cols)} | Header: {' | '.join(cols[:8])}...")
                continue

            row = process_line(cols)
            if not row:
                total_skipped += 1
                continue

            total_processed += 1
            composite_key = f"{row['address']}\0{row['lot_number']}"
            existing = batch.get(composite_key)
            if not existing:
                batch[composite_key] = row
            else:
                ex_date = existing.get("date_mutation") or ""
                if row.get("date_mutation") and (not ex_date or row["date_mutation"] > ex_date):
                    batch[composite_key] = row

            if len(batch) >= 50_000:
                rows = list(batch.values())
                for i in range(0, len(rows), batch_size):
                    chunk = rows[i : i + batch_size]
                    try:
                        client.table("properties_france").upsert(
                            chunk,
                            on_conflict="address,lot_number",
                        ).execute()
                        total_upserted += len(chunk)
                    except Exception as e:
                        print(f"Upsert error: {e}")
                        raise
                print(f"  Line {line_num} | Flushed {len(batch)} unique | Total: {total_upserted}")
                batch.clear()

    if batch:
        rows = list(batch.values())
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            try:
                client.table("properties_france").upsert(
                    chunk,
                    on_conflict="address,lot_number",
                ).execute()
                total_upserted += len(chunk)
            except Exception as e:
                print(f"Upsert error: {e}")
                raise
        print(f"  Final flush: {len(batch)} unique rows")

    print("\n=== Done ===")
    print(f"Processed: {total_processed} Vente rows | Skipped: {total_skipped} | Upserted: {total_upserted}")


if __name__ == "__main__":
    main()
