#!/usr/bin/env python3
"""
Upload France DVF (Demandes de Valeurs Foncières) data to BigQuery.

Uses google-cloud-bigquery and google.auth.default() for authentication.
Handles pipe-delimited format and file encoding.

Usage:
  pip install google-cloud-bigquery
  python scripts/upload_france_to_bigquery.py [path-to-file]

Default file: src/app/ValeursFoncieres-2024.txt
"""

import sys
from pathlib import Path

from google.cloud import bigquery
from google.auth import default


# DVF columns (pipe-delimited, 43 columns from header)
DVF_SCHEMA = [
    bigquery.SchemaField("identifiant_document", "STRING"),
    bigquery.SchemaField("reference_document", "STRING"),
    bigquery.SchemaField("articles_cgi_1", "STRING"),
    bigquery.SchemaField("articles_cgi_2", "STRING"),
    bigquery.SchemaField("articles_cgi_3", "STRING"),
    bigquery.SchemaField("articles_cgi_4", "STRING"),
    bigquery.SchemaField("articles_cgi_5", "STRING"),
    bigquery.SchemaField("no_disposition", "STRING"),
    bigquery.SchemaField("date_mutation", "STRING"),
    bigquery.SchemaField("nature_mutation", "STRING"),
    bigquery.SchemaField("valeur_fonciere", "STRING"),
    bigquery.SchemaField("no_voie", "STRING"),
    bigquery.SchemaField("btq", "STRING"),
    bigquery.SchemaField("type_voie", "STRING"),
    bigquery.SchemaField("code_voie", "STRING"),
    bigquery.SchemaField("voie", "STRING"),
    bigquery.SchemaField("code_postal", "STRING"),
    bigquery.SchemaField("commune", "STRING"),
    bigquery.SchemaField("code_departement", "STRING"),
    bigquery.SchemaField("code_commune", "STRING"),
    bigquery.SchemaField("prefixe_section", "STRING"),
    bigquery.SchemaField("section", "STRING"),
    bigquery.SchemaField("no_plan", "STRING"),
    bigquery.SchemaField("no_volume", "STRING"),
    bigquery.SchemaField("lot_1er", "STRING"),
    bigquery.SchemaField("surface_carrez_1er", "STRING"),
    bigquery.SchemaField("lot_2eme", "STRING"),
    bigquery.SchemaField("surface_carrez_2eme", "STRING"),
    bigquery.SchemaField("lot_3eme", "STRING"),
    bigquery.SchemaField("surface_carrez_3eme", "STRING"),
    bigquery.SchemaField("lot_4eme", "STRING"),
    bigquery.SchemaField("surface_carrez_4eme", "STRING"),
    bigquery.SchemaField("lot_5eme", "STRING"),
    bigquery.SchemaField("surface_carrez_5eme", "STRING"),
    bigquery.SchemaField("nombre_lots", "STRING"),
    bigquery.SchemaField("code_type_local", "STRING"),
    bigquery.SchemaField("type_local", "STRING"),
    bigquery.SchemaField("identifiant_local", "STRING"),
    bigquery.SchemaField("surface_reelle_bati", "STRING"),
    bigquery.SchemaField("nombre_pieces_principales", "STRING"),
    bigquery.SchemaField("nature_culture", "STRING"),
    bigquery.SchemaField("nature_culture_speciale", "STRING"),
    bigquery.SchemaField("surface_terrain", "STRING"),
]

PROJECT_ID = "project-29fdf5d2-b1fb-4c43-b66"
DATASET_ID = "real_estate_data"
TABLE_ID = "france_2024"


def main():
    print("Starting upload...")

    base = Path(__file__).resolve().parent.parent
    src_app = base / "src" / "app" / "ValeursFoncieres-2024.txt"
    default_path = src_app if src_app.exists() else base / "ValeursFoncieres-2024.txt"

    file_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_path
    file_path = Path(file_path)

    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        print("Usage: python scripts/upload_france_to_bigquery.py [path-to-file]")
        sys.exit(1)

    print(f"Uploading: {file_path}")
    print(f"Size: {file_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}\n")

    print("Authenticating with Google Cloud...")
    credentials, project = default()
    client = bigquery.Client(credentials=credentials, project=project or PROJECT_ID)

    # Ensure dataset exists
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        print(f"Creating dataset {DATASET_ID}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # France data; use EU for GDPR
        client.create_dataset(dataset)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Try UTF-8 first; fall back to Latin-1 for French files with accented chars
    encoding = "UTF-8"
    try:
        with open(file_path, "r", encoding="utf-8", errors="strict") as f:
            f.read(1)
    except UnicodeDecodeError:
        encoding = "ISO-8859-1"
        print("Using Latin-1 encoding for French characters\n")

    job_config = bigquery.LoadJobConfig(
        schema=DVF_SCHEMA,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="|",
        skip_leading_rows=1,
        encoding=encoding,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        max_bad_records=1000,
    )

    print("Starting BigQuery load job...")
    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)

    print("Loading... (this may take several minutes for large files)")
    job.result()
    print("Load job completed.")

    table = client.get_table(table_ref)
    print(f"\nDone! Loaded {table.num_rows:,} rows into {table_ref}")
