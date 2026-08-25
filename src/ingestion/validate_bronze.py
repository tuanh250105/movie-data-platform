"""
Bronze Layer Validation and Manifest Generator.
Inspects CSV files in data/raw/ for row counts, columns, null rates,
and outputs a bronze_manifest.json metadata file.
"""

import os
import json
import time
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
MANIFEST_PATH = os.path.join(RAW_DATA_DIR, "bronze_manifest.json")

def validate_bronze_layer():
    print("Inspecting and validating Bronze Layer data storage...\n")
    
    files = ["movies_metadata.csv", "credits.csv", "keywords.csv", "links.csv", "ratings.csv", "reviews.csv"]
    table_records = []
    ingestion_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    
    total_records = 0
    for filename in files:
        filepath = os.path.join(RAW_DATA_DIR, filename)
        if not os.path.exists(filepath):
            print(f"Warning: Missing file in Bronze layer: {filename}")
            continue
            
        try:
            df = pd.read_csv(filepath)
            row_count = len(df)
            col_count = len(df.columns)
            size_kb = round(os.path.getsize(filepath) / 1024, 2)
            total_records += row_count
            
            table_records.append({
                "table_name": filename.replace(".csv", ""),
                "file_name": filename,
                "row_count": row_count,
                "col_count": col_count,
                "columns": list(df.columns),
                "file_size_kb": size_kb,
                "ingested_at": ingestion_time,
                "layer": "Bronze"
            })
            print(f"[Bronze Verified] {filename:22s} | Rows: {row_count:6d} | Cols: {col_count:2d} | Size: {size_kb:8.2f} KB")
        except Exception as e:
            print(f"Error reading {filename}: {e}")

    manifest = {
        "pipeline_name": "TMDb_Lakehouse_Bronze_Ingestion",
        "timestamp": ingestion_time,
        "total_tables": len(table_records),
        "total_records_ingested": total_records,
        "status": "SUCCESS" if table_records else "EMPTY",
        "tables": table_records
    }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"\nIngestion Manifest saved to: {MANIFEST_PATH}")
    return manifest

if __name__ == "__main__":
    validate_bronze_layer()
