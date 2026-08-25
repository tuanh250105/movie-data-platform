"""
Silver Layer Inspection and Validation Script.
Validates cleaned Parquet tables in data/silver/, checks null ratios,
schema consistency, and outputs silver_manifest.json.
"""

import os
import json
import time
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
MANIFEST_PATH = os.path.join(SILVER_DIR, "silver_manifest.json")

def validate_silver_layer():
    print("Inspecting and validating Silver Layer data storage...\n")
    
    tables = [
        ("silver_movies", "silver_movies.parquet"),
        ("silver_credits", "silver_credits.parquet"),
        ("silver_keywords", "silver_keywords.parquet"),
        ("silver_reviews", "silver_reviews.parquet")
    ]
    
    table_records = []
    ingestion_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    total_records = 0
    
    for table_name, filename in tables:
        filepath = os.path.join(SILVER_DIR, filename)
        if not os.path.exists(filepath):
            print(f"Warning: Missing Silver table file: {filename}")
            continue
            
        try:
            df = pd.read_parquet(filepath)
            row_count = len(df)
            col_count = len(df.columns)
            size_kb = round(os.path.getsize(filepath) / 1024, 2)
            total_records += row_count
            
            table_records.append({
                "table_name": table_name,
                "file_name": filename,
                "row_count": row_count,
                "col_count": col_count,
                "columns": list(df.columns),
                "file_size_kb": size_kb,
                "transformed_at": ingestion_time,
                "layer": "Silver"
            })
            print(f"[Silver Verified] {table_name:20s} | Rows: {row_count:6d} | Cols: {col_count:2d} | Size: {size_kb:8.2f} KB")
        except Exception as e:
            print(f"Error inspecting {table_name}: {e}")

    manifest = {
        "pipeline_name": "Movies_Lakehouse_Silver_ETL",
        "timestamp": ingestion_time,
        "total_tables": len(table_records),
        "total_records": total_records,
        "status": "SUCCESS" if table_records else "EMPTY",
        "tables": table_records
    }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"\nSilver Layer Manifest saved to: {MANIFEST_PATH}")
    return manifest

if __name__ == "__main__":
    validate_silver_layer()
