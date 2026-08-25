"""
Silver Layer ETL Transformation Script.
Transforms raw Bronze datasets (data/raw/) into clean, standardized Silver Layer tables (data/silver/):
1. Deduplication on movie ID and review ID
2. Data Type Casting (Date, Double, Int, Float)
3. JSON Flattening (Extracting clean names from genres, cast, crew, keywords)
4. Text Normalization and NULL Handling
5. Saves clean tables in Parquet format.
"""

import os
import json
import pandas as pd
from typing import List, Dict

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BRONZE_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")

os.makedirs(SILVER_DIR, exist_ok=True)

def safe_parse_json_names(json_str: str) -> str:
    """Helper to extract clean comma-separated names from TMDb JSON strings."""
    if not json_str or pd.isna(json_str) or json_str == "[]":
        return ""
    try:
        data = json.loads(json_str)
        if isinstance(data, list):
            names = [item.get("name", "") for item in data if isinstance(item, dict) and item.get("name")]
            return ", ".join(names)
    except Exception:
        pass
    return ""

def transform_silver_layer():
    print("Starting Silver Layer ETL Transformation...\n", flush=True)

    # 1. Transform Movies Metadata (silver_movies)
    movies_path = os.path.join(BRONZE_DIR, "movies_metadata.csv")
    if os.path.exists(movies_path):
        print("[Silver 1/4] Processing Movies Metadata...", flush=True)
        df_movies = pd.read_csv(movies_path, dtype=str)
        initial_len = len(df_movies)

        # Deduplication
        df_movies = df_movies.drop_duplicates(subset=["id"]).copy()

        # Data Type Casting & Cleaning
        df_movies["id"] = pd.to_numeric(df_movies["id"], errors="coerce").astype("Int64")
        df_movies["budget"] = pd.to_numeric(df_movies["budget"], errors="coerce").fillna(0.0)
        df_movies["revenue"] = pd.to_numeric(df_movies["revenue"], errors="coerce").fillna(0.0)
        df_movies["runtime"] = pd.to_numeric(df_movies["runtime"], errors="coerce").fillna(0).astype(int)
        df_movies["popularity"] = pd.to_numeric(df_movies["popularity"], errors="coerce").fillna(0.0)
        df_movies["vote_average"] = pd.to_numeric(df_movies["vote_average"], errors="coerce").fillna(0.0)
        df_movies["vote_count"] = pd.to_numeric(df_movies["vote_count"], errors="coerce").fillna(0).astype(int)
        df_movies["release_date"] = pd.to_datetime(df_movies["release_date"], errors="coerce").dt.strftime("%Y-%m-%d")

        # JSON Flattening
        df_movies["genres_clean"] = df_movies["genres"].apply(safe_parse_json_names)
        df_movies["production_companies_clean"] = df_movies["production_companies"].apply(safe_parse_json_names)
        df_movies["production_countries_clean"] = df_movies["production_countries"].apply(safe_parse_json_names)

        # Fill text NA
        df_movies["overview"] = df_movies["overview"].fillna("").astype(str).str.strip()
        df_movies["title"] = df_movies["title"].fillna("Unknown Title").astype(str).str.strip()

        # Save to Silver Parquet & CSV
        out_parquet = os.path.join(SILVER_DIR, "silver_movies.parquet")
        out_csv = os.path.join(SILVER_DIR, "silver_movies.csv")
        df_movies.to_parquet(out_parquet, index=False)
        df_movies.to_csv(out_csv, index=False)
        print(f"Created silver_movies ({len(df_movies)} clean records, removed {initial_len - len(df_movies)} duplicates)")

    # 2. Transform Credits (silver_credits)
    credits_path = os.path.join(BRONZE_DIR, "credits.csv")
    if os.path.exists(credits_path):
        print("[Silver 2/4] Processing Credits (Cast & Crew)...", flush=True)
        df_credits = pd.read_csv(credits_path, dtype=str)
        df_credits = df_credits.drop_duplicates(subset=["id"]).copy()
        df_credits["id"] = pd.to_numeric(df_credits["id"], errors="coerce").astype("Int64")

        # Extract Cast Names & Director Names
        df_credits["top_cast_clean"] = df_credits["cast"].apply(safe_parse_json_names)
        df_credits["director_clean"] = df_credits["crew"].apply(safe_parse_json_names)

        out_parquet = os.path.join(SILVER_DIR, "silver_credits.parquet")
        out_csv = os.path.join(SILVER_DIR, "silver_credits.csv")
        df_credits.to_parquet(out_parquet, index=False)
        df_credits.to_csv(out_csv, index=False)
        print(f"Created silver_credits ({len(df_credits)} clean records)")

    # 3. Transform Keywords (silver_keywords)
    keywords_path = os.path.join(BRONZE_DIR, "keywords.csv")
    if os.path.exists(keywords_path):
        print("[Silver 3/4] Processing Keywords...", flush=True)
        df_kw = pd.read_csv(keywords_path, dtype=str)
        df_kw = df_kw.drop_duplicates(subset=["id"]).copy()
        df_kw["id"] = pd.to_numeric(df_kw["id"], errors="coerce").astype("Int64")

        df_kw["keywords_clean"] = df_kw["keywords"].apply(safe_parse_json_names)

        out_parquet = os.path.join(SILVER_DIR, "silver_keywords.parquet")
        out_csv = os.path.join(SILVER_DIR, "silver_keywords.csv")
        df_kw.to_parquet(out_parquet, index=False)
        df_kw.to_csv(out_csv, index=False)
        print(f"Created silver_keywords ({len(df_kw)} clean records)")

    # 4. Transform Reviews (silver_reviews)
    reviews_path = os.path.join(BRONZE_DIR, "reviews.csv")
    if os.path.exists(reviews_path):
        print("[Silver 4/4] Processing User Reviews (Sentiment Dataset)...", flush=True)
        df_rev = pd.read_csv(reviews_path, dtype=str)
        initial_rev_len = len(df_rev)

        df_rev = df_rev.drop_duplicates(subset=["review_id"]).copy()
        df_rev["movieId"] = pd.to_numeric(df_rev["movieId"], errors="coerce").astype("Int64")
        df_rev["rating"] = pd.to_numeric(df_rev["rating"], errors="coerce").fillna(7.0)
        
        # Clean Review Text
        df_rev["review_text"] = df_rev["review_text"].fillna("").astype(str).str.strip()
        df_rev = df_rev[df_rev["review_text"].str.len() > 20].copy()

        # Standardize Sentiment Label (positive / negative)
        df_rev["sentiment_label"] = df_rev["rating"].apply(lambda r: "positive" if float(r) >= 6.0 else "negative")

        out_parquet = os.path.join(SILVER_DIR, "silver_reviews.parquet")
        out_csv = os.path.join(SILVER_DIR, "silver_reviews.csv")
        df_rev.to_parquet(out_parquet, index=False)
        df_rev.to_csv(out_csv, index=False)
        print(f"Created silver_reviews ({len(df_rev)} clean records, removed {initial_rev_len - len(df_rev)} noisy records)")

    print("\nSilver Layer ETL Transformation Completed.")

if __name__ == "__main__":
    transform_silver_layer()
