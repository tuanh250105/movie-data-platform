"""
Gold Layer Feature Store Engineering Script.
Merges cleaned Silver tables (silver_movies, silver_credits, silver_keywords)
and constructs the multi-attribute Metadata Soup for Content-Based Recommendation.
Outputs: data/gold/gold_movies_features.parquet & .csv
"""

import os
import re
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")

os.makedirs(GOLD_DIR, exist_ok=True)

def clean_text_field(val: str) -> str:
    """Cleans text by stripping whitespace, removing excess spaces, and lowercasing."""
    if not val or pd.isna(val):
        return ""
    text = str(val).lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text

def build_gold_features():
    print("Starting Gold Layer Feature Store Generation...\n", flush=True)

    movies_path = os.path.join(SILVER_DIR, "silver_movies.parquet")
    credits_path = os.path.join(SILVER_DIR, "silver_credits.parquet")
    keywords_path = os.path.join(SILVER_DIR, "silver_keywords.parquet")

    for path in [movies_path, credits_path, keywords_path]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing required Silver table: {path}")

    print("[Gold 1/3] Loading Silver datasets...", flush=True)
    df_movies = pd.read_parquet(movies_path)
    df_credits = pd.read_parquet(credits_path)
    df_keywords = pd.read_parquet(keywords_path)

    print(f"Loaded movies: {len(df_movies)} | credits: {len(df_credits)} | keywords: {len(df_keywords)}", flush=True)

    # 1. Select and merge relevant columns
    print("[Gold 2/3] Merging multi-task attributes...", flush=True)
    df_gold = df_movies[[
        "id", "title", "original_title", "overview", "release_date",
        "runtime", "popularity", "vote_average", "vote_count",
        "original_language", "poster_path", "genres_clean",
        "production_companies_clean", "production_countries_clean"
    ]].copy()

    df_gold = df_gold.merge(
        df_credits[["id", "top_cast_clean", "director_clean", "writers_producers_clean", "all_crew_clean"]],
        on="id",
        how="left"
    )

    df_gold = df_gold.merge(
        df_keywords[["id", "keywords_clean"]],
        on="id",
        how="left"
    )

    # Fill NA strings
    text_cols = [
        "overview", "genres_clean", "keywords_clean",
        "director_clean", "top_cast_clean", "writers_producers_clean"
    ]
    for col in text_cols:
        df_gold[col] = df_gold[col].fillna("").apply(clean_text_field)

    # Extract release year
    df_gold["release_year"] = pd.to_datetime(df_gold["release_date"], errors="coerce").dt.year.fillna(0).astype(int)

    # 2. Construct Weighted Metadata Soup
    # Overview (x1) + Genres (x2) + Keywords (x2) + Director (x3) + Cast (x1) + Writers/Producers (x1)
    print("[Gold 3/3] Constructing Weighted Metadata Soup...", flush=True)
    def construct_soup(row) -> str:
        parts = []
        if row["overview"]:
            parts.append(row["overview"])
        if row["genres_clean"]:
            parts.append((row["genres_clean"] + " ") * 2)
        if row["keywords_clean"]:
            parts.append((row["keywords_clean"] + " ") * 2)
        if row["director_clean"]:
            parts.append((row["director_clean"] + " ") * 3)
        if row["top_cast_clean"]:
            parts.append(row["top_cast_clean"])
        if row["writers_producers_clean"]:
            parts.append(row["writers_producers_clean"])
        
        soup = " ".join(parts)
        return re.sub(r"\s+", " ", soup).strip()

    df_gold["metadata_soup"] = df_gold.apply(construct_soup, axis=1)

    # Save to Parquet and CSV
    out_parquet = os.path.join(GOLD_DIR, "gold_movies_features.parquet")
    out_csv = os.path.join(GOLD_DIR, "gold_movies_features.csv")

    df_gold.to_parquet(out_parquet, index=False, compression="snappy")
    df_gold.to_csv(out_csv, index=False)

    size_parquet_kb = round(os.path.getsize(out_parquet) / 1024, 2)
    size_csv_kb = round(os.path.getsize(out_csv) / 1024, 2)

    print(f"\nGold Feature Store Generation Completed.")
    print(f"Total Movies: {len(df_gold)} | Total Columns: {len(df_gold.columns)}")
    print(f"Parquet File: {out_parquet} ({size_parquet_kb} KB)")
    print(f"CSV File:     {out_csv} ({size_csv_kb} KB)")

    # Print sample
    sample = df_gold.iloc[0]
    print(f"\nSample Movie: {sample['title']} ({sample['release_year']})")
    print(f"Metadata Soup snippet: {sample['metadata_soup'][:150]}...")
    return df_gold

if __name__ == "__main__":
    build_gold_features()
