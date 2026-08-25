"""
On-Demand Dynamic Data Ingestion Module.
Allows Flask Web UI to dynamically fetch and cache any movie
searched by the user on the fly (Fast-Path Serving).
"""

import os
import csv
import json
import pandas as pd
from typing import Dict, Optional
from tmdb_client import get_tmdb

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

MOVIES_CSV = os.path.join(RAW_DATA_DIR, "movies_metadata.csv")
REVIEWS_CSV = os.path.join(RAW_DATA_DIR, "reviews.csv")

def append_dict_to_csv(filepath: str, data_dict: Dict):
    """Appends a single dictionary record to a CSV file."""
    file_exists = os.path.exists(filepath)
    with open(filepath, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(data_dict.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(data_dict)

def fetch_and_cache_movie_on_demand(query: str) -> Optional[Dict]:
    """
    Dynamically searches TMDb for a query string, fetches its metadata and reviews,
    appends it to raw Bronze layer storage, and returns details for Fast-Path serving.
    """
    print(f"[On-Demand Ingestion] Searching TMDb for: '{query}'...")
    
    search_res = get_tmdb("/search/movie", params={"query": query, "page": 1})
    results = search_res.get("results", [])
    
    if not results:
        search_tv = get_tmdb("/search/tv", params={"query": query, "page": 1})
        results = search_tv.get("results", [])
        media_type = "tv"
    else:
        media_type = "movie"

    if not results:
        print(f"No results found on TMDb for: '{query}'")
        return None

    top_item = results[0]
    movie_id = top_item["id"]

    endpoint = f"/{media_type}/{movie_id}"
    details = get_tmdb(endpoint, params={"append_to_response": "credits,keywords,reviews"})

    poster_path = details.get("poster_path") or ""
    runtime = details.get("runtime") if media_type == "movie" else (details.get("episode_run_time") or [45])[0]

    movie_record = {
        "id": str(movie_id),
        "title": details.get("title") or details.get("name") or "",
        "original_title": details.get("original_title") or details.get("original_name") or "",
        "overview": details.get("overview", ""),
        "genres": json.dumps(details.get("genres", []), ensure_ascii=False),
        "budget": str(details.get("budget", 0)),
        "revenue": str(details.get("revenue", 0)),
        "release_date": details.get("release_date") or details.get("first_air_date") or "",
        "runtime": str(runtime or 0),
        "popularity": str(details.get("popularity", 0.0)),
        "vote_average": str(details.get("vote_average", 0.0)),
        "vote_count": str(details.get("vote_count", 0)),
        "poster_path": poster_path,
        "imdb_id": details.get("imdb_id") or f"tt{movie_id}",
        "original_language": details.get("original_language", "en"),
        "status": details.get("status", "Released"),
        "tagline": details.get("tagline", ""),
        "media_type": media_type,
        "production_companies": json.dumps(details.get("production_companies", []), ensure_ascii=False),
        "production_countries": json.dumps(details.get("production_countries", []), ensure_ascii=False),
        "spoken_languages": json.dumps(details.get("spoken_languages", []), ensure_ascii=False)
    }

    append_dict_to_csv(MOVIES_CSV, movie_record)
    print(f"[On-Demand] Ingested and cached '{movie_record['title']}' to Bronze Layer.")

    reviews_obj = details.get("reviews", {})
    for rev in reviews_obj.get("results", [])[:3]:
        content = rev.get("content", "").strip().replace("\n", " ").replace("\r", "")
        if len(content) > 20:
            rating = (rev.get("author_details") or {}).get("rating") or 7.0
            rev_record = {
                "review_id": rev.get("id", f"r_{movie_id}"),
                "movieId": str(movie_id),
                "movie_title": movie_record["title"],
                "review_text": content,
                "sentiment": "positive" if rating >= 6.0 else "negative",
                "rating": str(rating)
            }
            append_dict_to_csv(REVIEWS_CSV, rev_record)

    return movie_record

if __name__ == "__main__":
    import sys
    search_term = sys.argv[1] if len(sys.argv) > 1 else "Everything Everywhere All at Once"
    res = fetch_and_cache_movie_on_demand(search_term)
    if res:
        print("Result Title:", res["title"])
        print("Release Date:", res["release_date"])
        print("Vote Average:", res["vote_average"])
