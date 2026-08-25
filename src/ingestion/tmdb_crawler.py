"""
TMDb Crawler Script.
Crawls movie & TV show data using 4-layer filtering and incremental deduplication.
Outputs raw CSV datasets to data/raw/.
"""

import os
import csv
import json
import time
import argparse
import pandas as pd
from typing import Dict, List, Set
from tmdb_client import get_tmdb

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

MOVIES_CSV = os.path.join(RAW_DATA_DIR, "movies_metadata.csv")

def load_existing_ids() -> Set[int]:
    """Preloads existing movie IDs to avoid duplicate crawling."""
    if not os.path.exists(MOVIES_CSV):
        return set()
    try:
        df = pd.read_csv(MOVIES_CSV, usecols=["id"])
        existing_ids = set(df["id"].dropna().astype(int).tolist())
        print(f"Preloaded {len(existing_ids)} existing items from dataset.")
        return existing_ids
    except Exception as e:
        print(f"Could not load existing IDs: {e}")
        return set()

def fetch_movie_details(movie_id: int) -> Dict:
    """Fetches full movie details including credits, keywords, and reviews."""
    return get_tmdb(f"/movie/{movie_id}", params={"append_to_response": "credits,keywords,reviews"})

def fetch_tv_details(tv_id: int) -> Dict:
    """Fetches full TV show details including credits, keywords, and reviews."""
    return get_tmdb(f"/tv/{tv_id}", params={"append_to_response": "credits,keywords,reviews"})

def crawl_tmdb_dataset(target_movie_count: int = 1000, start_year: int = 1970, end_year: int = 2026, max_runtime_min: float = 0, start_page: int = 1):
    start_timestamp = time.time()
    max_duration_sec = max_runtime_min * 60.0 if max_runtime_min > 0 else 0

    seen_ids: Set[int] = load_existing_ids()
    initial_count = len(seen_ids)

    print("Starting TMDb Data Crawler...", flush=True)
    print(f"Target New Items: {target_movie_count} | Years: {start_year} - {end_year} | Start Page: {start_page}", flush=True)
    if max_duration_sec > 0:
        print(f"Max Duration Limit: {max_runtime_min} minutes", flush=True)

    movies_list: List[Dict] = []
    credits_list: List[Dict] = []
    keywords_list: List[Dict] = []
    links_list: List[Dict] = []
    ratings_list: List[Dict] = []
    reviews_list: List[Dict] = []

    def is_time_exceeded() -> bool:
        if max_duration_sec > 0:
            elapsed = time.time() - start_timestamp
            if elapsed >= max_duration_sec:
                print(f"Time limit of {max_runtime_min} minutes reached. Stopping crawl...", flush=True)
                return True
        return False

    def process_movie_item(item: Dict, media_type: str = "movie"):
        if is_time_exceeded() or len(movies_list) >= target_movie_count:
            return

        movie_id = item.get("id")
        if not movie_id or movie_id in seen_ids:
            return

        # Check release year filter
        release_date = item.get("release_date") or item.get("first_air_date") or ""
        if release_date and len(release_date) >= 4:
            try:
                year = int(release_date[:4])
                if year < start_year or year > end_year:
                    return
            except ValueError:
                pass
        
        # Layer 3 Filter: Must have poster and overview > 20 chars
        overview = item.get("overview", "") or ""
        poster_path = item.get("poster_path")
        if not poster_path or len(overview) < 20:
            return

        # Fetch deep details
        if media_type == "movie":
            details = fetch_movie_details(movie_id)
            runtime = details.get("runtime") or 0
            vote_count = details.get("vote_count", 0)

            # Layer 2 Filter: Runtime >= 40m OR vote_count >= 500 for short films
            if runtime < 40 and vote_count < 500:
                return
        else:
            details = fetch_tv_details(tv_id=movie_id)
            episode_runtimes = details.get("episode_run_time") or [45]
            runtime = episode_runtimes[0] if episode_runtimes else 45
            vote_count = details.get("vote_count", 0)

        seen_ids.add(movie_id)

        # Movie Record
        genres = json.dumps(details.get("genres", []), ensure_ascii=False)
        production_companies = json.dumps(details.get("production_companies", []), ensure_ascii=False)
        production_countries = json.dumps(details.get("production_countries", []), ensure_ascii=False)
        spoken_languages = json.dumps(details.get("spoken_languages", []), ensure_ascii=False)

        movies_list.append({
            "id": str(movie_id),
            "title": details.get("title") or details.get("name") or "",
            "original_title": details.get("original_title") or details.get("original_name") or "",
            "overview": details.get("overview", ""),
            "genres": genres,
            "budget": str(details.get("budget", 0)),
            "revenue": str(details.get("revenue", 0)),
            "release_date": details.get("release_date") or details.get("first_air_date") or "",
            "runtime": str(runtime),
            "popularity": str(details.get("popularity", 0.0)),
            "vote_average": str(details.get("vote_average", 0.0)),
            "vote_count": str(vote_count),
            "poster_path": poster_path or "",
            "imdb_id": details.get("imdb_id") or f"tt{movie_id}",
            "original_language": details.get("original_language", "en"),
            "status": details.get("status", "Released"),
            "tagline": details.get("tagline", ""),
            "media_type": media_type,
            "production_companies": production_companies,
            "production_countries": production_countries,
            "spoken_languages": spoken_languages
        })

        # Credits (Cast & Crew)
        credits_obj = details.get("credits", {})
        cast = credits_obj.get("cast", [])[:10]
        crew = [c for c in credits_obj.get("crew", []) if c.get("job") == "Director"]
        
        credits_list.append({
            "id": str(movie_id),
            "cast": json.dumps(cast, ensure_ascii=False),
            "crew": json.dumps(crew, ensure_ascii=False)
        })

        # Keywords
        kw_obj = details.get("keywords", {})
        raw_kw = kw_obj.get("keywords") or kw_obj.get("results") or []
        keywords_list.append({
            "id": str(movie_id),
            "keywords": json.dumps(raw_kw, ensure_ascii=False)
        })

        # Links
        links_list.append({
            "movieId": str(movie_id),
            "imdbId": (details.get("imdb_id") or "").replace("tt", ""),
            "tmdbId": str(movie_id)
        })

        # Rating Seed
        ratings_list.append({
            "userId": "1",
            "movieId": str(movie_id),
            "rating": str(details.get("vote_average", 7.0)),
            "timestamp": "1600000000"
        })

        # Reviews for Sentiment Analysis
        reviews_obj = details.get("reviews", {})
        results = reviews_obj.get("results", [])
        for rev in results[:3]:
            content = rev.get("content", "").strip()
            if len(content) > 30:
                author_rating = (rev.get("author_details") or {}).get("rating") or 7.0
                sentiment = "positive" if author_rating >= 6.0 else "negative"
                reviews_list.append({
                    "review_id": rev.get("id", f"r_{movie_id}"),
                    "movieId": str(movie_id),
                    "movie_title": details.get("title") or details.get("name") or "",
                    "review_text": content.replace("\n", " ").replace("\r", ""),
                    "sentiment": sentiment,
                    "rating": str(author_rating)
                })

        if len(movies_list) % 25 == 0:
            elapsed_min = round((time.time() - start_timestamp) / 60.0, 2)
            print(f"Processed: {len(movies_list)}/{target_movie_count} items | Total in Database: {initial_count + len(movies_list)} | Elapsed: {elapsed_min} min...", flush=True)

    pages_to_crawl = max(1, target_movie_count // 15)

    # Stream 1: Popular Movies
    print("\n[Stream 1/5] Fetching Popular Movies...", flush=True)
    for page in range(start_page, start_page + pages_to_crawl):
        if len(movies_list) >= target_movie_count or is_time_exceeded():
            break
        res = get_tmdb("/discover/movie", params={
            "sort_by": "popularity.desc",
            "vote_count.gte": 50,
            "primary_release_date.gte": f"{start_year}-01-01",
            "primary_release_date.lte": f"{end_year}-12-31",
            "page": page
        })
        for item in res.get("results", []):
            process_movie_item(item, media_type="movie")

    # Stream 2: All-Time Classics
    if len(movies_list) < target_movie_count and not is_time_exceeded():
        print("[Stream 2/5] Fetching All-Time Classic Movies...", flush=True)
        for page in range(start_page, start_page + max(2, pages_to_crawl // 2)):
            if len(movies_list) >= target_movie_count or is_time_exceeded():
                break
            res = get_tmdb("/discover/movie", params={
                "sort_by": "vote_count.desc",
                "vote_count.gte": 100,
                "primary_release_date.gte": f"{start_year}-01-01",
                "primary_release_date.lte": f"{end_year}-12-31",
                "page": page
            })
            for item in res.get("results", []):
                process_movie_item(item, media_type="movie")

    # Stream 3: Weekly Trending
    if len(movies_list) < target_movie_count and not is_time_exceeded():
        print("[Stream 3/5] Fetching Weekly Trending Movies...", flush=True)
        for page in range(start_page, start_page + 3):
            if len(movies_list) >= target_movie_count or is_time_exceeded():
                break
            res = get_tmdb("/trending/movie/week", params={"page": page})
            for item in res.get("results", []):
                process_movie_item(item, media_type="movie")

    # Stream 4: Hidden Gems / Cult Classics
    if len(movies_list) < target_movie_count and not is_time_exceeded():
        print("[Stream 4/5] Fetching Cult Classics / Hidden Gems...", flush=True)
        for page in range(start_page, start_page + 3):
            if len(movies_list) >= target_movie_count or is_time_exceeded():
                break
            res = get_tmdb("/discover/movie", params={
                "vote_average.gte": 7.8,
                "vote_count.gte": 20,
                "primary_release_date.gte": f"{start_year}-01-01",
                "primary_release_date.lte": f"{end_year}-12-31",
                "page": page
            })
            for item in res.get("results", []):
                process_movie_item(item, media_type="movie")

    # Stream 5: Popular TV Series
    if len(movies_list) < target_movie_count and not is_time_exceeded():
        print("[Stream 5/5] Fetching Popular TV Series...", flush=True)
        for page in range(start_page, start_page + max(2, pages_to_crawl // 3)):
            if len(movies_list) >= target_movie_count or is_time_exceeded():
                break
            res = get_tmdb("/discover/tv", params={
                "sort_by": "popularity.desc",
                "vote_count.gte": 50,
                "first_air_date.gte": f"{start_year}-01-01",
                "first_air_date.lte": f"{end_year}-12-31",
                "page": page
            })
            for item in res.get("results", []):
                process_movie_item(item, media_type="tv")

    total_time_min = round((time.time() - start_timestamp) / 60.0, 2)
    print(f"\nCrawl finished in {total_time_min} minutes!", flush=True)
    print(f"Added {len(movies_list)} NEW items! Total in Database: {initial_count + len(movies_list)}", flush=True)
    print(f"Total Reviews Collected: {len(reviews_list)}", flush=True)

    def append_save_csv(filename: str, rows: List[Dict]):
        if not rows:
            return
        path = os.path.join(RAW_DATA_DIR, filename)
        file_exists = os.path.exists(path)
        fieldnames = list(rows[0].keys())
        with open(path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)
        print(f"Appended to {filename} ({len(rows)} new records, Total size: {round(os.path.getsize(path)/1024, 2)} KB)", flush=True)

    append_save_csv("movies_metadata.csv", movies_list)
    append_save_csv("credits.csv", credits_list)
    append_save_csv("keywords.csv", keywords_list)
    append_save_csv("links.csv", links_list)
    append_save_csv("ratings.csv", ratings_list)
    append_save_csv("reviews.csv", reviews_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TMDb Data Crawler for Movie Lakehouse")
    parser.add_argument("--limit", type=int, default=1000, help="Target number of new items to crawl")
    parser.add_argument("--start-year", type=int, default=1970, help="Minimum release year (e.g. 1990)")
    parser.add_argument("--end-year", type=int, default=2026, help="Maximum release year (e.g. 2026)")
    parser.add_argument("--max-runtime-min", type=float, default=0, help="Max duration in minutes to stop crawling")
    parser.add_argument("--start-page", type=int, default=1, help="Starting page number for API pagination")
    
    args = parser.parse_args()
    
    crawl_tmdb_dataset(
        target_movie_count=args.limit,
        start_year=args.start_year,
        end_year=args.end_year,
        max_runtime_min=args.max_runtime_min,
        start_page=args.start_page
    )
