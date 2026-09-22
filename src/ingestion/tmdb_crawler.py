"""
TMDb Key-Driven Data Crawler Module (Multi-Task & Regionally Diverse).
Combines 4 Key Classification Groups and 6 Core Foundational Streams:

4 KEY CLASSIFICATION GROUPS:
1. 19 Movie Genre Keys & 16 TV Genre Keys (Action, Sci-Fi, Anime, Drama, etc.)
2. True TMDb Keyword IDs via /search/keyword & /discover/movie?with_keywords=
3. Region & Language Keys (Vietnamese, Korean, Japanese, Indian, French, Spanish, etc.)
4. Cinema Era & Decade Keys (1970-1989, 1990-2009, 2010-2026)

6 FOUNDATIONAL STREAMS:
1. Popular Movies Stream (popularity.desc)
2. All-Time Classics Stream (vote_count.desc)
3. Weekly Trending Movies Stream (/trending/movie/week)
4. Cult Classics & Hidden Gems Stream (vote_average >= 7.8)
5. Dedicated Anime Stream (Animation with_original_language=ja)
6. TV Series Stream (/discover/tv by TV genres & popularity)

Includes expanded Crew roles (Director, Writer, Screenplay, Producer),
schema header reconciliation for CSV appends, and high-throughput execution.
Outputs raw datasets to data/raw/*.csv.
"""

import os
import csv
import json
import time
import argparse
import pandas as pd
from typing import Dict, List, Set, Optional
try:
    from tmdb_client import get_tmdb
except ImportError:
    from src.ingestion.tmdb_client import get_tmdb

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

MOVIES_CSV = os.path.join(RAW_DATA_DIR, "movies_metadata.csv")

# 1. KEY GROUP 1: Movie Genre Keys (19 Official TMDb Genres)
MOVIE_GENRE_TAXONOMY: Dict[str, int] = {
    "action": 28,
    "adventure": 12,
    "animation": 16,
    "comedy": 35,
    "crime": 80,
    "documentary": 99,
    "drama": 18,
    "family": 10751,
    "fantasy": 14,
    "history": 36,
    "horror": 27,
    "music": 10402,
    "mystery": 9648,
    "romance": 10749,
    "science_fiction": 878,
    "tv_movie": 10770,
    "thriller": 53,
    "war": 10752,
    "western": 37
}

# TV Genre Keys (TMDb Specific TV Show Genres)
TV_GENRE_TAXONOMY: Dict[str, int] = {
    "action_adventure": 10759,
    "animation": 16,
    "comedy": 35,
    "crime": 80,
    "documentary": 99,
    "drama": 18,
    "family": 10751,
    "kids": 10762,
    "mystery": 9648,
    "news": 10763,
    "reality": 10764,
    "sci_fi_fantasy": 10765,
    "soap": 10766,
    "talk": 10767,
    "war_politics": 10768,
    "western": 37
}

# 2. KEY GROUP 2: Thematic Keyword Queries (Resolved to official TMDb Keyword IDs)
KEYWORD_TAXONOMY: List[str] = [
    "superhero",
    "cyberpunk",
    "artificial intelligence",
    "time travel",
    "anime",
    "post-apocalyptic",
    "zombie",
    "martial arts",
    "space travel",
    "dystopia",
    "cult film"
]

# 3. KEY GROUP 3: Region & Language Diversity (Serving Everyone without Hollywood bias)
REGION_TAXONOMY: List[Dict[str, any]] = [
    {"name": "Vietnamese Cinema", "language": "vi", "vote_min": 3},
    {"name": "Korean Cinema", "language": "ko", "vote_min": 15},
    {"name": "Japanese Cinema", "language": "ja", "vote_min": 15},
    {"name": "Bollywood & Indian Cinema", "language": "hi", "vote_min": 15},
    {"name": "French Cinema", "language": "fr", "vote_min": 20},
    {"name": "Spanish & Latin Cinema", "language": "es", "vote_min": 20},
    {"name": "Chinese & Hong Kong Cinema", "language": "zh", "vote_min": 15},
    {"name": "German Cinema", "language": "de", "vote_min": 20},
    {"name": "Italian Cinema", "language": "it", "vote_min": 20},
    {"name": "Thai Cinema", "language": "th", "vote_min": 10}
]

# 4. KEY GROUP 4: Cinema Era & Decade Keys
ERA_TAXONOMY: List[Dict[str, any]] = [
    {"name": "Classics (1970 - 1989)", "start": 1970, "end": 1989},
    {"name": "Modern Breakthrough (1990 - 2009)", "start": 1990, "end": 2009},
    {"name": "Contemporary Era (2010 - 2026)", "start": 2010, "end": 2026}
]

# Audited static overrides for maximum thematic coverage on TMDb
KEYWORD_ID_OVERRIDE: Dict[str, int] = {
    "superhero": 9715,               # superhero -> ~2,823 movies
    "cyberpunk": 12190,              # cyberpunk -> ~521 movies
    "artificial intelligence": 310,  # artificial intelligence (a.i.) -> ~662 movies
    "time travel": 4379,             # time travel -> ~1,300 movies
    "anime": 210024,                 # anime -> ~6,707 movies
    "post-apocalyptic": 4458,        # post-apocalyptic future -> ~1,144 movies
    "zombie": 12377,                 # zombie -> ~1,954 movies
    "martial arts": 779,             # martial arts -> ~2,931 movies
    "space travel": 3801,            # space travel -> ~225 movies
    "dystopia": 4565,                # dystopia -> ~1,800 movies
    "cult film": 6158                # cult -> ~631 movies
}

# Cache to avoid duplicate keyword ID lookups
KEYWORD_ID_CACHE: Dict[str, Optional[int]] = {}

def resolve_keyword_id(keyword_name: str) -> Optional[int]:
    """
    Resolves official TMDb keyword ID by thematic coverage (movie count) rather than naive string match.
    Uses audited static overrides first, then dynamically compares total_results for candidate tags.
    """
    clean_key = keyword_name.strip().lower()
    
    # 1. Check static override for audited tags
    if clean_key in KEYWORD_ID_OVERRIDE:
        return KEYWORD_ID_OVERRIDE[clean_key]

    if clean_key in KEYWORD_ID_CACHE:
        return KEYWORD_ID_CACHE[clean_key]
    
    res = get_tmdb("/search/keyword", params={"query": clean_key})
    results = res.get("results", [])
    if not results:
        KEYWORD_ID_CACHE[clean_key] = None
        return None
    
    # 2. Compare coverage (total_results) across top 5 candidate tags
    candidates = results[:5]
    best_id, best_count = None, -1
    for item in candidates:
        cand_id = item["id"]
        check = get_tmdb("/discover/movie", params={"with_keywords": cand_id, "page": 1})
        total = check.get("total_results", 0)
        if total > best_count:
            best_id, best_count = cand_id, total

    KEYWORD_ID_CACHE[clean_key] = best_id
    return best_id

def load_existing_ids() -> Set[int]:
    """Preloads existing movie IDs from CSV to guarantee incremental crawling."""
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

def crawl_key_driven_dataset(
    mode: str = "all",
    target_movie_count: int = 1000,
    selected_genres: Optional[List[str]] = None,
    selected_keywords: Optional[List[str]] = None,
    selected_languages: Optional[List[str]] = None,
    start_year: int = 1970,
    end_year: int = 2026,
    max_runtime_min: float = 0,
    start_page: int = 1
):
    """
    Executes unified crawling combining 4 Key Groups and 6 Foundational Streams.
    Supports continuous crawling with dynamic page depth and regional language diversity.
    """
    start_timestamp = time.time()
    max_duration_sec = max_runtime_min * 60.0 if max_runtime_min > 0 else 0

    seen_ids: Set[int] = load_existing_ids()
    initial_count = len(seen_ids)

    print("Starting Multi-Task and Regionally Diverse TMDb Crawler...", flush=True)
    print(f"Mode: {mode.upper()} | Target New Items: {target_movie_count} | Years: {start_year} - {end_year} | Start Page: {start_page}", flush=True)
    if max_duration_sec > 0:
        print(f"Max Duration Limit: {max_runtime_min} minutes (Continuous execution up to {max_runtime_min} min)", flush=True)

    movies_list: List[Dict] = []
    credits_list: List[Dict] = []
    keywords_list: List[Dict] = []
    links_list: List[Dict] = []
    ratings_list: List[Dict] = []
    reviews_list: List[Dict] = []

    _time_exceeded_logged = False
    def is_time_exceeded() -> bool:
        nonlocal _time_exceeded_logged
        if max_duration_sec > 0:
            elapsed = time.time() - start_timestamp
            if elapsed >= max_duration_sec:
                if not _time_exceeded_logged:
                    print(f"\nTime limit of {max_runtime_min} minutes reached. Finalizing datasets...", flush=True)
                    _time_exceeded_logged = True
                return True
        return False

    # Fair-share time budgeting across 5 stages when running mode 'all'
    stage_time_budget_sec = (max_duration_sec / 5.0) if (mode == "all" and max_duration_sec > 0) else max_duration_sec

    def is_stage_budget_exceeded(stage_start_time: float) -> bool:
        if is_time_exceeded():
            return True
        if stage_time_budget_sec > 0 and (time.time() - stage_start_time) >= stage_time_budget_sec:
            print(f"Stage time budget of {round(stage_time_budget_sec/60, 1)} min reached. Advancing to next stage...", flush=True)
            return True
        return False

    def process_movie_item(item: Dict, media_type: str = "movie"):
        if is_time_exceeded() or len(movies_list) >= target_movie_count:
            return

        movie_id = item.get("id")
        if not movie_id or movie_id in seen_ids:
            return

        # 1. Release date filter
        release_date = item.get("release_date") or item.get("first_air_date") or ""
        if release_date and len(release_date) >= 4:
            try:
                year = int(release_date[:4])
                if year < start_year or year > end_year:
                    return
            except ValueError:
                pass
        
        # 2. Quality filter: Must have poster and overview > 20 chars
        overview = item.get("overview", "") or ""
        poster_path = item.get("poster_path")
        if not poster_path or len(overview) < 20:
            return

        # 3. Deep detail fetch & Runtime filter (with regional language tolerance)
        orig_lang = item.get("original_language", "en")
        if media_type == "movie":
            details = fetch_movie_details(movie_id)
            runtime = details.get("runtime") or 0
            vote_count = details.get("vote_count", 0)

            # Standard Hollywood filter: runtime >= 40m or vote_count >= 300
            # Non-English regional cinema filter: runtime >= 30m or vote_count >= 5
            is_non_english = orig_lang != "en"
            if is_non_english:
                if runtime < 30 and vote_count < 5:
                    return
            else:
                if runtime < 40 and vote_count < 500:
                    return
        else:
            details = fetch_tv_details(tv_id=movie_id)
            episode_runtimes = details.get("episode_run_time") or [45]
            runtime = episode_runtimes[0] if episode_runtimes else 45
            vote_count = details.get("vote_count", 0)

        seen_ids.add(movie_id)

        # Build Movie Record
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
            "original_language": orig_lang,
            "status": details.get("status", "Released"),
            "tagline": details.get("tagline", ""),
            "media_type": media_type,
            "production_companies": production_companies,
            "production_countries": production_countries,
            "spoken_languages": spoken_languages
        })

        # Expanded Crew Roles (Director, Writer, Screenplay, Producer)
        credits_obj = details.get("credits", {})
        cast = credits_obj.get("cast", [])[:10]
        key_crew_jobs = {"Director", "Writer", "Screenplay", "Producer", "Executive Producer"}
        crew = [c for c in credits_obj.get("crew", []) if c.get("job") in key_crew_jobs][:12]
        
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
            print(f"Progress: {len(movies_list)}/{target_movie_count} items | Total in Database: {initial_count + len(movies_list)} | Elapsed: {elapsed_min} min...", flush=True)

    # Dynamic depth based on runtime limit and target item count
    pages_per_genre = max(5, min(40, target_movie_count // 30))
    pages_per_keyword = max(4, min(25, target_movie_count // 40))
    pages_per_region = max(4, min(25, target_movie_count // 40))
    pages_per_era = max(5, min(30, target_movie_count // 30))
    pages_per_stream = max(8, min(60, target_movie_count // 20))

    # -------------------------------------------------------------
    # PART 1: 4 KEY CLASSIFICATION GROUPS
    # -------------------------------------------------------------

    # Key Group 1: 19 Movie Genre Keys
    if mode in ["all", "genres"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        stage1_start = time.time()
        target_genres = selected_genres if selected_genres else list(MOVIE_GENRE_TAXONOMY.keys())
        print(f"\n[Key Group 1/4] Crawling Movie Genres ({len(target_genres)} keys)...", flush=True)
        
        for genre_name in target_genres:
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage1_start):
                break
            genre_id = MOVIE_GENRE_TAXONOMY.get(genre_name.lower().strip())
            if not genre_id:
                try:
                    genre_id = int(genre_name)
                except ValueError:
                    print(f"Unknown genre: {genre_name}")
                    continue

            print(f"-> Movie Genre Key: {genre_name} (ID: {genre_id})...", flush=True)
            for page in range(start_page, start_page + pages_per_genre):
                if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage1_start):
                    break
                res = get_tmdb("/discover/movie", params={
                    "with_genres": genre_id,
                    "sort_by": "popularity.desc",
                    "vote_count.gte": 25,
                    "primary_release_date.gte": f"{start_year}-01-01",
                    "primary_release_date.lte": f"{end_year}-12-31",
                    "page": page
                })
                for item in res.get("results", []):
                    process_movie_item(item, media_type="movie")

    # Key Group 2: Thematic Keywords via Official TMDb Keyword IDs
    if mode in ["all", "keywords"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        stage2_start = time.time()
        target_keywords = selected_keywords if selected_keywords else KEYWORD_TAXONOMY
        print(f"\n[Key Group 2/4] Crawling by Official TMDb Keyword IDs ({len(target_keywords)} keys)...", flush=True)
        
        for kw in target_keywords:
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage2_start):
                break
            kw_id = resolve_keyword_id(kw)
            if not kw_id:
                print(f"-> Keyword '{kw}' could not be resolved to TMDb ID, skipping...")
                continue
            
            print(f"-> Thematic Keyword Key: '{kw}' (Resolved ID: {kw_id})...", flush=True)
            for page in range(start_page, start_page + pages_per_keyword):
                if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage2_start):
                    break
                res = get_tmdb("/discover/movie", params={
                    "with_keywords": kw_id,
                    "sort_by": "vote_count.desc",
                    "vote_count.gte": 20,
                    "page": page
                })
                for item in res.get("results", []):
                    process_movie_item(item, media_type="movie")

    # Key Group 3: Region & Language Diversity (Serving Everyone without Hollywood bias)
    if mode in ["all", "regions"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        stage3_start = time.time()
        print(f"\n[Key Group 3/4] Crawling Regional & World Cinema (Language Diversity)...", flush=True)
        active_regions = [r for r in REGION_TAXONOMY if not selected_languages or r["language"] in selected_languages]
        quota_per_region = max(5, target_movie_count // len(active_regions)) if active_regions else target_movie_count

        for reg in active_regions:
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage3_start):
                break
            
            region_collected = 0
            print(f"-> Regional Key: {reg['name']} (Lang: {reg['language']}, Min Votes: {reg['vote_min']})...", flush=True)
            for page in range(start_page, start_page + pages_per_region):
                if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage3_start) or region_collected >= quota_per_region:
                    break
                res = get_tmdb("/discover/movie", params={
                    "with_original_language": reg["language"],
                    "sort_by": "vote_count.desc",
                    "vote_count.gte": reg["vote_min"],
                    "page": page
                })
                for item in res.get("results", []):
                    before_len = len(movies_list)
                    process_movie_item(item, media_type="movie")
                    if len(movies_list) > before_len:
                        region_collected += 1
                        if region_collected >= quota_per_region:
                            break

    # Key Group 4: Cinema Era & Decade Keys
    if mode in ["all", "eras"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        stage4_start = time.time()
        print(f"\n[Key Group 4/4] Crawling by Cinema Era Keys...", flush=True)
        for era in ERA_TAXONOMY:
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage4_start):
                break
            print(f"-> Era Key: {era['name']} ({era['start']}-{era['end']})...", flush=True)
            for page in range(start_page, start_page + pages_per_era):
                if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage4_start):
                    break
                res = get_tmdb("/discover/movie", params={
                    "primary_release_date.gte": f"{era['start']}-01-01",
                    "primary_release_date.lte": f"{era['end']}-12-31",
                    "sort_by": "vote_count.desc",
                    "vote_count.gte": 30,
                    "page": page
                })
                for item in res.get("results", []):
                    process_movie_item(item, media_type="movie")

    # -------------------------------------------------------------
    # PART 2: 6 FOUNDATIONAL STREAMS
    # -------------------------------------------------------------
    stage5_start = time.time()

    # Stream 1: Popular Movies
    if mode in ["all", "streams"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        print(f"\n[Foundation Stream 1/6] Crawling Popular Movies...", flush=True)
        for page in range(start_page, start_page + pages_per_stream):
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage5_start):
                break
            res = get_tmdb("/discover/movie", params={
                "sort_by": "popularity.desc",
                "vote_count.gte": 30,
                "primary_release_date.gte": f"{start_year}-01-01",
                "primary_release_date.lte": f"{end_year}-12-31",
                "page": page
            })
            for item in res.get("results", []):
                process_movie_item(item, media_type="movie")

    # Stream 2: All-Time Classics
    if mode in ["all", "streams"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        print(f"\n[Foundation Stream 2/6] Crawling All-Time Classics...", flush=True)
        for page in range(start_page, start_page + (pages_per_stream // 2)):
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage5_start):
                break
            res = get_tmdb("/discover/movie", params={
                "sort_by": "vote_count.desc",
                "vote_count.gte": 60,
                "primary_release_date.gte": f"{start_year}-01-01",
                "primary_release_date.lte": f"{end_year}-12-31",
                "page": page
            })
            for item in res.get("results", []):
                process_movie_item(item, media_type="movie")

    # Stream 3: Weekly Trending
    if mode in ["all", "streams"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        print(f"\n[Foundation Stream 3/6] Crawling Weekly Trending Movies...", flush=True)
        for page in range(start_page, start_page + 10):
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage5_start):
                break
            res = get_tmdb("/trending/movie/week", params={"page": page})
            for item in res.get("results", []):
                process_movie_item(item, media_type="movie")

    # Stream 4: Cult Classics / Hidden Gems
    if mode in ["all", "streams"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        print(f"\n[Foundation Stream 4/6] Crawling Cult Classics & Hidden Gems...", flush=True)
        for page in range(start_page, start_page + (pages_per_stream // 2)):
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage5_start):
                break
            res = get_tmdb("/discover/movie", params={
                "vote_average.gte": 7.8,
                "vote_count.gte": 15,
                "primary_release_date.gte": f"{start_year}-01-01",
                "primary_release_date.lte": f"{end_year}-12-31",
                "page": page
            })
            for item in res.get("results", []):
                process_movie_item(item, media_type="movie")

    # Stream 5: Dedicated Japanese Anime Stream
    if mode in ["all", "streams"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        print(f"\n[Foundation Stream 5/6] Crawling Dedicated Japanese Anime...", flush=True)
        for page in range(start_page, start_page + 10):
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage5_start):
                break
            res = get_tmdb("/discover/movie", params={
                "with_genres": 16,
                "with_original_language": "ja",
                "sort_by": "popularity.desc",
                "vote_count.gte": 15,
                "page": page
            })
            for item in res.get("results", []):
                process_movie_item(item, media_type="movie")

    # Stream 6: TV Series Stream (Crawling TV Genres)
    if mode in ["all", "streams"] and len(movies_list) < target_movie_count and not is_time_exceeded():
        print(f"\n[Foundation Stream 6/6] Crawling TV Series across TV Genres...", flush=True)
        tv_genres_subset = ["action_adventure", "sci_fi_fantasy", "drama", "animation", "mystery"]
        for g_name in tv_genres_subset:
            if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage5_start):
                break
            g_id = TV_GENRE_TAXONOMY.get(g_name)
            for page in range(start_page, start_page + 5):
                if len(movies_list) >= target_movie_count or is_stage_budget_exceeded(stage5_start):
                    break
                res = get_tmdb("/discover/tv", params={
                    "with_genres": g_id,
                    "sort_by": "popularity.desc",
                    "vote_count.gte": 25,
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

        if file_exists:
            with open(path, "r", encoding="utf-8") as f:
                existing_header = [h.strip() for h in f.readline().strip().split(",") if h.strip()]
            if existing_header != fieldnames:
                raise ValueError(
                    f"Schema mismatch in {filename}: "
                    f"existing={existing_header} vs new={fieldnames}. "
                    f"Xóa hoặc backup file cũ trước khi đổi schema."
                )

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
    parser = argparse.ArgumentParser(description="Combined Key & Multi-Stream TMDb Crawler for Movie Lakehouse")
    parser.add_argument("--mode", type=str, default="all", choices=["all", "genres", "keywords", "regions", "eras", "streams"], help="Crawl mode")
    parser.add_argument("--limit", type=int, default=10000, help="Target number of new items to crawl")
    parser.add_argument("--genres", type=str, default=None, help="Comma-separated list of genre names or IDs")
    parser.add_argument("--keywords", type=str, default=None, help="Comma-separated list of topic keywords")
    parser.add_argument("--languages", type=str, default=None, help="Comma-separated list of language codes (e.g. 'vi,ko,ja,hi')")
    parser.add_argument("--start-year", type=int, default=1970, help="Minimum release year (e.g. 1980)")
    parser.add_argument("--end-year", type=int, default=2026, help="Maximum release year (e.g. 2026)")
    parser.add_argument("--max-runtime-min", type=float, default=0, help="Max duration in minutes to stop crawling")
    parser.add_argument("--start-page", type=int, default=1, help="Starting page number for API pagination")
    
    args = parser.parse_args()
    
    selected_genres = [g.strip() for g in args.genres.split(",")] if args.genres else None
    selected_keywords = [k.strip() for k in args.keywords.split(",")] if args.keywords else None
    selected_languages = [l.strip() for l in args.languages.split(",")] if args.languages else None

    crawl_key_driven_dataset(
        mode=args.mode,
        target_movie_count=args.limit,
        selected_genres=selected_genres,
        selected_keywords=selected_keywords,
        selected_languages=selected_languages,
        start_year=args.start_year,
        end_year=args.end_year,
        max_runtime_min=args.max_runtime_min,
        start_page=args.start_page
    )
