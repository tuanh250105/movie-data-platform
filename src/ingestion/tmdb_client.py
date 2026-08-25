"""
TMDb API Client Module.
Manages TMDb REST API connections, authentication via .env,
rate-limiting, error retries, and pagination.
"""

import os
import time
import requests
from dotenv import load_dotenv

# Load environment variables from .env
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(ENV_PATH)

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BASE_URL = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")

if not TMDB_API_KEY:
    raise ValueError("TMDB_API_KEY is not set in .env file.")

session = requests.Session()

def get_tmdb(endpoint: str, params: dict = None, retries: int = 3, delay_sec: float = 0.25) -> dict:
    """
    Sends an authenticated GET request to TMDb API with rate limiting and retry logic.
    """
    if params is None:
        params = {}
    
    params["api_key"] = TMDB_API_KEY
    url = f"{TMDB_BASE_URL}{endpoint}"

    for attempt in range(1, retries + 1):
        try:
            time.sleep(delay_sec)
            response = session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 2))
                print(f"Rate limit hit. Waiting {retry_after}s...")
                time.sleep(retry_after)
            else:
                print(f"TMDb API HTTP {response.status_code} for {endpoint}: {response.text[:100]}")
                
        except requests.RequestException as e:
            print(f"Attempt {attempt}/{retries} failed for {endpoint}: {e}")
            time.sleep(1)
            
    return {}

if __name__ == "__main__":
    print("Testing TMDb Client connection...")
    res = get_tmdb("/configuration")
    if "images" in res:
        print("TMDb API Authentication Successful.")
        print("Base Image URL:", res["images"]["secure_base_url"])
    else:
        print("TMDb API Authentication Failed. Response:", res)
