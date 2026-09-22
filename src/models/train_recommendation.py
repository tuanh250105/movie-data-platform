"""
Content-Based Movie Recommendation Engine Training Script.
Trains TF-IDF Vectorizer on Gold Layer Metadata Soup and implements
real-time sparse cosine similarity retrieval with quality re-ranking.
Outputs: models/recommender_tfidf.joblib & models/recommender_index.joblib
"""

import os
import time
import joblib
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")

os.makedirs(MODELS_DIR, exist_ok=True)

class MovieRecommender:
    def __init__(self, vectorizer=None, tfidf_matrix=None, movies_df=None):
        self.vectorizer = vectorizer
        self.tfidf_matrix = tfidf_matrix
        self.movies_df = movies_df
        if movies_df is not None:
            self._build_indices()

    def _build_indices(self):
        self.id_to_idx = {int(row["id"]): idx for idx, row in self.movies_df.iterrows()}
        self.title_to_idx = {str(row["title"]).lower().strip(): idx for idx, row in self.movies_df.iterrows()}

    def find_movie_index(self, query):
        """Finds row index by movieId or title (exact or partial)."""
        if isinstance(query, (int, np.integer)) or (isinstance(query, str) and query.isdigit()):
            return self.id_to_idx.get(int(query))
        
        q_clean = str(query).lower().strip()
        if q_clean in self.title_to_idx:
            return self.title_to_idx[q_clean]
        
        # Substring search fallback
        for title, idx in self.title_to_idx.items():
            if q_clean in title:
                return idx
        return None

    def recommend(self, query, top_k: int = 10, quality_boost: bool = True):
        """
        Computes cosine similarity on-the-fly using sparse dot product.
        Latency: < 20ms for 14,000+ items.
        """
        target_idx = self.find_movie_index(query)
        if target_idx is None:
            return []

        start_t = time.time()
        # Sparse matrix dot product: (1 x N) . (N x M) = (1 x M)
        sim_vector = self.tfidf_matrix[target_idx].dot(self.tfidf_matrix.T).toarray().ravel()

        # Zero out the query movie itself
        sim_vector[target_idx] = -1.0

        if quality_boost:
            # Re-ranking: 80% content similarity + 15% normalized rating + 5% popularity
            vote_avg = self.movies_df["vote_average"].values
            norm_rating = np.clip(vote_avg / 10.0, 0.0, 1.0)
            pop = self.movies_df["popularity"].values
            norm_pop = np.clip(pop / 100.0, 0.0, 1.0)
            final_scores = sim_vector * 0.80 + norm_rating * 0.15 + norm_pop * 0.05
        else:
            final_scores = sim_vector

        top_indices = np.argsort(final_scores)[::-1][:top_k]
        elapsed_ms = round((time.time() - start_t) * 1000, 2)

        results = []
        for rank, idx in enumerate(top_indices, 1):
            row = self.movies_df.iloc[idx]
            results.append({
                "rank": rank,
                "movieId": int(row["id"]),
                "title": row["title"],
                "release_year": int(row.get("release_year", 0)),
                "genres": row.get("genres_clean", ""),
                "director": row.get("director_clean", ""),
                "language": row.get("original_language", ""),
                "vote_average": float(row.get("vote_average", 0.0)),
                "similarity_score": round(float(sim_vector[idx]), 4),
                "final_score": round(float(final_scores[idx]), 4),
                "query_time_ms": elapsed_ms
            })

        return results

def train_recommender():
    print("Training Content-Based Recommendation Model...\n", flush=True)

    gold_path = os.path.join(GOLD_DIR, "gold_movies_features.parquet")
    if not os.path.exists(gold_path):
        raise FileNotFoundError(f"Missing Gold features table: {gold_path}. Run build_gold.py first.")

    print(f"[Recommender 1/3] Loading Gold Feature Store: {gold_path}...", flush=True)
    df_gold = pd.read_parquet(gold_path)
    total_movies = len(df_gold)
    print(f"Total Movies in Catalog: {total_movies}", flush=True)

    print("[Recommender 2/3] Fitting TF-IDF Vectorizer on Metadata Soup...", flush=True)
    vectorizer = TfidfVectorizer(
        max_features=20000,
        ngram_range=(1, 2),
        min_df=2,
        sublinear_tf=True,
        stop_words="english"
    )

    t0 = time.time()
    tfidf_matrix = vectorizer.fit_transform(df_gold["metadata_soup"].fillna(""))
    fit_time = round(time.time() - t0, 2)
    print(f"TF-IDF Matrix Shape: {tfidf_matrix.shape} (Vocabulary: {len(vectorizer.vocabulary_)} terms)")
    print(f"Matrix Memory: {round(tfidf_matrix.data.nbytes / (1024 * 1024), 2)} MB (Sparse CSR) | Fit time: {fit_time}s")

    print("[Recommender 3/3] Saving Model Artifacts to models/...", flush=True)
    # Save model artifacts
    tfidf_artifact_path = os.path.join(MODELS_DIR, "recommender_tfidf.joblib")
    index_artifact_path = os.path.join(MODELS_DIR, "recommender_index.joblib")

    joblib.dump({"vectorizer": vectorizer, "tfidf_matrix": tfidf_matrix}, tfidf_artifact_path, compress=3)
    
    # Store minimal metadata for inference to save disk & RAM
    inference_cols = [
        "id", "title", "release_year", "genres_clean",
        "director_clean", "top_cast_clean", "original_language",
        "vote_average", "vote_count", "popularity", "poster_path"
    ]
    df_inference = df_gold[inference_cols].copy()
    joblib.dump(df_inference, index_artifact_path, compress=3)

    size_tfidf_kb = round(os.path.getsize(tfidf_artifact_path) / 1024, 2)
    size_idx_kb = round(os.path.getsize(index_artifact_path) / 1024, 2)

    print(f"Saved: {tfidf_artifact_path} ({size_tfidf_kb} KB)")
    print(f"Saved: {index_artifact_path} ({size_idx_kb} KB)")

    # Run Demonstration Tests
    recommender = MovieRecommender(vectorizer, tfidf_matrix, df_inference)
    test_queries = ["The Dark Knight", "Princess Mononoke", "Parasite"]

    print("\n" + "="*70)
    print("DEMONSTRATION: REAL-TIME MOVIE RECOMMENDATIONS")
    print("="*70)

    for q in test_queries:
        recs = recommender.recommend(q, top_k=5)
        if recs:
            print(f"\nRecommendations for: '{q}' (Query Time: {recs[0]['query_time_ms']} ms):")
            for r in recs:
                print(f"  #{r['rank']} {r['title']} ({r['release_year']}) [{r['language'].upper()}] - Similarity: {r['similarity_score']} | Director: {r['director']} | Genres: {r['genres']}")
        else:
            print(f"\nMovie not found for query: '{q}'")

    print("\nRecommendation Model Training Completed.")
    return recommender

if __name__ == "__main__":
    train_recommender()
