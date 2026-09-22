"""
Gold Layer and Machine Learning Models Validation Script.
Validates the Gold Feature Store, trained Recommender, and Sentiment models.
Performs end-to-end smoke testing and generates gold_manifest.json.
"""

import os
import json
import time
import joblib
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")
MANIFEST_PATH = os.path.join(GOLD_DIR, "gold_manifest.json")

def validate_gold_layer():
    print("Inspecting and Validating Gold Layer and AI Models...\n", flush=True)

    gold_features_path = os.path.join(GOLD_DIR, "gold_movies_features.parquet")
    if not os.path.exists(gold_features_path):
        print(f"Error: Missing Gold feature table: {gold_features_path}")
        return False

    df_gold = pd.read_parquet(gold_features_path)
    row_count = len(df_gold)
    col_count = len(df_gold.columns)
    size_kb = round(os.path.getsize(gold_features_path) / 1024, 2)
    null_soup = df_gold["metadata_soup"].isna().sum()

    print(f"[Gold Verified] gold_movies_features | Rows: {row_count:6d} | Cols: {col_count:2d} | Size: {size_kb:8.2f} KB | Null Soup: {null_soup}")

    # Check Recommender Artifacts
    tfidf_path = os.path.join(MODELS_DIR, "recommender_tfidf.joblib")
    index_path = os.path.join(MODELS_DIR, "recommender_index.joblib")
    recommender_status = "READY" if os.path.exists(tfidf_path) and os.path.exists(index_path) else "NOT_TRAINED"

    # Check Sentiment Artifacts
    sentiment_path = os.path.join(MODELS_DIR, "sentiment_pipeline.joblib")
    metrics_path = os.path.join(MODELS_DIR, "sentiment_metrics.json")
    sentiment_status = "READY" if os.path.exists(sentiment_path) and os.path.exists(metrics_path) else "NOT_TRAINED"

    sentiment_metrics = {}
    if os.path.exists(metrics_path):
        with open(metrics_path, "r", encoding="utf-8") as f:
            sentiment_metrics = json.load(f)

    # Smoke test Recommender if available
    rec_smoke_test = "SKIPPED"
    if recommender_status == "READY":
        try:
            from train_recommendation import MovieRecommender
            tfidf_dict = joblib.load(tfidf_path)
            df_idx = joblib.load(index_path)
            rec = MovieRecommender(tfidf_dict["vectorizer"], tfidf_dict["tfidf_matrix"], df_idx)
            recs = rec.recommend(df_gold.iloc[0]["title"], top_k=3)
            if recs:
                rec_smoke_test = f"PASSED (Found {len(recs)} recs in {recs[0]['query_time_ms']} ms)"
        except Exception as e:
            rec_smoke_test = f"FAILED ({e})"

    # Smoke test Sentiment if available
    sent_smoke_test = "SKIPPED"
    if sentiment_status == "READY":
        try:
            pipe = joblib.load(sentiment_path)
            pred = pipe.predict(["A great masterpiece with brilliant acting!"])[0]
            sent_smoke_test = f"PASSED (Predicted label: {pred})"
        except Exception as e:
            sent_smoke_test = f"FAILED ({e})"

    print(f"\n[Model Verified] Content-Based Recommender: {recommender_status} | Smoke Test: {rec_smoke_test}")
    print(f"[Model Verified] Sentiment Classifier:     {sentiment_status} | Smoke Test: {sent_smoke_test}")

    # Generate Manifest
    ingestion_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    manifest = {
        "pipeline_name": "Movies_Lakehouse_Gold_FeatureStore_and_ML",
        "timestamp": ingestion_time,
        "status": "SUCCESS",
        "feature_store": {
            "table_name": "gold_movies_features",
            "file_name": "gold_movies_features.parquet",
            "row_count": row_count,
            "col_count": col_count,
            "columns": list(df_gold.columns),
            "file_size_kb": size_kb
        },
        "models": {
            "recommender": {
                "type": "TF-IDF Vectorizer + Cosine Similarity",
                "status": recommender_status,
                "smoke_test": rec_smoke_test
            },
            "sentiment_classifier": {
                "type": "TF-IDF + Balanced Logistic Regression",
                "status": sentiment_status,
                "smoke_test": sent_smoke_test,
                "metrics": sentiment_metrics
            }
        }
    }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"\nGold Layer Manifest saved to: {MANIFEST_PATH}")
    return manifest

if __name__ == "__main__":
    validate_gold_layer()
