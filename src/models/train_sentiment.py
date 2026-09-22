"""
Sentiment Analysis Model Training Script.
Trains TF-IDF + Balanced Logistic Regression on user movie reviews.
Evaluates model on an independent stratified test set using Macro F1, Balanced Accuracy,
Precision/Recall, and ROC-AUC metrics.
Outputs: models/sentiment_pipeline.joblib & models/sentiment_metrics.json
"""

import os
import json
import joblib
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    accuracy_score, balanced_accuracy_score, f1_score,
    precision_score, recall_score, roc_auc_score,
    classification_report, confusion_matrix
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")

os.makedirs(MODELS_DIR, exist_ok=True)

def train_sentiment_model():
    print("Training Sentiment Analysis Classifier...\n", flush=True)

    reviews_path = os.path.join(SILVER_DIR, "silver_reviews.parquet")
    if not os.path.exists(reviews_path):
        raise FileNotFoundError(f"Missing Silver reviews dataset: {reviews_path}. Run transform_silver.py first.")

    print(f"[Sentiment 1/4] Loading Silver reviews: {reviews_path}...", flush=True)
    df_rev = pd.read_parquet(reviews_path)
    initial_len = len(df_rev)

    # 1. Clean and filter noisy short reviews (< 10 words)
    df_rev["word_count"] = df_rev["review_text"].fillna("").astype(str).str.split().str.len()
    df_rev = df_rev[df_rev["word_count"] >= 10].copy()
    filtered_len = len(df_rev)
    print(f"Loaded {initial_len} reviews | Kept {filtered_len} high-quality reviews (removed {initial_len - filtered_len} short reviews < 10 words).")

    # 2. Prepare Labels (1: positive, 0: negative)
    df_rev["label"] = df_rev["sentiment_label"].apply(lambda s: 1 if s == "positive" else 0)
    
    pos_count = (df_rev["label"] == 1).sum()
    neg_count = (df_rev["label"] == 0).sum()
    print(f"Class Distribution: Positive: {pos_count} ({pos_count/filtered_len*100:.1f}%) | Negative: {neg_count} ({neg_count/filtered_len*100:.1f}%)")

    X = np.array(df_rev["review_text"].tolist(), dtype=object)
    y = np.array(df_rev["label"].tolist(), dtype=int)

    # 3. Stratified Train/Test Split (80% Train, 20% Test)
    print("\n[Sentiment 2/4] Splitting dataset into Stratified Train/Test sets...", flush=True)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, stratify=y, random_state=42
    )
    print(f"Train Set: {len(X_train)} samples | Test Set: {len(X_test)} samples")

    # 4. Construct Scikit-learn Pipeline
    print("[Sentiment 3/4] Constructing and Fitting Scikit-learn ML Pipeline...", flush=True)
    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(
            max_features=10000,
            ngram_range=(1, 2),
            min_df=3,
            sublinear_tf=True,
            stop_words="english"
        )),
        ("clf", LogisticRegression(
            class_weight="balanced",
            max_iter=1000,
            random_state=42,
            C=1.0,
            solver="lbfgs"
        ))
    ])

    pipeline.fit(X_train, y_train)

    # 5. Evaluate on Independent Test Set
    print("[Sentiment 4/4] Evaluating Model Performance on Test Set...\n", flush=True)
    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    bal_acc = balanced_accuracy_score(y_test, y_pred)
    macro_f1 = f1_score(y_test, y_pred, average="macro")
    weighted_f1 = f1_score(y_test, y_pred, average="weighted")
    pos_f1 = f1_score(y_test, y_pred, pos_label=1)
    neg_f1 = f1_score(y_test, y_pred, pos_label=0)
    pos_prec = precision_score(y_test, y_pred, pos_label=1)
    pos_rec = recall_score(y_test, y_pred, pos_label=1)
    neg_prec = precision_score(y_test, y_pred, pos_label=0)
    neg_rec = recall_score(y_test, y_pred, pos_label=0)
    roc_auc = roc_auc_score(y_test, y_prob)
    cm = confusion_matrix(y_test, y_pred).tolist()

    report_dict = classification_report(y_test, y_pred, target_names=["negative", "positive"], output_dict=True)

    metrics = {
        "model_type": "TF-IDF + Balanced Logistic Regression",
        "total_reviews": filtered_len,
        "train_samples": len(X_train),
        "test_samples": len(X_test),
        "accuracy": round(acc, 4),
        "balanced_accuracy": round(bal_acc, 4),
        "macro_f1": round(macro_f1, 4),
        "weighted_f1": round(weighted_f1, 4),
        "roc_auc": round(roc_auc, 4),
        "positive_class": {
            "precision": round(pos_prec, 4),
            "recall": round(pos_rec, 4),
            "f1_score": round(pos_f1, 4)
        },
        "negative_class": {
            "precision": round(neg_prec, 4),
            "recall": round(neg_rec, 4),
            "f1_score": round(neg_f1, 4)
        },
        "confusion_matrix": {
            "true_negative": cm[0][0],
            "false_positive": cm[0][1],
            "false_negative": cm[1][0],
            "true_positive": cm[1][1]
        }
    }

    # Print Formatted Evaluation Report
    print("="*65)
    print("SENTIMENT CLASSIFIER EVALUATION REPORT (INDEPENDENT TEST SET)")
    print("="*65)
    print(f"Accuracy:          {acc*100:.2f}%")
    print(f"Balanced Accuracy: {bal_acc*100:.2f}%")
    print(f"Macro F1-Score:    {macro_f1*100:.2f}%")
    print(f"ROC-AUC:           {roc_auc:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=["negative", "positive"]))
    print(f"Confusion Matrix: TN={cm[0][0]}, FP={cm[0][1]} | FN={cm[1][0]}, TP={cm[1][1]}")
    print("="*65)

    # Save Model Pipeline and Metrics
    pipeline_path = os.path.join(MODELS_DIR, "sentiment_pipeline.joblib")
    metrics_path = os.path.join(MODELS_DIR, "sentiment_metrics.json")

    joblib.dump(pipeline, pipeline_path, compress=3)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    size_kb = round(os.path.getsize(pipeline_path) / 1024, 2)
    print(f"\nSaved Pipeline Artifact: {pipeline_path} ({size_kb} KB)")
    print(f"Saved Metrics Report:    {metrics_path}")

    # Inference Demonstration
    test_samples = [
        "An absolute masterpiece with stunning visuals, incredible cinematography, and brilliant acting.",
        "Terrible acting, boring plot, bad pacing, and a complete waste of time.",
        "The visuals were great, but the plot was predictable and characters felt empty."
    ]

    print("\n" + "="*65)
    print("DEMONSTRATION: REAL-TIME SENTIMENT INFERENCE")
    print("="*65)
    for text in test_samples:
        pred_label = pipeline.predict([text])[0]
        prob = pipeline.predict_proba([text])[0]
        label_str = "POSITIVE" if pred_label == 1 else "NEGATIVE"
        conf = prob[1] if pred_label == 1 else prob[0]
        print(f"\nReview: '{text}'")
        print(f"Prediction: {label_str} (Confidence: {conf*100:.2f}%)")

    print("\nSentiment Model Training Completed.")
    return pipeline, metrics

if __name__ == "__main__":
    train_sentiment_model()
