<div align="center">

# 🍿 MOVIE & TV SHOW DATA LAKEHOUSE PLATFORM
### *End-to-End Open-Source Data Engineering & AI Recommendation System*

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4+-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-ACID_Table-blue?style=for-the-badge&logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Storage-C7254E?style=for-the-badge&logo=minio&logoColor=white)](https://min.io/)
[![Flask](https://img.shields.io/badge/Flask-Serving_API-000000?style=for-the-badge&logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](LICENSE)

---

**Tác giả thực hiện:** [Lê Trần Tuấn Anh](https://github.com/tuanh250105)  
**Giảng viên hướng dẫn:** ThS. Nguyễn Văn Thành  
**Đề tài:** *Xây dựng Data Platform sử dụng mã nguồn mở phục vụ hệ thống gợi ý phim*

</div>

---

## 📌 TỔNG QUAN DỰ ÁN (PROJECT OVERVIEW)

**Movie & TV Show Data Lakehouse Platform** là một hệ thống Data Platform hiện đại được thiết kế theo kiến trúc **Data Lakehouse (Medallion Architecture: Bronze ➔ Silver ➔ Gold)**. 

Dự án thu thập, xử lý và quản trị dữ liệu điện ảnh quy mô lớn từ **TMDb API**, phục vụ đồng thời cho cả **Data Engineering**, **Machine Learning** (Gợi ý phim Content-Based & Phân tích cảm xúc Sentiment Analysis) và **Business Intelligence (BI)**.

### 🌟 ĐIỂM SÁNG KIẾN TRÚC: KIẾN TRÚC LUỒNG KÉP (DUAL-PATH ARCHITECTURE)
Để tối ưu hóa mâu thuẫn giữa độ trễ Web UI (< 1s) và chi phí tính toán nặng của PySpark Batch ETL:
- ⚡ **Fast-Path Serving (< 1s)**: Khi xảy ra Cache Miss (người dùng tìm phim hiếm chưa có trong DB), `on_demand_ingest.py` cào nhanh metadata từ TMDb API, nạp tạm vào RAM Flask Backend và trả kết quả tức thời cho Web UI trong **< 1 giây**.
- 🐢 **Slow-Path Async Batch Engine**: Đồng thời, dữ liệu thô được nạp vào **Bronze Layer** và kích hoạt tiến trình **PySpark ETL running in background** để làm sạch, phẳng hóa JSON, ghi bảng **Apache Iceberg (Silver)** và cập nhật Ma trận Vector toàn cục ở **Gold Layer**.

---

## 🏗️ SƠ ĐỒ KIẾN TRÚC TỔNG THỂ (SYSTEM ARCHITECTURE)

```mermaid
flowchart TB
    subgraph INGESTION["1. INGESTION & DATA COLLECTION TIER (Tầng Thu thập Dữ liệu)"]
        node_tmdb["External API: TMDb REST Services"]
        node_client["Ingestion Core: tmdb_client.py (Auth, Rate Limiter & Retry)"]
        node_crawler["Batch Ingestion: tmdb_crawler.py (5-Stream Crawl & 4-Layer Filter)"]
        node_ondemand["Event Ingestion: on_demand_ingest.py (Realtime Catch-all, Async)"]
    end

    subgraph BRONZE_ZONE["BRONZE ZONE (Raw / Unprocessed Data - Schema-on-Read)"]
        b_movies["movies_metadata.csv"]
        b_reviews["reviews.csv"]
        b_credits["credits.csv"]
        b_keywords["keywords.csv"]
        b_manifest["bronze_manifest.json (Audit Log)"]
    end

    subgraph SILVER_ZONE["SILVER ZONE (Trusted Data - Schema Enforced Iceberg Tables)"]
        s_movies["silver_movies (Iceberg / Parquet)"]
        s_credits["silver_credits (Flattened JSON)"]
        s_keywords["silver_keywords (Flattened)"]
        s_reviews["silver_reviews (Cleaned Sentiment)"]
        s_catalog["Iceberg Metadata Catalog (Snapshots & Manifest Tree)"]
    end

    subgraph GOLD_ZONE["GOLD ZONE (Curated Data / Feature Store & Analytics Mart)"]
        g_features["gold_movie_features (Vector Feature Store)"]
        g_sentiment["gold_sentiment_model (Serialized ML Model)"]
        g_marts["gold_analytics_aggregates (OLAP BI Mart)"]
    end

    subgraph LAKEHOUSE["2. MEDALLION DATA LAKEHOUSE TIER (Tầng Lưu trữ & Quản trị Bảng)"]
        BRONZE_ZONE
        SILVER_ZONE
        GOLD_ZONE
    end

    subgraph ENGINE_TIER["3. COMPUTE & ORCHESTRATION TIER (Động cơ Tính toán & Điều phối)"]
        node_spark["Distributed Compute: Apache Spark (PySpark Core & MLlib)"]
        node_dagster["Data Pipeline Orchestration: Dagster (Software-Defined Assets)"]
        node_minio["Object Storage: MinIO (Amazon S3 Compatible API)"]
    end

    subgraph SERVING_TIER["4. SERVING & CONSUMPTION TIER (Tầng Phục vụ & Trực quan hóa)"]
        node_flask["Application Backend: Flask REST API Engine"]
        node_webui["End-User Client: Modern Web UI (Dark Glassmorphism)"]
        node_superset["BI & Analytics: Apache Superset (SQL OLAP Dashboards)"]
    end

    node_tmdb -->|"HTTPS GET (Raw JSON)"| node_client
    node_client -->|"Rate-Limited Session (0.25s)"| node_crawler
    node_client -->|"Rate-Limited Session (0.25s)"| node_ondemand

    s_movies -. "Managed by" .-> s_catalog
    s_credits -. "Managed by" .-> s_catalog
    s_keywords -. "Managed by" .-> s_catalog
    s_reviews -. "Managed by" .-> s_catalog

    node_dagster -->|"Orchestrates Scheduled Assets"| node_spark
    node_spark <-->|"S3 Protocol (s3a://)"| node_minio
    node_spark -. "Query Snapshots / Time Travel" .-> s_catalog

    node_flask <-->|"JSON REST Protocols"| node_webui

    node_crawler -. "Write Raw Files" .-> node_minio

    node_crawler -->|"1. Raw Movies Ingestion (Append-Only)"| b_movies
    node_crawler -->|"1. Raw Reviews Ingestion"| b_reviews
    node_crawler -->|"1. Raw Credits Ingestion"| b_credits
    node_crawler -->|"1. Raw Keywords Ingestion"| b_keywords
    node_crawler -. "1. Write Audit Entry" .-> b_manifest

    b_manifest -. "Pipeline Health / Observability" .-> node_dagster

    b_movies -->|"2. Batch Read Raw CSVs"| node_spark
    b_reviews -->|"2. Batch Read Raw CSVs"| node_spark
    b_credits -->|"2. Batch Read Raw CSVs"| node_spark
    b_keywords -->|"2. Batch Read Raw CSVs"| node_spark

    node_spark -->|"3. Silver Transformation ETL (Deduplicate, Cast, Flatten JSON)"| s_movies
    node_spark -->|"3. Clean & Normalize Reviews"| s_reviews
    node_spark -->|"3. Flatten Credits"| s_credits
    node_spark -->|"3. Flatten Keywords"| s_keywords

    s_movies -->|"4. Read Trusted Parquet Tables"| node_spark
    s_credits -->|"4. Read Trusted Parquet Tables"| node_spark
    s_keywords -->|"4. Read Trusted Parquet Tables"| node_spark
    s_reviews -->|"4. Read Trusted Parquet Tables"| node_spark

    node_spark -->|"5. ML Feature Extraction (Genres + Keywords + Cast via Word2Vec/TF-IDF)"| g_features
    node_spark -->|"5. Train Sentiment Classifier (Logistic Reg.)"| g_sentiment
    node_spark -->|"5. Build BI Aggregates"| g_marts

    g_features -->|"6. Low-Latency Feature Query (< 50ms)"| node_flask
    g_sentiment -->|"7. Realtime Sentiment Inference"| node_flask
    g_marts -->|"8. SQL OLAP Queries"| node_superset

    node_webui -. "A1. Search Miss Event (Cache Miss)" .-> node_ondemand
    node_ondemand -. "A2. Fast-Path Response (Bypass ETL, Minimal Fields)" .-> node_flask
    node_ondemand -->|"A3. Async Micro-Ingest"| b_movies
    node_ondemand -->|"A3. Async Micro-Ingest"| b_credits
    node_ondemand -->|"A3. Async Micro-Ingest"| b_reviews

    b_movies -. "A4. Next Scheduled Run Picks Up New Rows" .-> node_dagster
    b_credits -. "A4. Next Scheduled Run Picks Up New Rows" .-> node_dagster
    b_reviews -. "A4. Next Scheduled Run Picks Up New Rows" .-> node_dagster
```

---

## 🛠️ TÍNH NĂNG KỸ THUẬT NỔI BẬT (KEY TECHNICAL FEATURES)

1. **Storage-Compute Decoupling**: Tách biệt hoàn toàn PySpark (Compute Engine) và MinIO (S3 Object Storage).
2. **Bộ cào TMDb 5 Luồng & Bộ lọc 4 Lớp**: Cào tự động Phim Hot, Phim Kinh điển, Phim Trending, Phim Hàng độc (Cult Classics) và Phim Bộ (TV Series) dựa trên bộ lọc số vote, thời lượng và tính toàn vẹn dữ liệu.
3. **Cơ chế Chống trùng Nối tiếp (Incremental Crawl)**: Tự động tải danh sách ID cũ vào bộ nhớ, đảm bảo mọi lần cào sau chỉ lấy phim mới 100%.
4. **PySpark ETL Silver Layer**: Khử trùng lặp bản ghi (`dropDuplicates`), ép kiểu chuẩn (`release_date`, `budget`, `revenue`, `vote_average`), phẳng hóa các mảng JSON phức tạp (`genres`, `cast`, `crew`, `keywords`).
5. **AI Feature Store & Sentiment Analysis**:
   - **Content-Based Recommendation**: Chuỗi NLP (`RegexTokenizer` ➔ `StopWordsRemover` ➔ `Word2Vec`/`TF-IDF`) ➔ Tính tương đồng góc **Cosine Similarity**.
   - **Sentiment Analysis**: Trọng số **TF-IDF** ➔ Phân loại cảm xúc bằng **Logistic Regression Classifier**.

---

## 📁 CẤU TRÚC THƯ MỤC REPOSITORY

```
.
├── .env.example                        # Mẫu file cấu hình biến môi trường bảo mật
├── .gitignore                          # Cấu hình chặn commit secret & dữ liệu thô
├── README.md                           # Tài liệu hướng dẫn GitHub Repository
├── architecture_diagram.drawio.xml     # Sơ đồ kiến trúc dạng XML Draw.io
├── docs/                               # Thư mục chứa toàn bộ tài liệu kỹ thuật & báo cáo
│   ├── tai_lieu_ky_thuat_he_thong.md   # Tài liệu Kỹ thuật hệ thống tổng hợp
│   ├── bao_cao_y_tuong_va_tien_do_gui_gvhd.md # Báo cáo tiến độ gửi Giảng viên hướng dẫn
│   └── so_do_kien_truc_chi_tiet.md   # Sơ đồ Kiến trúc chi tiết các Layer (Mermaid)
├── data/
│   ├── raw/                            # Bronze Layer Storage (6.662 phim & 10.758 reviews)
│   │   ├── movies_metadata.csv
│   │   ├── credits.csv
│   │   ├── keywords.csv
│   │   ├── links.csv
│   │   ├── ratings.csv
│   │   ├── reviews.csv
│   │   └── bronze_manifest.json
│   └── silver/                         # Silver Layer Storage (Bảng Parquet/Iceberg)
└── src/
    ├── ingestion/                      # Bộ mã nguồn Cào & Nạp dữ liệu thô (Bronze)
    │   ├── tmdb_client.py              # Client kết nối TMDb API (Auth & Rate Limiting 0.25s)
    │   ├── tmdb_crawler.py             # Script cào 5 luồng tích hợp Bộ lọc 4 Lớp & Incremental Crawl
    │   ├── on_demand_ingest.py         # Module cào động Fast-Path cho Web UI (< 1s)
    │   └── validate_bronze.py          # Validator kiểm tra dữ liệu thô & ghi manifest
    └── pipeline/                       # Bộ mã nguồn PySpark ETL (Silver Layer)
        ├── spark_session.py            # Khởi tạo PySpark Lakehouse Session
        ├── transform_silver.py         # PySpark ETL (Khử trùng, ép kiểu, phẳng hóa JSON)
        └── validate_silver.py          # Validator kiểm tra bảng sạch & ghi manifest
```

---

## 🚀 HƯỚNG DẪN CHẠY DỰ ÁN (QUICKSTART GUIDE)

### 1. Cài đặt Môi trường & Khởi tạo Secret `.env`
```bash
# Clone repository về máy
git clone https://github.com/tuanh250105/movie-data-platform.git
cd movie-data-platform

# Tạo virtual environment và cài đặt dependencies
python -m venv venv
source venv/bin/activate  # Trên Windows: venv\Scripts\activate
pip install -r requirements.txt

# Tạo file .env chứa TMDB_API_KEY bảo mật
echo "TMDB_API_KEY=your_tmdb_api_key_here" > .env
echo "TMDB_BASE_URL=https://api.themoviedb.org/3" >> .env
```

### 2. Chạy Cào Dữ liệu Thật vào Bronze Layer (`data/raw/`)
```bash
# Cào 1.000 phim & TV series mới (từ 1990 - 2026)
python src/ingestion/tmdb_crawler.py --limit 1000 --start-year 1990 --end-year 2026

# Hoặc hẹn giờ cào tự động đúng 5 phút
python src/ingestion/tmdb_crawler.py --limit 3000 --max-runtime-min 5

# Kiểm tra báo cáo Bronze Layer Manifest
python src/ingestion/validate_bronze.py
```

### 3. Thực thi PySpark ETL Làm sạch Dữ liệu vào Silver Layer (`data/silver/`)
```bash
# Thực thi PySpark ETL làm sạch, khử trùng lặp & phẳng hóa JSON
python src/pipeline/transform_silver.py

# Kiểm tra báo cáo Silver Layer Manifest
python src/pipeline/validate_silver.py
```

---

## 📄 GIẤY PHÉP & BẢN QUYỀN (LICENSE & CREDITS)

- Dự án được phát hành dưới giấy phép mã nguồn mở [MIT License](LICENSE).
- Dữ liệu điện ảnh được cung cấp qua API chính thức từ [The Movie Database (TMDb)](https://www.themoviedb.org/).

<div align="center">
  <sub>Built with ❤️ by <b>Lê Trần Tuấn Anh</b> under guidance of <b>ThS. Nguyễn Văn Thành</b></sub>
</div>
