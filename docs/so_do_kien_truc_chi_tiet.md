# TÀI LIỆU SƠ ĐỒ KIẾN TRÚC TINH GỌN VÀ CHI TIẾT TỪNG LAYER (MODULAR ARCHITECTURE)

**Tác giả thực hiện**: Lê Trần Tuấn Anh  
**Đề tài**: XÂY DỰNG DATA PLATFORM SỬ DỤNG MÃ NGUỒN MỞ PHỤC VỤ HỆ THỐNG GỢI Ý PHIM  

---

## 1. SƠ ĐỒ KIẾN TRÚC TỔNG THỂ TINH GỌN (EXECUTIVE SYSTEM OVERVIEW)

Sơ đồ tổng thể được thiết kế thanh thoát, tập trung biểu diễn dòng chảy dữ liệu chính giữa 4 tầng công nghệ mà không gây rối mắt:

```mermaid
flowchart TD
    %% TẦNG 1: INGESTION
    subgraph T1 ["1. INGESTION TIER (Tầng Thu thập Dữ liệu)"]
        TMDB["TMDb REST API"] --> Client["Ingestion Core (tmdb_client.py)"]
        Client --> Crawler["Batch Crawler (tmdb_crawler.py)"]
        Client --> OnDemand["On-Demand Fetcher (on_demand_ingest.py)"]
    end

    %% TẦNG 2: LAKEHOUSE STORAGE
    subgraph T2 ["2. MEDALLION LAKEHOUSE STORAGE (Tầng Lưu trữ Bảng)"]
        Bronze["BRONZE ZONE<br/>(File CSV thô & Manifest Audit Log)"]
        Silver["SILVER ZONE<br/>(Bảng Apache Iceberg / Parquet sạch)"]
        Gold["GOLD ZONE<br/>(Feature Store Vectors & Analytics Mart)"]
    end

    %% TẦNG 3: ENGINE
    subgraph T3 ["3. COMPUTE & ORCHESTRATION TIER (Tính toán & Điều phối)"]
        Spark["Apache Spark Engine (PySpark)"]
        MinIO["MinIO Object Storage (S3 API)"]
        Dagster["Dagster Orchestrator"]
        
        Spark <--> MinIO
        Dagster --> Spark
    end

    %% TẦNG 4: SERVING & CONSUMPTION
    subgraph T4 ["4. SERVING & CONSUMPTION TIER (Phục vụ & Trực quan hóa)"]
        Flask["Flask REST API Backend"]
        WebUI["Modern Web UI (Dark Glassmorphism)"]
        Superset["Apache Superset BI Dashboards"]
        
        Flask <--> WebUI
    end

    %% LUỒNG NỐI CHÍNH (DATA LINEAGE CLEAR FLOW)
    Crawler -->|"1. Ingest dữ liệu thô"| Bronze
    Bronze -->|"2. Đọc CSV thô"| Spark
    Spark -->|"3. ETL Khử trùng & Phẳng hóa"| Silver
    Silver -->|"4. Đọc Parquet sạch"| Spark
    Spark -->|"5. Trích vector & Train AI"| Gold

    Gold -->|"6. Query Vector (< 50ms)"| Flask
    Gold -->|"7. SQL OLAP Queries"| Superset

    %% LUỒNG KÉP (DUAL-PATH FAST/SLOW LOOP)
    WebUI -. "A1. Cache Miss (Tìm phim hiếm)" .-> OnDemand
    OnDemand -. "A2. Fast-Path: Trả Data tạm (< 1s)" .-> Flask
    OnDemand -->|"A3. Slow-Path: Micro-Ingest"| Bronze
```

---

## 2. CHI TIẾT SƠ ĐỒ THEO TỪNG LAYER CHUYÊN BIỆT (MODULAR LAYER DIAGRAMS)

Được chia nhỏ thành 4 sơ đồ thành phần độc lập, dễ dàng xem kỹ cấu trúc từng phần:

---

### PHẦN 1: CHI TIẾT TẦNG THU THẬP VÀ LƯU THÔ (INGESTION & BRONZE LAYER)

```mermaid
flowchart LR
    TMDB["TMDb REST API"] -->|"1. HTTP GET (JSON)"| Client["tmdb_client.py<br/>(Rate Limiter 0.25s & Retries)"]
    
    subgraph Crawler_Box ["Batch Crawler (tmdb_crawler.py)"]
        Client --> Filter["Bộ lọc 4 Lớp<br/>• vote_count >= 50<br/>• runtime >= 40m<br/>• Poster & Overview<br/>• 5 Luồng Cào đa dạng"]
        Filter --> Incremental["Chống trùng Nối tiếp<br/>(Preload ID cũ)"]
    end

    subgraph Bronze_Store ["Bronze Storage (data/raw/)"]
        M["movies_metadata.csv (6.662 phim)"]
        R["reviews.csv (10.758 reviews)"]
        C["credits.csv & keywords.csv"]
        L["links.csv (imdb_id)"]
        Log["bronze_manifest.json"]
    end

    Incremental -->|"2. Append Data thô"| M & R & C & L
    M & R & C & L -->|"3. Ghi Audit Log"| Log
```

---

### PHẦN 2: CHI TIẾT TẦNG LÀM SẠCH VÀ CHUẨN HÓA (PYSPARK ETL & SILVER LAYER)

```mermaid
flowchart TD
    subgraph Bronze_Input ["Bronze Raw Input"]
        RawCSVs["data/raw/*.csv"]
    end

    subgraph Spark_ETL_Engine ["PySpark ETL Engine (transform_silver.py)"]
        Step1["1. dropDuplicates(['id'])<br/>Khử trùng lặp bản ghi"]
        Step2["2. Schema Enforcement & Type Casting<br/>• release_date -> DateType<br/>• budget/revenue -> DoubleType<br/>• vote_average -> FloatType"]
        Step3["3. JSON Flattening<br/>• Trích xuất genres -> genres_clean<br/>• Trích xuất cast/crew -> top_cast_clean & director_clean"]
        Step4["4. Text Clean & Sentiment Normalization<br/>• Lọc review_text > 20 chars<br/>• Gán nhãn positive/negative"]
        
        Step1 --> Step2 --> Step3 --> Step4
    end

    subgraph Silver_Output ["Silver Storage (data/silver/)"]
        SM["silver_movies.parquet"]
        SC["silver_credits.parquet"]
        SK["silver_keywords.parquet"]
        SR["silver_reviews.parquet"]
        Catalog["Iceberg Metadata Catalog"]
        
        SM & SC & SK & SR -. "Managed by" .-> Catalog
    end

    RawCSVs --> Step1
    Step4 -->|"Ghi bảng nén Snappy Parquet"| SM & SC & SK & SR
```

---

### PHẦN 3: CHI TIẾT TẦNG AI VÀ MA TRẬN VECTOR (GOLD LAYER & ML PIPELINE)

```mermaid
flowchart TD
    subgraph Silver_Tables ["Silver Parquet Tables"]
        SM["silver_movies.parquet"]
        SC["silver_credits.parquet"]
        SR["silver_reviews.parquet"]
    end

    subgraph ML_Engine ["PySpark MLlib Engine"]
        subgraph Rec_Model ["1. Mô hình Gợi ý Content-Based"]
            Comb["Gom thuộc tính: Overview + Genres + Cast + Keywords"]
            NLP["RegexTokenizer -> StopWordsRemover -> Word2Vec/TF-IDF"]
            Cosine["Ma trận Cosine Similarity: cos(θ) = (a·b)/(||a|| ||b||)"]
            Comb --> NLP --> Cosine
        end

        subgraph Sent_Model ["2. Mô hình Sentiment Analysis"]
            TFIDF_Text["TF-IDF Vectorizer"]
            LogReg["Logistic Regression Classifier<br/>Sigmoid σ(z) = 1/(1+e^-z)"]
            TFIDF_Text --> LogReg
        end
    end

    subgraph Gold_Storage ["Gold Storage (data/gold/)"]
        G_Feat["gold_movie_features.parquet<br/>(Vector Feature Store)"]
        G_Sent["gold_sentiment_model.bin<br/>(Serialized ML Model)"]
        G_Mart["gold_analytics_aggregates<br/>(OLAP BI Mart)"]
    end

    SM & SC --> Comb
    Cosine --> G_Feat
    SR --> TFIDF_Text
    LogReg --> G_Sent
    SM --> G_Mart
```

---

### PHẦN 4: CHI TIẾT TẦNG PHỤC VỤ VÀ LUỒNG KÉP FAST/SLOW PATH (SERVING & DUAL-PATH)

```mermaid
flowchart LR
    subgraph Gold_Store ["Gold Layer"]
        G_Feat["gold_movie_features"]
        G_Sent["gold_sentiment_model"]
        G_Mart["gold_analytics_aggregates"]
    end

    subgraph Flask_Backend ["Flask REST API Backend"]
        CacheRAM["RAM Feature Cache"]
        SearchAPI["/api/search"]
        RecAPI["/api/recommend"]
        SentAPI["/api/sentiment"]
    end

    subgraph Serving_Outputs ["Outputs Phục vụ"]
        WebUI["Modern Web UI (Dark Glassmorphism)"]
        Superset["Apache Superset BI Dashboards"]
    end

    subgraph Fast_Slow_Loop ["Luồng Kép (Dual-Path Loop)"]
        OnDemand["on_demand_ingest.py"]
        Bronze_CSV["Bronze Storage"]
    end

    G_Feat & G_Sent --> CacheRAM --> SearchAPI & RecAPI & SentAPI
    SearchAPI & RecAPI & SentAPI <-->|"JSON REST Protocols"| WebUI
    G_Mart -->|"SQL Queries"| Superset

    WebUI -. "1. Cache Miss (Tìm phim hiếm)" .-> OnDemand
    OnDemand -. "2a. FAST-PATH: Trả Data tạm (< 1s)" .-> Flask_Backend
    OnDemand -->|"2b. SLOW-PATH: Micro-Ingest"| Bronze_CSV
```

---

*Tài liệu sơ đồ kiến trúc này được chia nhỏ tinh gọn và chi tiết từng lớp theo thiết kế chuẩn của Lê Trần Tuấn Anh.*
