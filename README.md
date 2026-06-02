# 🚕 Chicago Taxi Pipeline

Production-grade data pipeline built on Google Cloud Platform. Ingests, transforms, and analyzes 2.5M+ real Chicago taxi trips with automated anomaly detection.

---

## 🏗️ Architecture

Airflow (local) → BigQuery Public Data → BigQuery (partitioned) → dbt → Looker Studio
→ Isolation Forest + SHAP

| Layer | Tool | Description |
|---|---|---|
| **Orchestration** | Apache Airflow | Monthly incremental DAG with backfill |
| **Source** | BigQuery Public Data | Chicago Taxi Trips dataset |
| **Storage** | BigQuery | Partitioned table by trip date |
| **Transformation** | dbt + BigQuery | Staging views and mart tables |
| **Anomaly Detection** | Isolation Forest + SHAP | 125K anomalies detected in 2.5M trips |
| **Dashboard** | Looker Studio | Business and anomaly analysis |

---

## 📊 Dashboard

[View Dashboard on Looker Studio](https://datastudio.google.com/reporting/43f73a6f-ea1d-4772-b41c-732ac6d5b919)

**Page 1 — General Analysis:**
- Trips by day of week
- Revenue by hour of day
- Top 10 companies by trips
- Payment method distribution

**Page 2 — Anomaly Detection:**
- Anomalies detected per day
- Top companies with anomalies
- Anomaly score distribution

---

## 🤖 Anomaly Detection Results

| Metric | Value |
|---|---|
| Total trips analyzed | 2,505,735 |
| Anomalies detected | 125,287 |
| Anomaly rate | 5.0% |
| Algorithm | Isolation Forest |
| Interpretability | SHAP values |

**Top features by SHAP importance:**

| Feature | Importance | Insight |
|---|---|---|
| day_of_week | 0.38 | Anomalies concentrate on specific days |
| trip_hour | 0.36 | Late night trips show unusual patterns |
| speed_zscore | 0.33 | Abnormal speeds indicate suspicious routes |
| miles_zscore | 0.30 | Unusual distances flag potential fraud |
| fare_zscore | 0.28 | Inflated fares correlate with anomalies |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Apache Airflow | Pipeline orchestration |
| Google BigQuery | Data warehouse |
| dbt-bigquery | SQL transformations |
| Python + pandas | Data processing |
| Isolation Forest | Anomaly detection |
| SHAP | Model interpretability |
| Looker Studio | BI Dashboard |

---

## 📁 Project Structure

    gcp-pipeline/
    ├── dags/
    │   └── taxi_pipeline.py       # Airflow DAG — incremental ingestion
    ├── dbt_taxi/
    │   └── models/
    │       ├── staging/           # Cleaned views with feature engineering
    │       └── marts/             # Business tables and anomaly features
    ├── src/
    │   └── models/
    │       └── anomaly_detection.py  # Isolation Forest + SHAP
    ├── utils/
    │   └── logger.py              # Centralized logging
    └── requirements.txt

---

## 🚀 How to Run

### 1. Clone and setup
```bash
git clone git@github.com:DylanRReexx/gcp-pipeline.git
cd gcp-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure GCP credentials
```bash
gcloud auth application-default login
gcloud config set project savvy-kit-494301-m6
```

### 3. Start Airflow
```bash
export AIRFLOW_HOME=~/airflow
airflow standalone
```

### 4. Run dbt transformations
```bash
cd dbt_taxi
dbt run
```

### 5. Run anomaly detection
```bash
python src/models/anomaly_detection.py
```

---

## 🧪 Data Quality

- MERGE pattern prevents duplicate loading
- Deduplication on unique_key before load
- dbt source freshness checks
- Z-score validation for outlier detection

---

## 📐 How to Scale

| Current | Production Scale |
|---|---|
| Airflow local | Cloud Composer on GCP |
| Monthly schedule | Daily or real-time with Pub/Sub |
| Manual backfill | Automated catchup with sensors |
| Single model | Model registry with MLflow |
| Looker Studio | Looker Enterprise |

---

## 👤 Author

**Dylan** — Systems Engineering Student @ ULATINA
[GitHub](https://github.com/DylanRReexx)