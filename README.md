# Delta Lake Payment Gateway Pipeline on GCP

**Status:** 🚧 In Progress | **Current Phase:** Bronze Layer Complete ✅  
**Tech Stack:** Delta Lake, Apache Spark, Airflow, GCP Dataproc, Cloud SQL, BigQuery

---

## 📋 Project Overview

### What Is This?

A production-grade data pipeline that processes payment gateway transactions using **Delta Lake lakehouse architecture** on Google Cloud Platform. The pipeline handles 15,000 daily transactions with built-in data quality validation, audit trail tracking, and flexible reprocessing capabilities.

### Why This Project?

**Business Context:**  
Modern payment gateways need to:
- Track transaction status changes (Pending → Successful → Refunded)
- Handle late-arriving data (transactions reported days after occurrence)
- Maintain compliance audit trails (prove transaction state at any point in time)
- Support data corrections (reprocess specific date ranges when errors occur)
- Query current state efficiently while preserving complete history
- Minimize Data Egress and Storage Costs for analytics and reporting by avoiding costly loading/reading operations from a proprietary data warehouse.

**Technical Challenge:**  
Traditional data warehouses (BigQuery-only) struggle with:
- ❌ Schema evolution (adding columns breaks downstream)
- ❌ Time travel (can't query "what was revenue on Dec 2 before refunds?")
- ❌ Vendor lock-in (proprietary format, hard to migrate)
- ❌ Expensive historical storage (pay for data you rarely query)
- ❌ High Read/Write Costs via the BigQuery Storage Connector, where enterprise-scale pipelines (30+ systems) reading/loading gigabytes of data daily incur substantial per-GB/TB costs.
- ❌ Inefficient Upserts/Deletes (Change Data Capture is complex and slow).

**Solution:**  
Delta Lake on GCP provides:
- ✅ ACID transactions (no duplicate/missing data)
- ✅ Schema evolution (add columns without breaking pipeline)
- ✅ Time travel (query any historical version)
- ✅ Open Format (Parquet + transaction log, portable, and allows changing between open formats like Delta, Hudi, or Iceberg).
- ✅ Direct Reporting & Cost Savings: Utilizing BigLake to query Delta tables in GCS directly for BI/reporting, eliminating the need for expensive data loading into BigQuery's managed storage layer.
- ✅ Upserts/Deletes (MERGE support for efficient Change Data Capture).

---

## 🎯 Problem Statement

**Scenario:**  
You're a data engineer at a payment gateway processing 5.4M transactions/year. Your current BigQuery pipeline has issues:

1. **Data Quality Problems:**  
   - 0.67% of transactions have NULL IDs → crash the pipeline
   - 2.67% have negative amounts → need flagging, not blocking
   - 1.33% missing device metadata → should default to "UNKNOWN"

2. **Late Arrivals:**  
   - 0.33% of transactions arrive 3+ days late (bank delays)
   - Current pipeline ignores them → revenue underreported

3. **Status Updates:**  
   - Transactions change status (Pending → Successful → Refunded)
   - Need to track full lifecycle, not just final state

4. **Reprocessing:**  
   - When upstream fixes data errors, need to reload specific dates
   - Current approach: reload entire history (expensive, slow)

**Your Task:**  
Build a Delta Lake pipeline that handles these issues elegantly while maintaining audit trail for compliance.

---

## 🏗️ Architecture

### High-Level Design

```
Raw CSV Files (GCS)
    ↓
┌─────────────────────────────────────┐
│ Bronze Layer (Raw + Validation)     │
│ - Full audit history                │
│ - 3-tier validation                 │
│ - Composite key: (txn_id, updated)  │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ Silver Layer (Cleaned + Deduped)    │
│ - Current state only                │
│ - Business rules applied            │
│ - Single key: txn_id                │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ Gold Layer (Star Schema)            │
│ - Fact: Transactions                │
│ - Dims: Customer, Merchant (SCD2)  │
│ - Optimized for BI queries          │
└─────────────────────────────────────┘
    ↓
Power BI / Tableau / Looker
```

### Why This Architecture? 

**Hybrid Strategy:**
- **Bronze = Full History:** Stores every transaction version (Pending → Successful → Refunded) using composite key `(transaction_id, updated_at)`. Enables compliance queries like "prove transaction status on Dec 2".
- **Silver = Current State:** Deduplicates Bronze to single version per transaction. Optimized for "what's happening now" queries.
- **Gold = Analytics:** Star schema with fact table + dimensions. BI tools query this for dashboards.

**Why not store history in Gold?**  
Gold prioritizes query performance. Storing 3 versions of every transaction slows down "current revenue" queries. Bronze serves compliance, Gold serves analytics.

---

## 🔧 Technical Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Storage** | Google Cloud Storage | Raw CSV files, Delta Lake Parquet files |
| **Compute** | Dataproc 2.2 (Spark 3.3) | PySpark job execution |
| **Metastore** | Cloud SQL MySQL | External Hive metastore for Delta tables |
| **Orchestration** | Airflow Composer 3 | DAG scheduling, retry logic, monitoring |
| **Table Format** | Delta Lake 2.4.0 | ACID transactions, time travel, MERGE |
| **Query Engine** | BigQuery (external tables) | SQL queries on Delta tables via BigLake |
| **Security** | Cloud KMS | Encrypted Hive metastore password |

**Why These Choices?**

**Dataproc over EMR/Databricks:**  
- Native GCP integration (no cross-cloud complexity)
- Ephemeral clusters (cost optimization: $0.40/hour vs always-on)
- External metastore (metadata persists, clusters are disposable)

**Airflow over Cloud Workflows:**  
- Native Delta Lake support (read/write Delta tables in Python)
- Rich ecosystem (sensors, operators, retry logic)
- Industry standard (transferable skill)

**External Hive Metastore over Dataproc Metastore Service:**  
- Full control (Cloud SQL is standard MySQL, easy debugging)
- Cost-effective (Metastore Service = $1/hour, Cloud SQL = $50/month)
- Encrypted at rest (KMS integration)

---

## ✨ Key Features

### 1. 3-Tier Data Quality Validation

**Tier 1 - Block & Quarantine (Critical):**
```python
# NULL transaction_id → Cannot proceed, send to quarantine
if transaction_id IS NULL:
    quarantine(reason="NULL_TRANSACTION_ID")
```
**Action:** 0.67% of records quarantined, manual review required

**Tier 2 - Flag & Load (Business Rules):**
```python
# Negative amount → Suspicious, but load with warning flag
if amount < 0:
    flag(data_quality_flag="FAILED_VALIDATION")
    load_to_bronze()
```
**Action:** 2.67% of records flagged, analysts investigate

**Tier 3 - Fix & Load (Missing Optional Data):**
```python
# NULL device_type → Apply default
device_type = COALESCE(device_type, 'UNKNOWN')
```
**Action:** 1.33% of records auto-fixed

**Result:** 96.67% of data flows cleanly, 2.67% flagged for review, 0.67% quarantined

---

### 2. Composite Key for Audit Trail

**Challenge:** Transaction changes status over time:
```
Day 1: TXN001, Pending
Day 4: TXN001, Successful
Day 5: TXN001, Refunded
```

**Traditional approach (single key):** Only store latest version → lose history

**Delta Lake MERGE with single key:**
```sql
MERGE INTO transactions t USING updates s ON t.transaction_id = s.transaction_id
-- Problem: Which version to keep when 3 updates arrive at once?
-- Error: DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
```

**Our solution (composite key):**
```sql
MERGE INTO transactions t USING updates s 
ON t.transaction_id = s.transaction_id AND t.updated_at = s.updated_at
-- Works: Each version has unique (transaction_id, updated_at) pair
```

**Benefit:** Complete audit trail for compliance, time travel queries

---

### 3. Multiple Loading Patterns

**Incremental Load (Daily Production):**
```python
# Load only new data since last watermark
WHERE transaction_timestamp > last_watermark 
   OR (transaction_timestamp <= last_watermark AND updated_at > last_watermark)
```
**Use case:** Daily 2 AM job, process yesterday's transactions

**Backfill (Date Range Reprocessing):**
```python
# Reload specific dates after data correction
WHERE DATE(transaction_timestamp) BETWEEN '2025-11-29' AND '2025-12-01'
```
**Use case:** Upstream sent bad data on Nov 30, fix and reload just that day

**Full Refresh (Complete Rebuild):**
```python
# Reload everything from scratch
INSERT OVERWRITE transactions SELECT * FROM raw_data
```
**Use case:** Major schema change, disaster recovery, or testing

---

### 4. Late Arrival Handling

**Problem:** Bank reports transaction 5 days late:
```
Transaction occurred: Nov 29
Received in pipeline: Dec 4
```

**Detection:**
```python
is_late_arrival = (transaction_timestamp < last_watermark) AND (updated_at > last_watermark)
arrival_delay_hours = (updated_at - transaction_timestamp) / 3600
```

**Action:** Flag and process, don't reject (0.33% of transactions)

---

## 📂 Project Structure

```
delta-lake-gcp-implementation/
│
├── README.md                          # This file
│
├── bronze/                            # Bronze layer (current phase)
│   ├── README.md                      # Bronze documentation
│   ├── TESTING_GUIDE.md               # Test scenarios & validation
│   ├── jobs/                          # PySpark jobs
│   │   ├── validate_bronze.py         # 3-tier validation
│   │   ├── load_bronze.py             # MERGE with composite key
│   │   ├── bronze_backfill.py         # Date range reprocessing
│   │   └── bronze_full_refresh.py     # Complete reload
│   └── dags/                          # Airflow DAGs
│       ├── bronze_incremental_dag.py  # Daily production load
│       ├── bronze_backfill_dag.py     # Manual date range reload
│       └── bronze_full_refresh_dag.py # Full rebuild (with confirmation)
│
├── silver/                            # Silver layer (next phase)
│   └── (coming soon)
│
├── gold/                              # Gold layer (future)
│   └── (coming soon)
│
└── docs/                              # Shared documentation
    ├── MIGRATION_DOC_COMPLETE.md      # Complete project context
    ├── DATA_LINEAGE.md                # Data flow documentation
    ├── VALIDATION_RULES.md            # Quality rules details
    └── SCHEMA_REGISTRY.md             # All table schemas
```

---

## 🚀 How to Run

### Prerequisites

1. **GCP Project** with these services enabled:
   - Dataproc API
   - Cloud SQL Admin API
   - Cloud Storage
   - Cloud Composer
   - BigQuery
   - Cloud KMS

2. **Infrastructure Setup** (from Blog 3):
   - Cloud SQL MySQL (Hive metastore)
   - Dataproc 2.2-debian12 cluster
   - GCS bucket with Delta tables
   - KMS-encrypted metastore password
   - BigQuery external tables via BigLake

3. **Test Data:**
   - Upload CSVs to `gs://your-bucket/raw/20241202/`
   - See `bronze/TESTING_GUIDE.md` for data generation

### Quick Start

**1. Clone repository:**
```bash
git clone https://github.com/yourusername/delta-lake-gcp-implementation.git
cd delta-lake-gcp-implementation
```

**2. Deploy Spark jobs to GCS:**
```bash
gsutil cp bronze/jobs/*.py gs://your-bucket/airflow/jobs/
```

**3. Deploy DAGs to Composer:**
```bash
gsutil cp bronze/dags/*.py gs://your-composer-dags-bucket/dags/
```

**4. Trigger incremental load:**
- Go to Airflow UI → `bronze_incremental_load` → Trigger DAG
- Wait 8-10 minutes
- Verify: Query `bronze.transactions` in BigQuery

**5. Test backfill:**
```json
{
  "start_date": "2025-11-29",
  "end_date": "2025-12-01"
}
```
Trigger `bronze_backfill` DAG with this config

### Manual Testing (No Airflow)

```bash
# Validate data
gcloud dataproc jobs submit pyspark \
  gs://your-bucket/airflow/jobs/validate_bronze.py \
  --cluster=your-cluster \
  --region=us-central1 \
  -- gs://your-bucket/raw/20241202/day*.csv NULL batch-test-001

# Load data
gcloud dataproc jobs submit pyspark \
  gs://your-bucket/airflow/jobs/load_bronze.py \
  --cluster=your-cluster \
  --region=us-central1 \
  -- batch-test-001 bronze_incremental_load incremental 90000 100 2025-12-07T10:00:00
```

---

## 📊 Results & Metrics

**Bronze Layer (Current Phase):**
- ✅ 89,800 records loaded (90K - 100 tier1 - 100 late updates)
- ✅ 100 records quarantined (0.67% - Tier 1 failures)
- ✅ 400 records flagged (2.67% - Tier 2 violations)
- ✅ 200 records auto-fixed (1.33% - Tier 3 defaults)
- ✅ 150 status updates tracked (multiple versions per transaction)

**Performance:**
- Incremental load: 8-10 min (ephemeral cluster)
- Backfill (3 days): 8-9 min
- Full refresh: 8-10 min
- Query latency: <2 sec (BigQuery external tables)

**Cost Optimization:**
- Ephemeral clusters: $0.40/hour (vs $292/month always-on)
- Lifecycle management: Auto-delete after 10 min idle
- Monthly cost: ~$53 + usage (vs $345 with persistent cluster)

---

## 📝 Blog Posts

Follow the implementation journey on Medium:

- **Blog 1-2:** [BigQuery-Native Pipeline](link-to-blog) (baseline)
- **Blog 3:** [Delta Lake Setup on GCP](link-to-blog) (infrastructure)
- **Blog 3a:** [Bronze Layer Implementation](link-to-blog) ✅ **Current**
- **Blog 3b:** Silver Layer (coming soon)
- **Blog 3c:** Gold Layer - Star Schema (coming soon)
- **Blog 3d:** CDC & Advanced Patterns (coming soon)
- **Blog 3e:** Operations & Optimization (coming soon)

---

## 🎯 What's Next

### Silver Layer (Blog 3b)
- Deduplicate Bronze to current state
- Apply business transformation rules
- Handle late arrival updates
- Soft delete propagation

### Gold Layer (Blog 3c)
- Star schema design (fact + dimensions)
- SCD Type 2 for customers/merchants
- Surrogate key generation
- BI-optimized queries

### Operations (Blog 3e)
- Compaction (merge small files)
- Z-ordering (locality optimization)
- Vacuum (delete old file versions)
- Monitoring dashboards

---

## 🤝 Contributing

- This project serves as a proof-of-concept and validation environment for building a robust, cost-efficient, and audit-compliant data architecture using Delta Lake on Google Cloud Platform (GCP).
- Your feedback is highly valued as it helps validate the architecture's assumptions and utility in real-world enterprise scenarios.
- If you find this project valuable or have ideas for improvement, please consider the following ways to engage:
**Ways to Engage and Contribute:**
- ⭐ Star the repo
- 🐛 Report Bugs/Issues
- 💡 Suggest improvements
- 📝 Share Your Delta Lake Journey by discussing how you are applying similar concepts or overcoming challenges in your own data environment.

---

## 📄 License

MIT License - Feel free to use this for learning/portfolio projects
- Attribution Request: If you use this repository as a basis for your own public work, please link back to it. This helps increase the project's visibility and reach.

---

## 👤 Author

**[mohamed Kashifuddin]**  
Data Engineer | Delta Lake Enthusiast | Cloud Architecture

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohamedkashifuddin/)
[![Medium](https://img.shields.io/badge/Medium-12100E?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@mohamed_kashifuddin)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/mohamedkashifuddin)
[![Portfolio](https://img.shields.io/badge/Portfolio-FF7139?style=for-the-badge&logo=Firefox&logoColor=white)](https://mohamedkashifuddin.pages.dev)

📧 Email: mohamedkashifuddin24@gmail.com

---

## 🙏 Acknowledgments

- Delta Lake community for documentation
- Google Cloud for free tier credits
- Medium data engineering community for inspiration

---

**Built with ❤️ using Delta Lake, Spark, and way too much coffee ☕**