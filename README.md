# Delta Lake Payment Gateway Pipeline on GCP

**Status:** 🚧 In Progress | **Current Phase:** Silver Layer Complete ✅  
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
- Minimize storage and compute costs for analytics workloads

**Technical Challenge:**  
Traditional data warehouses (BigQuery-only) have limitations:
- ❌ Expensive upserts/deletes (Change Data Capture is complex and slow)
- ❌ Vendor lock-in (proprietary format, hard to migrate)
- ❌ Expensive historical storage (pay for data you rarely query)
- ❌ High data movement costs (loading/reading via BigQuery Storage Connector incurs per-GB fees)
- ❌ Schema evolution complexity (adding columns can break downstream)

**Solution:**  
Delta Lake on GCP provides:
- ✅ ACID transactions (no duplicate/missing data)
- ✅ Schema evolution (add columns without breaking pipeline)
- ✅ Time travel (query any historical version via Delta log)
- ✅ Open format (Parquet + transaction log, portable across lakehouse platforms)
- ✅ **Cost savings:** Query Delta tables in GCS directly via BigLake (no data movement, no BigQuery Storage Connector fees)
- ✅ Efficient upserts/deletes (MERGE support for CDC operations)

**Cost Comparison (25-50 TB Dataset):**
- BigQuery managed storage: $512-1,024/month (storage only)
- Delta Lake on GCS: Same storage cost, but **no per-GB read/write fees** for BI tools querying via BigLake
- **Savings:** Eliminates data movement costs for enterprise pipelines (30+ systems reading gigabytes daily)

---

## 🎯 Problem Statement

**Scenario:**  
You're a data engineer at a payment gateway processing 5.4M transactions/year. Your current BigQuery pipeline has issues:

1. **Data Quality Problems:**  
   - 0.67% of transactions have NULL IDs → crash the pipeline
   - 60.67% have negative amounts or invalid merchants → need flagging, not blocking
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

5. **Compliance:**  
   - GDPR/CCPA "Right to be Forgotten" requires permanent deletion
   - Audit logs must retain history (conflicting requirements)

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
│ - 25 columns (17 orig + 8 tracking) │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ Silver Layer (Cleaned + Deduped)    │
│ - Current state only                │
│ - Business rules applied            │
│ - Single key: txn_id                │
│ - 21 columns (removed 4 Bronze-only)│
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
(Query via BigQuery BigLake - No data movement!)
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
**Action:** 60.67% of records flagged, analysts investigate (increased for testing)

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

### 5. GDPR Compliance (Dual-Delete Pattern)

**Challenge:** GDPR says "delete all data" but auditors say "keep logs"

**Solution:**
- **Bronze:** Soft delete (`is_deleted = true`, data preserved for audit)
- **Silver:** Hard delete (data permanently removed from analytics)

**Workflow:**
```bash
# Step 1: Mark deleted in Bronze (audit trail)
bronze_mark_deleted_by_customer.py --customer_id=USER_0331

# Step 2: Remove from Silver (analytics clean)
silver_propagate_deletes.py USER_0331
```

**Result:**
- Compliance team can prove deletion (Bronze metadata)
- Business users never see deleted data (Silver removed)
- Legal requirement satisfied (customer data purged from analytics)

---

### 6. Intra-Batch Deduplication

**Problem Discovered:** Same CSV file had exact duplicate rows (data generator bug + real-world edge case)

**Solution:** Added ROW_NUMBER deduplication in all Bronze jobs:
```python
CREATE OR REPLACE TEMP VIEW bronze_staging AS
SELECT * FROM (
    SELECT *, 
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id, updated_at 
            ORDER BY transaction_id
        ) AS row_num
    FROM filtered_data
) WHERE row_num = 1
```

**Impact:**
- Files updated: `validate_bronze.py`, `bronze_backfill.py`, `bronze_full_refresh.py`
- Overhead: ~5% slower (worth it to prevent MERGE errors)
- Defensive coding: Handles upstream retry logic, CDC duplicates

---

## 📂 Project Structure

```
delta-lake-gcp-implementation/
│
├── README.md                          # This file
│
├── bronze/                            # Bronze layer (Blog 3a - Complete ✅)
│   ├── README.md                      # Bronze documentation
│   ├── TESTING_GUIDE.md               # Test scenarios & validation
│   ├── jobs/                          # PySpark jobs (4 total)
│   │   ├── validate_bronze.py         # 3-tier validation + deduplication
│   │   ├── load_bronze.py             # MERGE with composite key
│   │   ├── bronze_backfill.py         # Date range reprocessing
│   │   └── bronze_full_refresh.py     # Complete reload
│   └── dags/                          # Airflow DAGs (3 total)
│       ├── bronze_incremental_dag.py  # Daily production load
│       ├── bronze_backfill_dag.py     # Manual date range reload
│       └── bronze_full_refresh_dag.py # Full rebuild (with confirmation)
│
├── silver/                            # Silver layer (Blog 3b - Complete ✅)
│   ├── README.md                      # Silver documentation
│   ├── RUNBOOK.md                     # Operations guide
│   ├── HELPER.md                      # Developer extension guide
│   ├── jobs/                          # PySpark jobs (5 total)
│   │   ├── validate_silver.py         # Read Bronze, dedupe, write staging
│   │   ├── load_silver.py             # MERGE staging → silver
│   │   ├── silver_full_refresh.py     # Rebuild from all Bronze
│   │   ├── bronze_mark_deleted_by_customer.py  # GDPR soft delete
│   │   └── silver_propagate_deletes.py         # GDPR hard delete
│   └── dags/                          # Airflow DAGs (3 total)
│       ├── silver_incremental_dag.py  # Daily after Bronze completes
│       ├── silver_full_refresh_dag.py # Manual rebuild
│       └── bronze_compliance_deletion_dag.py  # GDPR workflow
│
├── gold/                              # Gold layer (Blog 3c - Coming Soon)
│   └── (next phase)
│
├── data_generator/                    # Test data generation
│   ├── generate_payment_data.py       # Enhanced with Silver test data
│   └── generated_data/                # Output: day1.csv, day2.csv, ...
│
└── docs/                              # Shared documentation
    ├── MIGRATION_DOC_COMPLETE.md      # Complete project context
    ├── DATA_LINEAGE.md                # Data flow documentation
    ├── VALIDATION_RULES.md            # Quality rules details
    ├── SCHEMA_REGISTRY.md             # All table schemas (25 cols Bronze, 21 cols Silver)
    ├── KNOWN_ISSUES.md                # Side effects & OSS Delta limitations
    └── MANUAL_COMMANDS.md             # All 9 job commands
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
   - Generate with `data_generator/generate_payment_data.py`
   - Upload CSVs to `gs://your-bucket/raw/20241202/`

### Quick Start

**1. Clone repository:**
```bash
git clone https://github.com/yourusername/delta-lake-gcp-implementation.git
cd delta-lake-gcp-implementation
```

**2. Generate test data:**
```bash
cd data_generator
python generate_payment_data.py
# Output: generated_data/day1.csv through day100.csv
```

**3. Deploy Spark jobs to GCS:**
```bash
gsutil cp bronze/jobs/*.py gs://your-bucket/airflow/jobs/
gsutil cp silver/jobs/*.py gs://your-bucket/airflow/jobs/
```

**4. Deploy DAGs to Composer:**
```bash
gsutil cp bronze/dags/*.py gs://your-composer-dags-bucket/dags/
gsutil cp silver/dags/*.py gs://your-composer-dags-bucket/dags/
```

**5. Trigger Bronze incremental load:**
- Go to Airflow UI → `bronze_incremental_load` → Trigger DAG
- Wait 8-10 minutes
- Verify: Query `bronze.transactions` in BigQuery

**6. Trigger Silver incremental load:**
- Airflow UI → `silver_incremental_load` → Trigger DAG
- Wait 30-60 seconds
- Verify: Query `silver.transactions` in BigQuery

### Manual Testing (No Airflow)

**Bronze:**
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

**Silver:**
```bash
# Validate (dedupe Bronze → staging)
gcloud dataproc jobs submit pyspark \
  gs://your-bucket/airflow/jobs/validate_silver.py \
  --cluster=your-cluster \
  --region=us-central1

# Load (MERGE staging → silver)
gcloud dataproc jobs submit pyspark \
  gs://your-bucket/airflow/jobs/load_silver.py \
  --cluster=your-cluster \
  --region=us-central1
```

**Full command reference:** See `/docs/MANUAL_COMMANDS.md` (all 9 jobs)

---

## 📊 Results & Metrics

### Bronze Layer (Blog 3a - Complete ✅)
- ✅ 1,462,039 records loaded (from 1.4M CSV rows)
- ✅ 1,411 records quarantined (0.67% - Tier 1 failures)
- ✅ ~900K records flagged (60.67% - Tier 2 violations, intentionally high for testing)
- ✅ ~19K records auto-fixed (1.33% - Tier 3 defaults)
- ✅ 150 status updates tracked (multiple versions per transaction)

### Silver Layer (Blog 3b - Complete ✅)
- ✅ 1,379,914 records deduplicated (from 1.46M Bronze records)
- ✅ 82,851 duplicates removed (5.66% - audit trail versions)
- ✅ 1,309 GDPR deletions tested (soft delete Bronze, hard delete Silver)
- ✅ 810 late arrivals handled (flagged in Bronze, processed in Silver)
- ✅ 0 duplicate transaction_ids in Silver (deduplication working)

**Performance:**
- Bronze incremental: 8-10 min (ephemeral cluster)
- Bronze full refresh: 8-10 min (1.4M records)
- Silver incremental: 30-60 sec (0-5K records)
- Silver full refresh: 69 sec (1.4M records)
- GDPR deletion: 35 sec (mark + propagate)
- Query latency: <2 sec (BigQuery external tables)

**Cost Optimization:**
- Ephemeral clusters: $0.40/hour (vs $292/month always-on)
- Lifecycle management: Auto-delete after 10 min idle
- **No data movement fees:** BigQuery queries Delta via BigLake (reads GCS directly)
- Monthly cost: ~$53 + usage (vs $345 with persistent cluster)

---

## 📝 Blog Posts

Follow the implementation journey on Medium:

- **Blog 1-2:** [BigQuery-Native Pipeline](link-to-blog) (baseline)
- **Blog 3:** [Delta Lake Setup on GCP](link-to-blog) (infrastructure)
- **Blog 3a:** [Bronze Layer Implementation](link-to-blog) ✅ **Complete**
- **Blog 3b:** [Silver Layer - Cleaning the Data](link-to-blog) ✅ **Complete**
- **Blog 3c:** Gold Layer - Star Schema (coming soon)
- **Blog 3d:** CDC & Advanced Patterns (coming soon)
- **Blog 3e:** Operations & Optimization (coming soon)

---

## 🎯 What's Next

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

## 📚 Documentation

**Getting Started:**
- `/README.md` (this file) - Project overview
- `/docs/MIGRATION_DOC_COMPLETE.md` - Complete migration context

**Layer-Specific:**
- `/bronze/README.md` - Bronze layer documentation
- `/silver/README.md` - Silver layer documentation
- `/silver/RUNBOOK.md` - Operations guide
- `/silver/HELPER.md` - Developer extension guide

**Technical Reference:**
- `/docs/SCHEMA_REGISTRY.md` - All table schemas
- `/docs/VALIDATION_RULES.md` - Data quality rules
- `/docs/KNOWN_ISSUES.md` - Side effects & limitations
- `/docs/MANUAL_COMMANDS.md` - All 9 job commands

---

## 🧪 Data Generator Configuration

**Location:** `/data_generator/generate_payment_data.py`

**Key Configuration:**
```python
# Output
ROWS_PER_DAY = 15000          # Transactions per day
DAYS_TO_GENERATE = range(1, 101)  # 100 days of history

# Data quality issues (for testing validation)
TIER1_ISSUES_PCT = 0.67       # NULL IDs → quarantine
TIER2_ISSUES_PCT = 60.67      # Bad data → flag (intentionally high)
TIER3_ISSUES_PCT = 1.33       # Missing → fix

# Silver layer test data
SOFT_DELETE_COUNT = 50        # GDPR deletions per day
LATE_ARRIVAL_COUNT = 50       # Late transactions per day
STATUS_UPDATE_COUNT = 100     # Status changes per day (Day 4+)
EXTRA_DUPLICATES_COUNT = 50   # Extra duplicates per day

# Time-aware incremental (Day 4+)
FRESH_DATA_PCT = 0.30         # 30% recent timestamps
HISTORICAL_DATA_PCT = 0.70    # 70% historical timestamps
```

**What It Generates:**
- 15,000 transactions per day × 100 days = 1.5M transactions
- 0.67% Tier 1 failures (quarantine)
- 60.67% Tier 2 violations (flagged, intentionally high for testing)
- 50 soft deletes per day (GDPR compliance testing)
- 50 late arrivals per day (late arrival handling)
- 100 status updates per day (audit trail testing)

**Usage:**
```bash
cd data_generator
python generate_payment_data.py
# Output: generated_data/day1.csv, day2.csv, ..., day100.csv
```

---

## 🤝 Contributing

- This project serves as a proof-of-concept and validation environment for building a robust, cost-efficient, and audit-compliant data architecture using Delta Lake on Google Cloud Platform (GCP).
- Your feedback is highly valued as it helps validate the architecture's assumptions and utility in real-world enterprise scenarios.

**Ways to Engage and Contribute:**
- ⭐ Star the repo
- 🐛 Report Bugs/Issues (see `/docs/KNOWN_ISSUES.md` first)
- 💡 Suggest improvements
- 📝 Share Your Delta Lake Journey

---

## 📄 License

MIT License - Feel free to use this for learning/portfolio projects
- Attribution Request: If you use this repository as a basis for your own public work, please link back to it.

---

## 👤 Author

**[Mohamed Kashifuddin]**  
Data Engineer | Delta Lake Enthusiast | Cloud Architecture

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohamedkashifuddin/)
[![Medium](https://img.shields.io/badge/Medium-12100E?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@mohamed_kashifuddin)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/mohamedkashifuddin)
[![Portfolio](https://img.shields.io/badge/Portfolio-FF7139?style=for-the-badge&logo=Firefox&logoColor=white)](https://mohamedkashifuddin.com)

📧 Email: mohamedkashifuddin24@gmail.com

---

## 🙏 Acknowledgments

- Delta Lake community for documentation
- Google Cloud for free tier credits
- Medium data engineering community for inspiration
- Open source contributors (Spark, Airflow, Delta Lake)

---

**Built with ❤️ using Delta Lake, Spark, and way too much coffee ☕**

**Project Status:** Bronze ✅ | Silver ✅ | Gold ⏳ | Operations ⏳