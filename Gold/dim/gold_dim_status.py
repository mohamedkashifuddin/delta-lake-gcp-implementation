"""
Gold Dim Status Generator - One-Time Load
Extracts distinct transaction statuses from Silver, adds surrogate keys
Uses pure Spark SQL (no DataFrame API)
"""

from pyspark.sql import SparkSession
from datetime import datetime
import json

# Track execution time
start_time = datetime.now()

# ==================== SPARK SESSION ====================

spark = SparkSession.builder \
    .appName("Gold_Dim_Status_Generator") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

print("✅ Spark session created")

# ==================== CONFIGURATION ====================

SOURCE_TABLE = "silver.transactions"
TARGET_TABLE = "gold.dim_status"  # Fixed: matches your table name

print(f"\n📊 Source: {SOURCE_TABLE}")
print(f"📊 Target: {TARGET_TABLE}")

# ==================== EXTRACT & ENRICH ====================

print("\n🔄 Extracting distinct transaction statuses from Silver...")

spark.sql("""
CREATE OR REPLACE TEMP VIEW status_enriched AS
SELECT
    ROW_NUMBER() OVER (ORDER BY transaction_status) AS status_key,
    transaction_status,
    CASE 
        WHEN transaction_status = 'Successful' THEN 'COMPLETED'
        WHEN transaction_status = 'Pending' THEN 'IN_PROGRESS'
        WHEN transaction_status = 'Failed' THEN 'FAILED'
        ELSE 'UNKNOWN'
    END AS status_category,
    CASE 
        WHEN transaction_status = 'Successful' THEN true
        ELSE false
    END AS is_successful,
    CURRENT_TIMESTAMP() AS loaded_at,
    'payment_gateway' AS source_system
FROM (
    SELECT DISTINCT transaction_status
    FROM silver.transactions
    WHERE transaction_status IS NOT NULL
)
ORDER BY transaction_status
""")

status_count = spark.sql("SELECT COUNT(*) as cnt FROM status_enriched").first()['cnt']
print(f"✅ Found {status_count} distinct transaction statuses")

# ==================== INSERT INTO TARGET ====================

print(f"\n🔄 Inserting into {TARGET_TABLE}...")

spark.sql(f"""
INSERT INTO {TARGET_TABLE}
SELECT 
    status_key,
    transaction_status,
    status_category,
    is_successful,
    loaded_at,
    source_system
FROM status_enriched
""")

print(f"✅ {TARGET_TABLE} populated successfully")

# ==================== VALIDATION ====================

print("\n🔍 Validating data...")

result_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").first()['cnt']
print(f"✅ Total rows: {result_count}")

print("\n📊 Dimension data:")
spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY status_key").show(truncate=False)

# ==================== JSON OUTPUT ====================

end_time = datetime.now()
execution_seconds = (end_time - start_time).total_seconds()

result = {
    "status": "SUCCESS",
    "table": TARGET_TABLE,
    "rows_written": result_count,
    "source_table": SOURCE_TABLE,
    "execution_time_seconds": round(execution_seconds, 2)
}

print("\n" + "="*70)
print("🎉 DIM_STATUS GENERATION COMPLETE")
print("="*70)
print(f"✅ Table: {TARGET_TABLE}")
print(f"✅ Rows: {result_count}")
print(f"✅ Surrogate keys: 1 to {result_count}")
print(f"✅ Ready for fact table joins")
print("="*70)
print(f"\nRESULT_JSON: {json.dumps(result)}")

spark.stop()