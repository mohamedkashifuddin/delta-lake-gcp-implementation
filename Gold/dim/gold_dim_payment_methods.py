"""
Gold Dim Payment Methods Generator - One-Time Load
Extracts distinct payment methods from Silver, adds surrogate keys
Uses pure Spark SQL (no DataFrame API)
"""

from pyspark.sql import SparkSession
from datetime import datetime
import json

# Track execution time
start_time = datetime.now()

# ==================== SPARK SESSION ====================

spark = SparkSession.builder \
    .appName("Gold_Dim_Payment_Methods_Generator") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

print("✅ Spark session created")

# ==================== CONFIGURATION ====================

SOURCE_TABLE = "silver.transactions"
TARGET_TABLE = "gold.dim_payment_method"  # Fixed: singular, not plural

print(f"\n📊 Source: {SOURCE_TABLE}")
print(f"📊 Target: {TARGET_TABLE}")

# ==================== EXTRACT & ENRICH ====================

print("\n🔄 Extracting distinct payment methods from Silver...")

spark.sql("""
CREATE OR REPLACE TEMP VIEW payment_methods_enriched AS
SELECT
    ROW_NUMBER() OVER (ORDER BY payment_method) AS payment_method_key,
    payment_method,
    CASE 
        WHEN payment_method = 'UPI' THEN 'Unified Payments Interface'
        WHEN payment_method = 'Credit Card' THEN 'Credit Card Payment'
        WHEN payment_method = 'Debit Card' THEN 'Debit Card Payment'
        WHEN payment_method = 'Wallet Balance' THEN 'Digital Wallet Payment'
        WHEN payment_method = 'Bank Transfer' THEN 'Direct Bank Transfer'
        ELSE 'Other Payment Method'
    END AS description,
    CURRENT_TIMESTAMP() AS loaded_at,
    'payment_gateway' AS source_system
FROM (
    SELECT DISTINCT payment_method
    FROM silver.transactions
    WHERE payment_method IS NOT NULL
)
ORDER BY payment_method
""")

method_count = spark.sql("SELECT COUNT(*) as cnt FROM payment_methods_enriched").first()['cnt']
print(f"✅ Found {method_count} distinct payment methods")

# ==================== INSERT INTO TARGET ====================

print(f"\n🔄 Inserting into {TARGET_TABLE}...")

spark.sql(f"""
INSERT INTO {TARGET_TABLE}
SELECT 
    payment_method_key,
    payment_method,
    description,
    loaded_at,
    source_system
FROM payment_methods_enriched
""")

print(f"✅ {TARGET_TABLE} populated successfully")

# ==================== VALIDATION ====================

print("\n🔍 Validating data...")

result_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").first()['cnt']
print(f"✅ Total rows: {result_count}")

print("\n📊 Dimension data:")
spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY payment_method_key").show(truncate=False)

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
print("🎉 DIM_PAYMENT_METHODS GENERATION COMPLETE")
print("="*70)
print(f"✅ Table: {TARGET_TABLE}")
print(f"✅ Rows: {result_count}")
print(f"✅ Surrogate keys: 1 to {result_count}")
print(f"✅ Ready for fact table joins")
print("="*70)
print(f"\nRESULT_JSON: {json.dumps(result)}")

spark.stop()