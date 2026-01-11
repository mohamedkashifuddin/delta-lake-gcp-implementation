"""
Gold Dim Date Generator - One-Time Load
Generates 6 years of date dimension (2023-2028)
Uses pure Spark SQL (no DataFrame API)
"""

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import json

# Track execution time
start_time = datetime.now()

# ==================== SPARK SESSION ====================

spark = SparkSession.builder \
    .appName("Gold_Dim_Date_Generator") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

print("✅ Spark session created")

# ==================== CONFIGURATION ====================

START_DATE = "2023-01-01"
END_DATE = "2028-12-31"
TABLE_NAME = "gold.dim_date"

print(f"\n📅 Date Range: {START_DATE} to {END_DATE}")

# ==================== GENERATE DATE LIST ====================

def generate_dates(start, end):
    """Generate list of dates as SQL values"""
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    
    dates = []
    current = start_dt
    
    while current <= end_dt:
        dates.append(f"(DATE'{current.strftime('%Y-%m-%d')}')")
        current += timedelta(days=1)
    
    return ",\n".join(dates)

print("🔄 Generating date list...")
date_values = generate_dates(START_DATE, END_DATE)
date_count = date_values.count("DATE")
print(f"✅ Generated {date_count:,} dates")

# ==================== CREATE TEMP VIEW WITH DATES ====================

print("🔄 Creating temporary view with dates...")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW date_seed AS
SELECT date_col
FROM VALUES
{date_values}
AS t(date_col)
""")

print("✅ Temporary view created")

# ==================== INSERT INTO DIM_DATE ====================

print(f"\n🔄 Inserting into {TABLE_NAME}...")

spark.sql(f"""
INSERT INTO {TABLE_NAME}
SELECT
    CAST(DATE_FORMAT(date_col, 'yyyyMMdd') AS BIGINT) AS date_key,
    date_col AS full_date,
    DAY(date_col) AS day_of_month,
    DATE_FORMAT(date_col, 'EEEE') AS day_name,
    MONTH(date_col) AS month_number,
    DATE_FORMAT(date_col, 'MMMM') AS month_name,
    YEAR(date_col) AS year,
    QUARTER(date_col) AS quarter,
    DAYOFWEEK(date_col) AS day_of_week,
    DAYOFYEAR(date_col) AS day_of_year,
    CASE WHEN DAYOFWEEK(date_col) IN (1, 7) THEN true ELSE false END AS is_weekend
FROM date_seed
""")

print(f"✅ {TABLE_NAME} populated successfully")

# ==================== VALIDATION ====================

print("\n🔍 Validating data...")

# Check row count
result_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME}").first()['cnt']
print(f"✅ Total rows: {result_count:,}")

# Check date range
date_range = spark.sql(f"""
    SELECT MIN(full_date) as min_date, MAX(full_date) as max_date 
    FROM {TABLE_NAME}
""").first()
print(f"✅ Date range: {date_range['min_date']} to {date_range['max_date']}")

# Sample weekends
print("\n📊 Sample weekend dates:")
spark.sql(f"""
    SELECT date_key, full_date, day_name 
    FROM {TABLE_NAME} 
    WHERE is_weekend = true 
    LIMIT 5
""").show()

# Year distribution
print("📊 Year distribution:")
spark.sql(f"""
    SELECT year, COUNT(*) as days
    FROM {TABLE_NAME}
    GROUP BY year
    ORDER BY year
""").show()

# ==================== JSON OUTPUT ====================

end_time = datetime.now()
execution_seconds = (end_time - start_time).total_seconds()

result = {
    "status": "SUCCESS",
    "table": TABLE_NAME,
    "rows_written": result_count,
    "date_range": {
        "start": START_DATE,
        "end": END_DATE
    },
    "execution_time_seconds": round(execution_seconds, 2)
}

print("\n" + "="*70)
print("🎉 DIM_DATE GENERATION COMPLETE")
print("="*70)
print(f"✅ Table: {TABLE_NAME}")
print(f"✅ Rows: {result_count:,}")
print(f"✅ Range: {START_DATE} to {END_DATE}")
print(f"✅ Ready for fact table joins")
print("="*70)
print(f"\nRESULT_JSON: {json.dumps(result)}")

spark.stop()