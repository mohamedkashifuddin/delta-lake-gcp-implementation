"""
=================================================================
FILE 1: shared/read_watermark.py
=================================================================
Read last processed watermark from job_control Delta table
"""

from pyspark.sql import SparkSession
from datetime import datetime
import sys

def read_watermark(spark, layer, job_name):
    """
    Read last successful watermark for a given layer and job
    
    Args:
        spark: SparkSession
        layer: 'bronze', 'silver', or 'gold'
        job_name: Name of the job (e.g., 'bronze_incremental_load')
    
    Returns:
        dict with watermark info or None if no previous run
    """
    
    table_name = f"{layer}_job_control"
    
    try:
        # Read from Delta table
        df = spark.read.format("delta").table(table_name)
        
        # Get last successful run for this job
        last_run = df.filter(
            (df.job_name == job_name) & 
            (df.status == 'SUCCESS')
        ).orderBy(df.completed_at.desc()).limit(1)
        
        if last_run.count() == 0:
            print(f"No previous successful run found for {job_name}")
            return None
        
        row = last_run.first()
        
        watermark = {
            'last_processed_timestamp': row.last_processed_timestamp,
            'last_batch_id': row.batch_id,
            'completed_at': row.completed_at,
            'records_written': row.records_written
        }
        
        print(f"Last watermark: {watermark['last_processed_timestamp']}")
        return watermark
        
    except Exception as e:
        print(f"Error reading watermark: {str(e)}")
        return None


if __name__ == "__main__":
    # For testing
    spark = SparkSession.builder \
        .appName("ReadWatermark") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()
    
    layer = sys.argv[1] if len(sys.argv) > 1 else "bronze"
    job_name = sys.argv[2] if len(sys.argv) > 2 else "test_job"
    
    result = read_watermark(spark, layer, job_name)
    print(f"Watermark result: {result}")
    
    spark.stop()

