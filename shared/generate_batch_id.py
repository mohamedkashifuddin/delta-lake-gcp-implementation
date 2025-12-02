"""
=================================================================
FILE 3: shared/generate_batch_id.py
=================================================================
Generate unique batch ID (UUID) for job execution tracking
"""
import uuid
from datetime import datetime

def generate_batch_id(job_name=None, timestamp=None):
    """
    Generate unique batch ID
    
    Args:
        job_name: Optional job name to prefix
        timestamp: Optional timestamp (defaults to now)
    
    Returns:
        str: Unique batch ID (format: job_name_YYYYMMDD_HHMMSS_uuid)
    """
    
    if timestamp is None:
        timestamp = datetime.now()
    
    date_str = timestamp.strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    
    if job_name:
        batch_id = f"{job_name}_{date_str}_{unique_id}"
    else:
        batch_id = f"{date_str}_{unique_id}"
    
    return batch_id


if __name__ == "__main__":
    # Test
    batch_id = generate_batch_id("bronze_incremental_load")
    print(f"Generated batch ID: {batch_id}")
    
    # Example output: bronze_incremental_load_20241202_143052_a3b4c5d6
