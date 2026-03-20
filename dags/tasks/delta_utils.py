"""
Delta Lake utility functions for safe concurrent writes
"""
import time
from delta.tables import DeltaTable


def safe_delta_write(spark, df, path, mode="overwrite", max_retries=3):
    """
    Safely write to a Delta table with concurrency handling.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to write
        path: Delta table path
        mode: Write mode (default: "overwrite")
        max_retries: Maximum number of retry attempts (default: 3)
    
    Returns:
        bool: True if successful
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            writer = (
                df.write
                .format("delta")
                .mode(mode)
            )
            
            # Add options based on mode
            if mode == "overwrite":
                writer = writer.option("overwriteSchema", "true")
                writer = writer.option("replaceWhere", "1=1")
            else:
                writer = writer.option("mergeSchema", "true")
            
            writer.save(path)
            
            print(f"✅ Successfully wrote to Delta table at {path}")
            return True
            
        except Exception as e:
            error_msg = str(e)
            
            # Check for concurrency-related errors
            if ("DELTA_PROTOCOL_CHANGED" in error_msg or 
                "concurrent" in error_msg.lower() or
                "ConcurrentAppendException" in error_msg):
                
                retry_count += 1
                
                if retry_count >= max_retries:
                    raise Exception(f"Failed after {max_retries} retries due to concurrent writes: {error_msg}")
                
                # Exponential backoff
                wait_time = retry_count * 2
                print(f"⚠️  Concurrent write detected (attempt {retry_count}/{max_retries}). Retrying in {wait_time}s...")
                time.sleep(wait_time)
                
            else:
                # Non-retryable error
                raise
    
    return False


def delta_table_exists(spark, path):
    """
    Check if a Delta table exists at the given path.
    
    Args:
        spark: SparkSession instance
        path: Path to check
    
    Returns:
        bool: True if table exists
    """
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def create_empty_delta_table(spark, df, path):
    """
    Create an empty Delta table with the schema from df.
    
    Args:
        spark: SparkSession instance
        df: DataFrame with desired schema
        path: Path to create table
    """
    try:
        (
            df.limit(0)
            .write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )
        print(f"✅ Empty Delta table created at {path}")
        return True
    except Exception as e:
        print(f"⚠️  Error creating empty table: {e}")
        return False
