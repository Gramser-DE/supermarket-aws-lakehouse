if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

from pyspark.sql.functions import col, current_timestamp, to_timestamp
from pyspark.sql import SparkSession
import os


@transformer
def transform(bronze_path, *args, **kwargs):
    
    print(f"[INFO] [START] Transformer: Processing data from: {bronze_path}")
    
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("multiline", "true").json(bronze_path)
    
    print(f"[INFO] [READ] Read {df.count()} raw records from S3 Bronze.")

    df_silver = df.select(
        col("transaction_id"),
        col("product_name"),
        col("category").alias("product_category"),
        col("quantity").cast("int"),
        col("unit_price"), 
        col("total_price").cast("double"),
        col("store_id"),
        col("source_system"),
        to_timestamp(col("timestamp")).alias("sale_timestamp"),
        to_timestamp(col("ingested_at")).alias("ingested_at")
    ).withColumn("processed_at", current_timestamp())

    silver_bucket = os.getenv('S3_BUCKET_BRONZE').replace('bronze', 'silver')
    silver_path = f"s3a://{silver_bucket}/sales_data_parquet"
    
    print(f"[INFO] [WRITE] Exporting refined data to: {silver_path}")
    
    try:
        df_silver.write \
        .mode("overwrite") \
        .parquet(silver_path)

        print(f"[SUCCESS] [SAVE] Cleaned data successfully saved to S3 Silver (Parquet): {silver_path}")
        
        return silver_path
    except Exception as e:
        print(f"[ERROR] [S3_READ_FAILURE] Details: {e}")
        raise e
    