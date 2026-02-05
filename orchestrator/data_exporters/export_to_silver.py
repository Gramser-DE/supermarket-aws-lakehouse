import os
from pyspark.sql import SparkSession
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(silver_path, *args, **kwargs):

    print(f"[INFO] [START] Data Exporter: Fetching refined data from {silver_path} for PostgreSQL ingestion")
    
    spark = SparkSession.builder.getOrCreate()  

    df_final = spark.read.parquet(silver_path)
    
    pg_user = os.getenv('POSTGRES_USER')
    pg_pass = os.getenv('POSTGRES_PASSWORD')
    pg_host = os.getenv('POSTGRES_HOST') 
    pg_port = os.getenv('POSTGRES_PORT')
    pg_db = os.getenv('POSTGRES_DB')
    
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    
    print(f"[INFO] [CONNECT] Establishing JDBC connection to {pg_host}:{pg_port}/{pg_db} (User: {pg_user})")
    
    df_final.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "public.sales_silver") \
        .option("user", pg_user) \
        .option("password", pg_pass) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
        
    print("✅ [SUCCESS] [EXPORT] PostgreSQL data load completed successfully.")