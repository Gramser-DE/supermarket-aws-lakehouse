import os
from pyspark.sql import SparkSession
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(silver_path, *args, **kwargs):

    print(f"[INFO] [START] Data Exporter: Fetching refined data from {silver_path} for PostgreSQL ingestion")
    
    spark = SparkSession.builder.getOrCreate()  

    df_new_data = spark.read.parquet(silver_path)
    
    pg_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    pg_props = {
        "user": os.getenv('POSTGRES_USER'),
        "password": os.getenv('POSTGRES_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    table_staging = "public.stg_sales_silver"
    table_final = "public.sales_silver"

    print(f"⏳ [INFO] [WRITE] Writing to Staging Table ({table_staging})...")
    df_new_data.write.jdbc(url=pg_url, table=table_staging, mode="overwrite", properties=pg_props)


    print(f"🔄 [INFO] [MERGE] Executing Upsert from {table_staging} to {table_final}...")

    upsert_query = f"""
    INSERT INTO {table_final} (
        product_category, product_name, quantity, unit_price, total_price, 
        store_id, source_system, transaction_id, sale_timestamp, ingested_at, processed_at
    )
    SELECT 
        product_category, product_name, quantity, unit_price, total_price, 
        store_id, source_system, transaction_id, sale_timestamp, ingested_at, processed_at
    FROM {table_staging}
    ON CONFLICT (transaction_id) DO UPDATE SET
        product_category = EXCLUDED.product_category,
        product_name = EXCLUDED.product_name, 
        quantity = EXCLUDED.quantity,         
        unit_price = EXCLUDED.unit_price,
        total_price = EXCLUDED.total_price,
        sale_timestamp = EXCLUDED.sale_timestamp, 
        processed_at = EXCLUDED.processed_at;
    """
    
    try:
        connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            pg_url, pg_props["user"], pg_props["password"]
        )
        statement = connection.createStatement()
        statement.execute(upsert_query)
        statement.execute(f"DROP TABLE IF EXISTS {table_staging}")
        statement.close()
        connection.close()
        print(f"✅ [SUCCESS] [EXPORT] Idempotent load completed. No duplicates created.")
        
    except Exception as e:
        print(f"❌ [ERROR] [DB_TRANSACTION] Failed to execute Upsert: {e}")
        raise e