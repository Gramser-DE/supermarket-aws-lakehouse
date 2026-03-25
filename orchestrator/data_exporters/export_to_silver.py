import os
from pyspark.sql import SparkSession
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(silver_path, *_args, **_kwargs):

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(os.getenv('MAGE_PROJECT_NAME', 'SupermarketLakehouse')) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540,org.postgresql:postgresql:42.7.1") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv('AWS_ENDPOINT_URL')) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    df_valid = spark.read.parquet(silver_path)
    valid_count = df_valid.count()
    print(f"[INFO] [START] Exporter: {valid_count} record(s) read from {silver_path}")

    # Load valid records into PostgreSQL
    pg_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    pg_props = {
        "user":     os.getenv('POSTGRES_USER'),
        "password": os.getenv('POSTGRES_PASSWORD'),
        "driver":   "org.postgresql.Driver",
    }

    table_staging = "public.stg_sales_silver"
    table_final   = "public.sales_silver"

    print(f"[INFO] [WRITE] Writing {valid_count} record(s) to staging table ({table_staging})...")
    df_valid.write.jdbc(url=pg_url, table=table_staging, mode="overwrite", properties=pg_props)

    upsert_query = f"""
    INSERT INTO {table_final} (
        transaction_id, product_id, product_name, product_category,
        quantity, unit_price, total_price,
        store_id, payment_method, client_type,
        sale_timestamp, processed_at
    )
    SELECT
        transaction_id, product_id, product_name, product_category,
        quantity, unit_price, total_price,
        store_id, payment_method, client_type,
        sale_timestamp, processed_at
    FROM {table_staging}
    ON CONFLICT (transaction_id, product_id) DO UPDATE SET
        product_name     = EXCLUDED.product_name,
        product_category = EXCLUDED.product_category,
        quantity         = EXCLUDED.quantity,
        unit_price       = EXCLUDED.unit_price,
        total_price      = EXCLUDED.total_price,
        sale_timestamp   = EXCLUDED.sale_timestamp,
        processed_at     = EXCLUDED.processed_at;
    """

    print(f"[INFO] [MERGE] Executing upsert from {table_staging} to {table_final}...")
    try:
        connection = df_valid.sparkSession._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            pg_url, pg_props["user"], pg_props["password"]
        )
        statement = connection.createStatement()
        statement.execute(upsert_query)
        statement.execute(f"DROP TABLE IF EXISTS {table_staging}")
        statement.close()
        connection.close()
        print(f"[SUCCESS] [EXPORT] Upsert completed. No duplicates created.")
    except Exception as e:
        print(f"[ERROR] [DB_TRANSACTION] Failed to execute upsert: {e}")
        raise e
