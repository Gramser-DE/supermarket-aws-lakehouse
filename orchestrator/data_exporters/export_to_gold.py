import os
from pyspark.sql import SparkSession
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(paths, *_args, **_kwargs):

    dims_base = paths["dims_base"]
    fact_path = paths["fact_path"]

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

    pg_url   = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    pg_user  = os.getenv('POSTGRES_USER')
    pg_pass  = os.getenv('POSTGRES_PASSWORD')
    pg_props = {"user": pg_user, "password": pg_pass, "driver": "org.postgresql.Driver"}

    dims = [
        {
            "s3_path": f"{dims_base}/dim_product",
            "staging": "public.stg_dim_product",
            "target":  "public.dim_product",
            "upsert":  """
                INSERT INTO public.dim_product (product_key, product_id, product_name, product_category)
                SELECT product_key, product_id, product_name, product_category FROM public.stg_dim_product
                ON CONFLICT (product_key) DO UPDATE SET
                    product_name     = EXCLUDED.product_name,
                    product_category = EXCLUDED.product_category
            """,
        },
        {
            "s3_path": f"{dims_base}/dim_store",
            "staging": "public.stg_dim_store",
            "target":  "public.dim_store",
            "upsert":  """
                INSERT INTO public.dim_store (store_key, store_id)
                SELECT store_key, store_id FROM public.stg_dim_store
                ON CONFLICT (store_key) DO NOTHING
            """,
        },
        {
            "s3_path": f"{dims_base}/dim_payment_method",
            "staging": "public.stg_dim_payment_method",
            "target":  "public.dim_payment_method",
            "upsert":  """
                INSERT INTO public.dim_payment_method (payment_method_key, payment_method)
                SELECT payment_method_key, payment_method FROM public.stg_dim_payment_method
                ON CONFLICT (payment_method_key) DO NOTHING
            """,
        },
        {
            "s3_path": f"{dims_base}/dim_client_type",
            "staging": "public.stg_dim_client_type",
            "target":  "public.dim_client_type",
            "upsert":  """
                INSERT INTO public.dim_client_type (client_type_key, client_type)
                SELECT client_type_key, client_type FROM public.stg_dim_client_type
                ON CONFLICT (client_type_key) DO NOTHING
            """,
        },
    ]

    connection = None
    statement  = None

    for dim in dims:
        print(f"[INFO] [DIM] Upserting {dim['target']}...")
        df_dim = spark.read.parquet(dim["s3_path"])
        df_dim.write.jdbc(url=pg_url, table=dim["staging"], mode="overwrite", properties=pg_props)
        if connection is None:
            connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(pg_url, pg_user, pg_pass)
            statement  = connection.createStatement()
        statement.execute(dim["upsert"])
        statement.execute(f"DROP TABLE IF EXISTS {dim['staging']}")
        print(f"[INFO] [DIM] {dim['target']}: {df_dim.count()} row(s) upserted.")

    table_staging = "public.stg_fact_sales"
    table_final   = "public.fact_sales"

    print(f"[INFO] [FACT] Reading fact_sales from: {fact_path}")
    df_fact = spark.read.parquet(fact_path)
    fact_count = df_fact.count()
    print(f"[INFO] [FACT] {fact_count} record(s) read. Writing to staging...")

    df_fact.write.jdbc(url=pg_url, table=table_staging, mode="overwrite", properties=pg_props)

    upsert_query = f"""
        INSERT INTO {table_final} (
            transaction_id, product_key, store_key, date_key,
            payment_method_key, client_type_key,
            quantity, unit_price, total_price,
            sale_timestamp, processed_at
        )
        SELECT
            transaction_id, product_key, store_key, date_key,
            payment_method_key, client_type_key,
            quantity, unit_price, total_price,
            sale_timestamp, processed_at
        FROM {table_staging}
        ON CONFLICT (transaction_id, product_key) DO UPDATE SET
            store_key          = EXCLUDED.store_key,
            date_key           = EXCLUDED.date_key,
            payment_method_key = EXCLUDED.payment_method_key,
            client_type_key    = EXCLUDED.client_type_key,
            quantity           = EXCLUDED.quantity,
            unit_price         = EXCLUDED.unit_price,
            total_price        = EXCLUDED.total_price,
            sale_timestamp     = EXCLUDED.sale_timestamp,
            processed_at       = EXCLUDED.processed_at
    """

    print(f"[INFO] [FACT] Executing upsert into {table_final}...")
    try:
        statement.execute(upsert_query)
        statement.execute(f"DROP TABLE IF EXISTS {table_staging}")
        statement.close()
        connection.close()
        print(f"[SUCCESS] [EXPORT] {fact_count} fact_sales record(s) upserted. No duplicates created.")
    except Exception as e:
        print(f"[ERROR] [DB_TRANSACTION] Failed to execute upsert: {e}")
        raise e
