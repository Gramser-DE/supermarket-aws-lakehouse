# NOTE — Architectural decision: 
#
# Mage AI runs each block in its own process and cannot serialize Spark DataFrames
# between blocks — passing a DataFrame produces an empty dict on the receiving end.
# S3 acts as the data bus between this transformer and the exporter:
#   - transform_silver  →  transformation + writes dims + fact to S3 Gold
#   - export_to_gold    →  reads from S3 Gold, upserts dims + fact_sales into PostgreSQL
#
# Surrogate keys are generated with md5() in Spark .

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, year, month, dayofmonth
import os


@transformer
def transform(silver_path, *args, **kwargs):

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

    # Derive Gold paths mirroring the Silver partition structure:
    # silver: s3a://supermarket-silver/sales_data_parquet/year=YYYY/month=MM/day=DD
    # gold:   s3a://supermarket-gold/fact_sales/year=YYYY/month=MM/day=DD
    gold_bucket    = os.getenv('S3_BUCKET_GOLD')
    date_partition = silver_path.split("/sales_data_parquet/")[1]   # "year=.../month=.../day=..."
    gold_fact_path = f"s3a://{gold_bucket}/fact_sales/{date_partition}"
    gold_dims_base = f"s3a://{gold_bucket}"

    print(f"[INFO] [READ] Reading Silver partition: {date_partition}...")
    df_silver = spark.read.parquet(silver_path)
    total_records = df_silver.count()
    print(f"[INFO] [READ] {total_records} records loaded from Silver.")

    # 1. Build dimension DataFrames with MD5 surrogate keys
    print("[INFO] [DIM] Building dimension tables with MD5 surrogate keys...")

    dim_product = df_silver \
        .select("product_id", "product_name", "product_category") \
        .distinct() \
        .withColumn("product_key", md5(col("product_id")))

    dim_store = df_silver \
        .select("store_id") \
        .distinct() \
        .withColumn("store_key", md5(col("store_id")))

    dim_payment_method = df_silver \
        .select("payment_method") \
        .distinct() \
        .withColumn("payment_method_key", md5(col("payment_method")))

    dim_client_type = df_silver \
        .select("client_type") \
        .distinct() \
        .withColumn("client_type_key", md5(col("client_type")))

    # 2. Build fact_sales with foreign keys computed inline via md5()
    print("[INFO] [TRANSFORM] Building fact_sales...")
    df_fact = df_silver \
        .withColumn("product_key",        md5(col("product_id"))) \
        .withColumn("store_key",          md5(col("store_id"))) \
        .withColumn("payment_method_key", md5(col("payment_method"))) \
        .withColumn("client_type_key",    md5(col("client_type"))) \
        .withColumn(
            "date_key",
            (
                year(col("sale_timestamp"))        * 10000
                + month(col("sale_timestamp"))     * 100
                + dayofmonth(col("sale_timestamp"))
            ).cast("int")
        ) \
        .select(
            col("transaction_id"),
            col("product_key"),
            col("store_key"),
            col("date_key"),
            col("payment_method_key"),
            col("client_type_key"),
            col("quantity"),
            col("unit_price"),
            col("total_price"),
            col("sale_timestamp"),
            col("processed_at"),
        )

    fact_count = df_fact.count()
    print(f"[INFO] [TRANSFORM] {fact_count} fact_sales record(s) built.")

    # 3. Write dimensions to S3 Gold (no date partition, overwritten each run)
    print("[INFO] [WRITE] Writing dimension tables to S3 Gold...")
    try:
        dim_product.write.mode("overwrite").parquet(f"{gold_dims_base}/dim_product")
        dim_store.write.mode("overwrite").parquet(f"{gold_dims_base}/dim_store")
        dim_payment_method.write.mode("overwrite").parquet(f"{gold_dims_base}/dim_payment_method")
        dim_client_type.write.mode("overwrite").parquet(f"{gold_dims_base}/dim_client_type")
        print("[INFO] [WRITE] Dimension tables written.")
    except Exception as e:
        print(f"[ERROR] [S3_WRITE_FAILURE] Dimensions: {e}")
        raise e

    # 4. Write fact_sales to S3 Gold (date-partitioned, overwrite for idempotency)
    print(f"[INFO] [WRITE] Writing fact_sales to: {gold_fact_path}")
    try:
        df_fact.write.mode("overwrite").parquet(gold_fact_path)
        print(f"[SUCCESS] [S3] fact_sales saved to S3 Gold: {gold_fact_path}")
    except Exception as e:
        print(f"[ERROR] [S3_WRITE_FAILURE] fact_sales: {e}")
        raise e

    # 5. Return serializable dict — Mage AI cannot pass DataFrames between blocks
    return {"dims_base": gold_dims_base, "fact_path": gold_fact_path}
