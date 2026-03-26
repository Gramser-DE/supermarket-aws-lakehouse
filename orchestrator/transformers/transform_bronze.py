# NOTE — Architectural decision: 
#
# Ideally, all I/O (S3 + PostgreSQL) would live in the data exporter, keeping
# this transformer as a pure transformation step. However, Mage AI runs each
# block in its own process and cannot serialize Spark DataFrames between blocks
# — passing a DataFrame produces an empty dict on the receiving end.
#
# The only serializable types between blocks are primitives (strings, dicts, etc.).
# Using toPandas() as an alternative was ruled out because it collects all data
# onto the driver, making the pipeline non-scalable for large datasets.
#
# Therefore, S3 acts as the data bus between this transformer and the exporter:
#   - transform_bronze  →  validates + writes to S3 Silver (parquet) + S3 Quarantine
#   - export_to_silver  →  reads from S3 Silver, upserts into PostgreSQL


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_timestamp,
    trim, upper, initcap,
    when, lit,
    abs as spark_abs, round as spark_round,
)
import os


PRICE_TOLERANCE = 0.01


@transformer
def transform(bronze_path, *args, **kwargs):

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

    # Derive silver paths mirroring the bronze partition structure:
    # bronze:     s3a://supermarket-bronze/sales_data/year=YYYY/month=MM/day=DD
    # silver:     s3a://supermarket-silver/sales_data_parquet/year=YYYY/month=MM/day=DD
    # quarantine: s3a://supermarket-silver/quarantine/year=YYYY/month=MM/day=DD
    silver_bucket   = os.getenv('S3_BUCKET_SILVER')
    date_partition  = bronze_path.split("/sales_data/")[1]  # "year=.../month=.../day=..."
    silver_path     = f"s3a://{silver_bucket}/sales_data_parquet/{date_partition}"
    quarantine_path = f"s3a://{silver_bucket}/quarantine/{date_partition}"

    print(f"[INFO] [START] Transformer: Reading partition {date_partition}...")
    df = spark.read.json(bronze_path)
    total_records = df.count()
    print(f"[INFO] [READ] {total_records} records loaded from Bronze.")

    # 1. Select, cast and standardize text
    df_typed = df.select(
        trim(col("transaction_id")).alias("transaction_id"),
        trim(col("product_id")).alias("product_id"),
        initcap(trim(col("product_name"))).alias("product_name"),
        upper(trim(col("category"))).alias("product_category"),
        col("quantity").cast("int"),
        col("unit_price").cast("double"),
        col("total_price").cast("double"),
        upper(trim(col("store_id"))).alias("store_id"),
        upper(trim(col("payment_method"))).alias("payment_method"),
        upper(trim(col("client_type"))).alias("client_type"),
        to_timestamp(col("timestamp")).alias("sale_timestamp"),
    ).withColumn("processed_at", current_timestamp())

    # 2. Tag each record with the first failing validation rule (null = valid)
    df_validated = df_typed.withColumn(
        "quarantine_reason",
        when(col("transaction_id").isNull(), lit("null:transaction_id"))
        .when(col("product_id").isNull(),     lit("null:product_id"))
        .when(col("store_id").isNull(),       lit("null:store_id"))
        .when(col("sale_timestamp").isNull(), lit("null:sale_timestamp"))
        .when(col("quantity").isNull()    | (col("quantity")    <= 0), lit("invalid:quantity"))
        .when(col("unit_price").isNull()  | (col("unit_price")  <= 0), lit("invalid:unit_price"))
        .when(col("total_price").isNull() | (col("total_price") <= 0), lit("invalid:total_price"))
        .when(
            spark_abs(col("total_price") - spark_round(col("unit_price") * col("quantity"), 2)) > PRICE_TOLERANCE,
            lit("inconsistent:total_vs_unit_x_qty")
        )
        .otherwise(lit(None))
    )

    # 3. Split valid vs quarantine
    df_valid      = df_validated.filter(col("quarantine_reason").isNull()).drop("quarantine_reason")
    df_quarantine = df_validated.filter(col("quarantine_reason").isNotNull())

    valid_count      = df_valid.count()
    quarantine_count = df_quarantine.count()
    print(f"[INFO] [VALIDATE] Valid: {valid_count} | Quarantined: {quarantine_count} / {total_records}")

    # 4. Write quarantined records (overwrite this day's partition, partitioned by reason for auditing)
    if quarantine_count > 0:
        print(f"[WARN] [QUARANTINE] Writing {quarantine_count} record(s) to: {quarantine_path}")
        try:
            df_quarantine.write \
                .mode("overwrite") \
                .partitionBy("quarantine_reason") \
                .parquet(quarantine_path)
            print(f"[WARN] [QUARANTINE] Done.")
        except Exception as e:
            print(f"[ERROR] [QUARANTINE_WRITE] Details: {e}")
            raise e

    # 5. Write valid records to S3 Silver
    print(f"[INFO] [WRITE] Writing {valid_count} valid record(s) to: {silver_path}")
    try:
        df_valid.write \
            .mode("overwrite") \
            .parquet(silver_path)
        print(f"[SUCCESS] [S3] Data saved to S3 Silver: {silver_path}")
        return silver_path
    except Exception as e:
        print(f"[ERROR] [S3_WRITE_FAILURE] Details: {e}")
        raise e
