import os
from datetime import date
from pyspark.sql import SparkSession
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):

    silver_bucket = os.getenv('S3_BUCKET_BRONZE').replace('bronze', 'silver')

    try:
        spark = SparkSession.getActiveSession()
        if spark:
            print("[INFO] Stopping existing Spark session...")
            spark.stop()
    except:
        pass

    print("[INFO] [START] Initializing Spark session...")

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

    print(f"[SUCCESS] Spark {spark.version} session established.")

    try:
        today = date.today()
        date_partition = f"year={today.year}/month={today.month:02d}/day={today.day:02d}"
        silver_path = f"s3a://{silver_bucket}/sales_data_parquet/{date_partition}"

        print(f"[INFO] [READ] Reading partition: {silver_path}")
        df = spark.read.parquet(silver_path)
        print(f"[SUCCESS] [LOAD] Total records loaded: {df.count()}")
        return silver_path
    except Exception as e:
        print(f"[ERROR] [S3_READ_FAILURE] Details: {e}")
        raise e
