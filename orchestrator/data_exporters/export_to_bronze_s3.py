import os
import boto3
import json
from datetime import datetime, timezone
from mage_ai.streaming.sinks.base_python import BasePythonSink

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink

@streaming_sink
class SupermarketS3Sink(BasePythonSink):
    def init_client(self):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION'),
            endpoint_url=os.getenv('AWS_ENDPOINT_URL')
        )
        self.bucket = os.getenv('S3_BUCKET_BRONZE')

    def batch_write(self, messages):
        if not messages:
            return

        now = datetime.now(timezone.utc)
        partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        timestamp = now.strftime('%H%M%S_%f')
        key = f"sales_data/{partition}/{timestamp}.ndjson"

        body = "\n".join(json.dumps(msg) for msg in messages)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body
        )
        print(f"[INFO] Written {len(messages)} record(s) to s3://{self.bucket}/{key}")
