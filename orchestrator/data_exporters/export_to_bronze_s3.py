import os
import boto3
import json
from datetime import datetime
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
        for msg in messages:
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            filename = f"sales_data/{timestamp}.json"
            self.s3.put_object(
                Bucket=self.bucket,
                Key=filename,
                Body=json.dumps(msg)
            )