import os
import boto3
import json
import time
from mage_ai.streaming.sources.base_python import BasePythonSource
from concurrent.futures import ThreadPoolExecutor, as_completed

if 'streaming_source' not in globals():
    from mage_ai.data_preparation.decorators import streaming_source

@streaming_source
class SupermarketSource(BasePythonSource):
    def init_client(self):
        self.kinesis = boto3.client(
            'kinesis',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION'),
            endpoint_url=os.getenv('AWS_ENDPOINT_URL')
        )
        self.stream_name = os.getenv('KINESIS_STREAM_NAME')

    def batch_read(self, handler):
        stream_info = self.kinesis.describe_stream(StreamName=self.stream_name)
        shards = stream_info['StreamDescription']['Shards']
        print(f"[INFO] Found {len(shards)} shard(s): {[s['ShardId'] for s in shards]}")

        with ThreadPoolExecutor(max_workers=len(shards)) as executor:
            futures = {
                executor.submit(self._read_shard_loop, shard['ShardId'], handler): shard['ShardId'] for shard in shards
            }
            for future in as_completed(futures):
                shard_id = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[ERROR] Shard {shard_id} failed: {e}")

    def _read_shard_loop(self, shard_id, handler):
        iterator = self.kinesis.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )['ShardIterator']

        print(f"[INFO] Starting read loop for shard {shard_id}")

        while True:
            if not iterator:
                print(f"[WARN] Shard {shard_id}: iterator expired, stopping.")
                break

            response = self.kinesis.get_records(ShardIterator=iterator, Limit=100)
            records = [json.loads(r['Data']) for r in response['Records']]

            if records:
                print(f"[INFO] Shard {shard_id}: {len(records)} record(s) received.")
                handler(records)

            iterator = response.get('NextShardIterator')
            time.sleep(1)
