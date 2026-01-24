import os
import boto3
import json
import time
from mage_ai.streaming.sources.base_python import BasePythonSource

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

        shard_iterators = {}
        for shard in shards:
            shard_id = shard['ShardId']
            shard_iterators[shard_id] = self.kinesis.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'
            )['ShardIterator']

        while True:
            for shard_id, iterator in shard_iterators.items():
                if not iterator:
                    continue
                
                response = self.kinesis.get_records(ShardIterator=iterator, Limit=100)
                records = [json.loads(r['Data']) for r in response['Records']]
                
                if records:
                    handler(records)
                
                shard_iterators[shard_id] = response.get('NextShardIterator')
            
            time.sleep(1)