from typing import Dict, List
from datetime import datetime
import json

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(messages: List[Dict], *args, **kwargs):

    transformed_messages = []

    for msg in messages:

        if isinstance(msg, (str, bytes)):
            record = json.loads(msg)
        else:
            record = msg

        record['ingested_at'] = datetime.utcnow().isoformat()
        record['source_system'] = 'kinesis_producer'

        transformed_messages.append(record)

    return transformed_messages
