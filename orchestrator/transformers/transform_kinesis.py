from typing import Dict, List
from datetime import datetime, timezone
import json

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

REQUIRED_FIELDS = {
    'transaction_id', 'product_id', 'product_name', 'category',
    'quantity', 'unit_price', 'total_price', 'timestamp',
    'store_id', 'payment_method', 'client_type'
}

@transformer
def transform(messages: List[Dict], *args, **kwargs):
    transformed = []
    skipped = 0

    for msg in messages:
        if isinstance(msg, (str, bytes)):
            record = json.loads(msg)
        else:
            record = msg

        missing = REQUIRED_FIELDS - record.keys()
        if missing:
            print(f"[WARN] Skipping record missing fields: {missing}")
            skipped += 1
            continue

        record['ingested_at'] = datetime.now(timezone.utc).isoformat()
        record['source_system'] = 'kinesis_producer'
        transformed.append(record)

    if skipped:
        print(f"[WARN] {skipped} record(s) skipped due to missing fields.")

    return transformed
