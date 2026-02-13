import time
import json
from scripts.data_producer.generator import generate_sale
from scripts.data_producer.sender import get_kinesis_client, send_records
from scripts.data_producer.config import STREAM_NAME


def run():
    print(f"[INFO] [START] Starting Producer...")
    print(f"[INFO] [CONFIG] Target Stream: {STREAM_NAME}")
    kinesis = get_kinesis_client()
    BATCH_SIZE = 50  
    batch_buffer = []
    try:
        while True:
            sale = generate_sale()
            record = {
                "Data": json.dumps(sale),
                "PartitionKey": sale["transaction_id"] 
            }
            batch_buffer.append(record)

            if len(batch_buffer) >= BATCH_SIZE:
                send_records(kinesis, batch_buffer)
                batch_buffer = []
                time.sleep(0.5) 

    except KeyboardInterrupt:
        print(f"\n[INFO] [STOP] Producer stopped by user.")
    except Exception as e:
        print(f"[ERROR] Unexpected error in the main bucle: {e}")


if __name__ == "__main__":
    run()