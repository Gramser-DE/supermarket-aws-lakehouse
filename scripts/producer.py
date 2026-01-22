import boto3
import json
import time
import os
import random
import uuid
from datetime import datetime, timezone

STREAM_NAME = "supermarket-sales-stream"
AWS_REGION = os.getenv("AWS_REGION")
LOCALSTACK_ENDPOINT = "http://localstack:4566"

kinesis_client = boto3.client(
    "kinesis",
    region_name=AWS_REGION,
    endpoint_url=LOCALSTACK_ENDPOINT,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def generate_sale():
    products = [
        {"name": "Milk", "category": "Dairy", "price": 1.20},
        {"name": "Bread", "category": "Bakery", "price": 0.85},
        {"name": "Apples", "category": "Fruit", "price": 2.50},
        {"name": "Kitchen Roll", "category": "Household", "price": 3.90},
        {"name": "Oil", "category": "Pantry", "price": 4.20}
    ]
    
    product = random.choice(products)
    quantity = random.randint(1, 5)
    
    return {
        "transaction_id": str(uuid.uuid4()),
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "total_price": round(product["price"] * quantity, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": "Toledo_01"
    }

def main():
    print(f"Starting producer on stream: {STREAM_NAME}")
    try:
        while True:
            data = generate_sale()
            encoded_data = json.dumps(data).encode("utf-8")
            
            response = kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=encoded_data,
                PartitionKey=data["product_name"]
            )
            
            print(f"Sent: {data['product_name']} | Sequence: {response['SequenceNumber'][:12]}")
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nProducer stopped.")

if __name__ == "__main__":
    main()