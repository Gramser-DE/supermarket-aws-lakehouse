import random
import uuid
from datetime import datetime, timezone
from scripts.data_producer.config import STORES, PRODUCTS

def generate_sale():
    store = random.choice(STORES)
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    return {
        "transaction_id": str(uuid.uuid4()),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_price": round(product["price"] * quantity, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": store,
        "payment_method": random.choice(["Credit Card", "Cash", "App"]),
        "client_type": random.choice(["Regular", "Premium", "New"])
    }
