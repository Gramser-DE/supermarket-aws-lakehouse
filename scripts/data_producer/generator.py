import random
import uuid
from datetime import datetime, timezone
from scripts.data_producer.config import STORES, PRODUCTS, PAYMENT_METHODS, CLIENT_TYPES

def generate_basket():
    transaction_id = str(uuid.uuid4())
    store = random.choice(STORES)
    timestamp = datetime.now(timezone.utc).isoformat()
    payment_method = random.choice(PAYMENT_METHODS)
    client_type = random.choice(CLIENT_TYPES)

    num_items = random.randint(1, min(5, len(PRODUCTS)))
    products = random.sample(PRODUCTS, num_items)

    line_items = []
    for product in products:
        quantity = random.randint(1, 5)
        line_items.append({
            "transaction_id": transaction_id,
            "product_id": product["id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": product["price"],
            "total_price": round(product["price"] * quantity, 2),
            "timestamp": timestamp,
            "store_id": store,
            "payment_method": payment_method,
            "client_type": client_type,
        })

    return line_items
