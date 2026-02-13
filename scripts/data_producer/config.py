import os

STREAM_NAME = "supermarket-sales-stream"
AWS_REGION = os.getenv("AWS_REGION")
LOCALSTACK_ENDPOINT = os.getenv("AWS_ENDPOINT_URL")
AWS_ACCES_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")

STORES = [
    "Toledo_01", "Toledo_02",
    "Madrid_01", "Madrid_02", "Madrid_03", 
    "Malaga_01", 
    "Valencia_01", 
    "Barcelona_01", "Barcelona_02"
    ]

PRODUCTS = [
    {"id": "P001","name": "Milk", "category": "Dairy", "price": 1.20},
    {"id": "P002","name": "Bread", "category": "Bakery", "price": 0.85},
    {"id": "P003","name": "Apples", "category": "Fruit", "price": 2.50},
    {"id": "P004","name": "Kitchen Roll", "category": "Household", "price": 3.90},
    {"id": "P005","name": "Oil", "category": "Pantry", "price": 4.20},
    {"id": "P006", "name": "Shampoo", "category": "Hygiene", "price": 4.10},
    {"id": "P007", "name": "Water 1.5L", "category": "Beverages", "price": 0.60},
    {"id": "P008", "name": "Spaghetti", "category": "Pantry", "price": 1.10},
    {"id": "P009", "name": "Tomato Sauce", "category": "Pantry", "price": 1.35},
    {"id": "P010", "name": "Toothpaste", "category": "Hygiene", "price": 2.20}
    ]
        