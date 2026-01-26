CREATE TABLE IF NOT EXISTS silver_sales (
    transaction_id VARCHAR(50) PRIMARY KEY, 
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    price_unit DECIMAL(10, 2) NOT NULL,
    total_price DECIMAL(10, 2) NOT NULL,
    store_id VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    transaction_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,    
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP 
);

CREATE INDEX IF NOT EXISTS idx_silver_sales_store ON silver_sales(store_id);
CREATE INDEX IF NOT EXISTS idx_silver_sales_date ON silver_sales(transaction_at);