DROP TABLE IF EXISTS public.sales_silver;

CREATE TABLE public.sales_silver (
    transaction_id VARCHAR(255) PRIMARY KEY,
    product_name VARCHAR(100),
    product_category VARCHAR(100),
    quantity INTEGER,
    unit_price DECIMAL(10, 2), 
    total_price DECIMAL(10, 2),
    store_id VARCHAR(50),
    source_system VARCHAR(50), 
    
    sale_timestamp TIMESTAMP WITH TIME ZONE,     
    ingested_at TIMESTAMP WITH TIME ZONE,         
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP 
);

CREATE INDEX IF NOT EXISTS idx_silver_sales_store ON public.silver_sales(store_id);
CREATE INDEX IF NOT EXISTS idx_silver_sales_date ON public.silver_sales(transaction_at);