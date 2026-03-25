DROP TABLE IF EXISTS public.sales_silver;

CREATE TABLE public.sales_silver (
    transaction_id   VARCHAR(255)   NOT NULL,
    product_id       VARCHAR(50)    NOT NULL,
    product_name     VARCHAR(100),
    product_category VARCHAR(100),
    quantity         INTEGER,
    unit_price       DECIMAL(10, 2),
    total_price      DECIMAL(10, 2),
    store_id         VARCHAR(50),
    payment_method   VARCHAR(50),
    client_type      VARCHAR(50),
    sale_timestamp   TIMESTAMP WITH TIME ZONE,
    processed_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_sales_silver PRIMARY KEY (transaction_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_sales_store ON public.sales_silver(store_id);
CREATE INDEX IF NOT EXISTS idx_silver_sales_date  ON public.sales_silver(sale_timestamp);
