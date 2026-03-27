-- ============================================================
-- Gold layer: Star Schema
-- Run once to initialize the schema.
-- ============================================================

-- Dimensions 

DROP TABLE IF EXISTS public.fact_sales CASCADE;
DROP TABLE IF EXISTS public.dim_product CASCADE;
DROP TABLE IF EXISTS public.dim_store CASCADE;
DROP TABLE IF EXISTS public.dim_date CASCADE;
DROP TABLE IF EXISTS public.dim_payment_method CASCADE;
DROP TABLE IF EXISTS public.dim_client_type CASCADE;


CREATE TABLE public.dim_product (
    product_key CHAR(32) PRIMARY KEY,   
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(100),
    product_category VARCHAR(100)
);

CREATE TABLE public.dim_store (
    store_key CHAR(32) PRIMARY KEY,           
    store_id VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE public.dim_payment_method (
    payment_method_key  CHAR(32) PRIMARY KEY,  
    payment_method VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE public.dim_client_type (
    client_type_key CHAR(32) PRIMARY KEY,     
    client_type VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE public.dim_date (
    date_key INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,      -- 1=Monday … 7=Sunday
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Fact table

CREATE TABLE public.fact_sales (
    sale_key BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) NOT NULL,
    product_key CHAR(32) NOT NULL REFERENCES public.dim_product(product_key),
    store_key CHAR(32) NOT NULL REFERENCES public.dim_store(store_key),
    date_key INTEGER NOT NULL REFERENCES public.dim_date(date_key),
    payment_method_key CHAR(32) NOT NULL REFERENCES public.dim_payment_method(payment_method_key),
    client_type_key CHAR(32) NOT NULL REFERENCES public.dim_client_type(client_type_key),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_price DECIMAL(10, 2),
    sale_timestamp TIMESTAMP WITH TIME ZONE,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_fact_sales UNIQUE (transaction_id, product_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON public.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store ON public.fact_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON public.fact_sales(product_key);

-- Populate dim_date (2024–2027) 

INSERT INTO public.dim_date (
    date_key, full_date, year, quarter, month, month_name,
    week_of_year, day_of_month, day_of_week, day_name, is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d::DATE AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM GENERATE_SERIES(
    '2024-01-01'::DATE,
    '2027-12-31'::DATE,
    '1 day'::INTERVAL
) AS t(d)
ON CONFLICT (date_key) DO NOTHING;
