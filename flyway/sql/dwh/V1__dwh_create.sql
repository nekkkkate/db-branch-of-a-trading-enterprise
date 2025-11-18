CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_uid UUID UNIQUE NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    load_date TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to TIMESTAMPTZ DEFAULT '9999-12-31'::timestamptz,
    is_current BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS dwh.dim_category (
    category_key SERIAL PRIMARY KEY,
    category_uid UUID UNIQUE NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    load_date TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to TIMESTAMPTZ DEFAULT '9999-12-31'::timestamptz,
    is_current BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_uid UUID UNIQUE NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    sku TEXT NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    load_date TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to TIMESTAMPTZ DEFAULT '9999-12-31'::timestamptz,
    is_current BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS dwh.dim_product_category (
    product_category_key SERIAL PRIMARY KEY,
    product_key INT NOT NULL,
    category_key INT NOT NULL,
    source_uid UUID UNIQUE NOT NULL,
    load_date TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (product_key) REFERENCES dwh.dim_product(product_key),
    FOREIGN KEY (category_key) REFERENCES dwh.dim_category(category_key),
    UNIQUE(product_key, category_key)
);

CREATE TABLE IF NOT EXISTS dwh.dim_branch (
    branch_key SERIAL PRIMARY KEY,
    branch_name VARCHAR(100) NOT NULL,
    database_name VARCHAR(50) UNIQUE NOT NULL,
    city VARCHAR(100),
    region VARCHAR(100),
    load_date TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_sale (
    sale_key SERIAL PRIMARY KEY,
    sale_uid UUID UNIQUE NOT NULL,
    sale_number VARCHAR(50),
    source_system VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    weekday INT NOT NULL,
    weekday_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    load_date TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    sale_item_key SERIAL PRIMARY KEY,

    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    branch_key INT NOT NULL,
    sale_key INT NOT NULL,
    sale_date_key INT NOT NULL,

    sale_uid UUID NOT NULL,
    sale_item_uid UUID UNIQUE NOT NULL,

    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    line_total NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,

    sale_date TIMESTAMPTZ NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    load_date TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (customer_key) REFERENCES dwh.dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dwh.dim_product(product_key),
    FOREIGN KEY (branch_key) REFERENCES dwh.dim_branch(branch_key),
    FOREIGN KEY (sale_key) REFERENCES dwh.dim_sale(sale_key),
    FOREIGN KEY (sale_date_key) REFERENCES dwh.dim_date(date_key)
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_uid ON dwh.dim_customer(customer_uid);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON dwh.dim_customer(is_current);

CREATE INDEX IF NOT EXISTS idx_dim_product_uid ON dwh.dim_product(product_uid);
CREATE INDEX IF NOT EXISTS idx_dim_product_sku ON dwh.dim_product(sku);
CREATE INDEX IF NOT EXISTS idx_dim_product_current ON dwh.dim_product(is_current);

CREATE INDEX IF NOT EXISTS idx_dim_category_uid ON dwh.dim_category(category_uid);
CREATE INDEX IF NOT EXISTS idx_dim_category_current ON dwh.dim_category(is_current);

CREATE INDEX IF NOT EXISTS idx_dim_sale_uid ON dwh.dim_sale(sale_uid);
CREATE INDEX IF NOT EXISTS idx_dim_sale_number ON dwh.dim_sale(sale_number);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON dwh.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON dwh.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_branch ON dwh.fact_sales(branch_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_sale ON dwh.fact_sales(sale_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON dwh.fact_sales(sale_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_sale_date ON dwh.fact_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_source ON dwh.fact_sales(source_system);
CREATE INDEX IF NOT EXISTS idx_fact_sales_sale_uid ON dwh.fact_sales(sale_uid);