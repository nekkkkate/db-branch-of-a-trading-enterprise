CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS mart;
SET search_path TO mart;

CREATE TABLE IF NOT EXISTS dim_week (
    week_key INT PRIMARY KEY,
    week_start_date DATE NOT NULL UNIQUE,
    week_end_date DATE NOT NULL,
    year INT NOT NULL,
    iso_week INT NOT NULL,
    load_date TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_weekly_sales (
    weekly_sale_key SERIAL PRIMARY KEY,
    week_key INT NOT NULL,
    branch_key INT NOT NULL,
    product_key INT NOT NULL,
    total_quantity BIGINT NOT NULL DEFAULT 0,
    total_revenue NUMERIC(18,2) NOT NULL DEFAULT 0,
    sales_count BIGINT NOT NULL DEFAULT 0,
    last_update TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (week_key, branch_key, product_key)
);

