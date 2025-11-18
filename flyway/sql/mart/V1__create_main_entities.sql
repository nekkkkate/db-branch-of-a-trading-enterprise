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

CREATE INDEX IF NOT EXISTS idx_fact_week ON fact_weekly_sales(week_key);
CREATE INDEX IF NOT EXISTS idx_fact_branch ON fact_weekly_sales(branch_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_weekly_sales(product_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_week_branch_product
  ON fact_weekly_sales(week_key, branch_key, product_key);

DO $$
DECLARE
  week_start date := ('2023-01-01'::date - EXTRACT(ISODOW FROM '2023-01-01'::date)::int + 1)::date;
  last date := '2025-12-31';
  wk_iso int;
  wk_year int;
  wk_key int;
BEGIN
  WHILE week_start <= last LOOP
    wk_iso := EXTRACT(week FROM week_start)::int;
    wk_year := EXTRACT(isoyear FROM week_start)::int;
    wk_key := wk_year * 100 + wk_iso;

    INSERT INTO dim_week (week_key, week_start_date, week_end_date, year, iso_week)
    VALUES (wk_key, week_start, week_start + INTERVAL '6 days', wk_year, wk_iso)
    ON CONFLICT (week_key) DO NOTHING;

    week_start := week_start + INTERVAL '1 week';
  END LOOP;
END $$;
