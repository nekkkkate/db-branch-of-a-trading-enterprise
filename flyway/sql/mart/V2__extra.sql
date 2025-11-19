SET search_path TO mart;

CREATE INDEX IF NOT EXISTS idx_fact_week ON fact_weekly_sales(week_key);
CREATE INDEX IF NOT EXISTS idx_fact_branch ON fact_weekly_sales(branch_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_weekly_sales(product_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_week_branch_product
  ON fact_weekly_sales(week_key, branch_key, product_key);

