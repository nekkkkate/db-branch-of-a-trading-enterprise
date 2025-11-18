CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS customer (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_name VARCHAR(50) NOT NULL,
  customer_uid uuid NOT NULL DEFAULT gen_random_uuid(),
  modified_date timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS product (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku text UNIQUE NOT NULL,
  product_name VARCHAR(50) NOT NULL,
  price numeric(12,2) NOT NULL CHECK (price >= 0),
  product_uid uuid NOT NULL DEFAULT gen_random_uuid(),
  modified_date timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS category (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  category_name VARCHAR(50) UNIQUE NOT NULL,
  category_uid uuid NOT NULL DEFAULT gen_random_uuid(),
  modified_date timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS product_category (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_id int NOT NULL,
  category_id int NOT NULL,
  uid_product_category uuid NOT NULL DEFAULT gen_random_uuid(),
  modified_date timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT product_category_uniq UNIQUE (product_id, category_id)
);

CREATE TABLE IF NOT EXISTS sale (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id int NOT NULL,
  sale_date timestamptz NOT NULL DEFAULT now(),
  in_total numeric(12,2) NOT NULL DEFAULT 0,
  sale_uid uuid NOT NULL DEFAULT gen_random_uuid(),
  modified_date timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sale_item (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sale_id int NOT NULL,
  product_id int NOT NULL,
  quantity int NOT NULL CHECK (quantity > 0),
  unit_price numeric(12,2) NOT NULL CHECK (unit_price >= 0),
  line_total numeric(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
  sale_item_uid uuid NOT NULL DEFAULT gen_random_uuid(),
  modified_date timestamptz NOT NULL DEFAULT now()
);