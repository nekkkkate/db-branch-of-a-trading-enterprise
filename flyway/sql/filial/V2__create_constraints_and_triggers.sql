ALTER TABLE customer ADD CONSTRAINT customer_uid_uniq UNIQUE (customer_uid);
ALTER TABLE product ADD CONSTRAINT product_uid_uniq UNIQUE (product_uid);
ALTER TABLE category ADD CONSTRAINT category_uid_uniq UNIQUE (category_uid);
ALTER TABLE product_category ADD CONSTRAINT product_category_uid_uniq UNIQUE (uid_product_category);
ALTER TABLE sale ADD CONSTRAINT sale_uid_uniq UNIQUE (sale_uid);
ALTER TABLE sale_item ADD CONSTRAINT sale_item_uid_uniq UNIQUE (sale_item_uid);

ALTER TABLE product_category
  ADD CONSTRAINT fk_pc_product FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE CASCADE;

ALTER TABLE product_category
  ADD CONSTRAINT fk_pc_category FOREIGN KEY (category_id) REFERENCES category(id) ON DELETE RESTRICT;

ALTER TABLE sale
  ADD CONSTRAINT fk_sale_customer FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE RESTRICT;

ALTER TABLE sale_item
  ADD CONSTRAINT fk_si_sale FOREIGN KEY (sale_id) REFERENCES sale(id) ON DELETE CASCADE;

ALTER TABLE sale_item
  ADD CONSTRAINT fk_si_product FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_sale_customer_id ON sale(customer_id);
CREATE INDEX IF NOT EXISTS idx_si_sale_id ON sale_item(sale_id);
CREATE INDEX IF NOT EXISTS idx_si_product_id ON sale_item(product_id);
CREATE INDEX IF NOT EXISTS idx_pc_product_id ON product_category(product_id);
CREATE INDEX IF NOT EXISTS idx_pc_category_id ON product_category(category_id);

CREATE OR REPLACE FUNCTION set_modified_date()
RETURNS trigger AS $$
BEGIN
  NEW.modified_date := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customer_moddate ON customer;
CREATE TRIGGER trg_customer_moddate BEFORE INSERT OR UPDATE ON customer
  FOR EACH ROW EXECUTE FUNCTION set_modified_date();

DROP TRIGGER IF EXISTS trg_product_moddate ON product;
CREATE TRIGGER trg_product_moddate BEFORE INSERT OR UPDATE ON product
  FOR EACH ROW EXECUTE FUNCTION set_modified_date();

DROP TRIGGER IF EXISTS trg_category_moddate ON category;
CREATE TRIGGER trg_category_moddate BEFORE INSERT OR UPDATE ON category
  FOR EACH ROW EXECUTE FUNCTION set_modified_date();

DROP TRIGGER IF EXISTS trg_product_category_moddate ON product_category;
CREATE TRIGGER trg_product_category_moddate BEFORE INSERT OR UPDATE ON product_category
  FOR EACH ROW EXECUTE FUNCTION set_modified_date();

DROP TRIGGER IF EXISTS trg_sale_moddate ON sale;
CREATE TRIGGER trg_sale_moddate BEFORE INSERT OR UPDATE ON sale
  FOR EACH ROW EXECUTE FUNCTION set_modified_date();

DROP TRIGGER IF EXISTS trg_sale_item_moddate ON sale_item;
CREATE TRIGGER trg_sale_item_moddate BEFORE INSERT OR UPDATE ON sale_item
  FOR EACH ROW EXECUTE FUNCTION set_modified_date();