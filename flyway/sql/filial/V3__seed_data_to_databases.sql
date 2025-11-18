INSERT INTO category (category_name)
SELECT 'Category ' || g
FROM generate_series(1,8) g;

INSERT INTO customer (customer_name)
SELECT 'Customer ' || g
FROM generate_series(1,25) g;

INSERT INTO product (sku, product_name, price)
SELECT LPAD(g::text,3,'0') AS sku_full,
       'Product ' || g,
       round((10 + random()*990)::numeric, 2)
FROM generate_series(1,25) g;

INSERT INTO product_category (product_id, category_id)
SELECT p.id, ((p.id - 1) % 8) + 1
FROM product p
UNION ALL
SELECT p.id, ((p.id) % 8) + 1
FROM product p;

INSERT INTO sale (customer_id, sale_date)
SELECT (SELECT id FROM customer ORDER BY random() LIMIT 1),
       now() - ((floor(random()*365)::int) || ' days')::interval
FROM generate_series(1,25);

INSERT INTO sale_item (sale_id, product_id, quantity, unit_price)
SELECT s.id, p.id, (floor(random()*5)+1)::int, 
       round((p.price * (0.6 + random()*0.8))::numeric, 2)
FROM generate_series(1,50)
  CROSS JOIN LATERAL (SELECT id, price FROM product ORDER BY random() LIMIT 1) p
  CROSS JOIN LATERAL (SELECT id FROM sale ORDER BY random() LIMIT 1) s;

UPDATE sale
SET in_total = sub.sum
FROM (
  SELECT sale_id, SUM(line_total) AS sum
  FROM sale_item
  GROUP BY sale_id
) sub
WHERE sale.id = sub.sale_id;