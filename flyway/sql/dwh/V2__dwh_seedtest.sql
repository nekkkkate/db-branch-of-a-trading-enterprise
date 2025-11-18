INSERT INTO dwh.dim_branch (branch_name, database_name, city, region)
VALUES
    ('Центральный филиал', 'filial_central', 'Москва', 'Центральный округ'),
    ('Северный филиал', 'filial_north', 'Санкт-Петербург', 'Северо-Западный округ'),
    ('Восточный филиал', 'filial_east', 'Новосибирск', 'Сибирский округ')
ON CONFLICT (database_name) DO NOTHING;

INSERT INTO dwh.dim_customer (customer_uid, customer_name, source_system)
VALUES
    (gen_random_uuid(), 'ООО Альфа', 'CRM'),
    (gen_random_uuid(), 'ООО Бета', 'CRM'),
    (gen_random_uuid(), 'ООО Гамма', 'ERP');

INSERT INTO dwh.dim_category (category_uid, category_name, source_system)
VALUES
    (gen_random_uuid(), 'Электроника', 'PIM'),
    (gen_random_uuid(), 'Бытовая техника', 'PIM'),
    (gen_random_uuid(), 'Одежда', 'PIM');

INSERT INTO dwh.dim_product (product_uid, product_name, sku, source_system)
VALUES
    (gen_random_uuid(), 'Смартфон X100', 'SKU100', 'ERP'),
    (gen_random_uuid(), 'Холодильник Frosty 2000', 'SKU200', 'ERP'),
    (gen_random_uuid(), 'Футболка Cotton', 'SKU300', 'ERP');

INSERT INTO dwh.dim_product_category (product_key, category_key, source_uid)
VALUES
    (1, 1, gen_random_uuid()),
    (2, 2, gen_random_uuid()),
    (3, 3, gen_random_uuid())
ON CONFLICT DO NOTHING;

DO $$
DECLARE
    start_date DATE := '2024-01-01';
    end_date DATE := '2024-12-31';
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO dwh.dim_date (
            date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            day,
            weekday,
            weekday_name,
            is_weekend
        ) VALUES (
            TO_CHAR(curr_date, 'YYYYMMDD')::INT,
            curr_date,
            EXTRACT(YEAR FROM curr_date)::INT,
            EXTRACT(QUARTER FROM curr_date)::INT,
            EXTRACT(MONTH FROM curr_date)::INT,
            TO_CHAR(curr_date, 'FMMonth'),
            EXTRACT(DAY FROM curr_date)::INT,
            EXTRACT(DOW FROM curr_date)::INT,
            TO_CHAR(curr_date, 'FMDay'),
            EXTRACT(DOW FROM curr_date) IN (0, 6)
        )
        ON CONFLICT (date_key) DO NOTHING;

        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END $$;

INSERT INTO dwh.dim_sale (sale_uid, sale_number, source_system)
VALUES
    (gen_random_uuid(), 'SALE-001', 'POS'),
    (gen_random_uuid(), 'SALE-002', 'POS'),
    (gen_random_uuid(), 'SALE-003', 'POS');

INSERT INTO dwh.fact_sales (
    customer_key,
    product_key,
    branch_key,
    sale_key,
    sale_date_key,
    sale_uid,
    sale_item_uid,
    quantity,
    unit_price,
    sale_date,
    source_system
)
VALUES
    (1, 1, 1, 1, 20240115, gen_random_uuid(), gen_random_uuid(), 2, 35000.00, '2024-01-15', 'POS'),
    (1, 2, 1, 1, 20240115, gen_random_uuid(), gen_random_uuid(), 1, 55000.00, '2024-01-15', 'POS'),
    (2, 3, 2, 2, 20240210, gen_random_uuid(), gen_random_uuid(), 3, 1500.00, '2024-02-10', 'POS'),
    (3, 1, 3, 3, 20240305, gen_random_uuid(), gen_random_uuid(), 1, 36000.00, '2024-03-05', 'POS');