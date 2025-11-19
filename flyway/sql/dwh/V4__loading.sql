CREATE OR REPLACE PROCEDURE dwh.load_dimensions_from_branches()
LANGUAGE plpgsql
AS $$
DECLARE
    branch RECORD;
BEGIN
    FOR branch IN
        SELECT database_name AS schema_name, branch_name, branch_key
        FROM dwh.dim_branch
    LOOP
        RAISE NOTICE 'Загружаем измерения из филиала: %', branch.branch_name;

        EXECUTE format('
            INSERT INTO dwh.dim_customer (
                customer_uid, customer_name, source_system
            )
            SELECT
                c.customer_uid,
                c.customer_name,
                %L
            FROM %I.customer c
            WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.dim_customer dc
                WHERE dc.customer_uid = c.customer_uid
            )',
            branch.branch_name, branch.schema_name
        );

        EXECUTE format('
            INSERT INTO dwh.dim_category (
                category_uid, category_name, source_system
            )
            SELECT
                cat.category_uid,
                cat.category_name,
                %L
            FROM %I.category cat
            WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.dim_category dc
                WHERE dc.category_uid = cat.category_uid
            )',
            branch.branch_name, branch.schema_name
        );

        EXECUTE format('
            INSERT INTO dwh.dim_product (
                product_uid, product_name, sku, source_system
            )
            SELECT
                p.product_uid,
                p.product_name,
                p.sku,
                %L
            FROM %I.product p
            WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.dim_product dp
                WHERE dp.product_uid = p.product_uid
            )',
            branch.branch_name, branch.schema_name
        );

        EXECUTE format('
            INSERT INTO dwh.dim_product_category (
                product_key, category_key, source_uid
            )
            SELECT
                dp.product_key,
                dc.category_key,
                pc.uid_product_category
            FROM %I.product_category pc
            JOIN %I.product p ON p.id = pc.product_id
            JOIN %I.category c ON c.id = pc.category_id
            JOIN dwh.dim_product dp ON dp.product_uid = p.product_uid
            JOIN dwh.dim_category dc ON dc.category_uid = c.category_uid
            WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.dim_product_category dpc
                WHERE dpc.source_uid = pc.uid_product_category
            )',
            branch.schema_name, branch.schema_name, branch.schema_name
        );

        EXECUTE format('
            INSERT INTO dwh.dim_sale (
                sale_uid, sale_number, source_system
            )
            SELECT
                s.sale_uid,
                ''SALE-'' || s.id::text,
                %L
            FROM %I.sale s
            WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.dim_sale ds
                WHERE ds.sale_uid = s.sale_uid
            )',
            branch.branch_name, branch.schema_name
        );

    END LOOP;
END;
$$;

CREATE OR REPLACE PROCEDURE dwh.load_facts_from_branches()
LANGUAGE plpgsql
AS $$
DECLARE
    branch RECORD;
BEGIN
    FOR branch IN
        SELECT database_name AS schema_name, branch_name, branch_key
        FROM dwh.dim_branch
    LOOP
        RAISE NOTICE 'Загружаем факты из филиала: %', branch.branch_name;

        EXECUTE format('
            INSERT INTO dwh.dim_date (date_key, full_date, year, quarter, month, month_name, day, weekday, weekday_name, is_weekend)
            SELECT DISTINCT
                TO_CHAR(s.sale_date, ''YYYYMMDD'')::int as date_key,
                s.sale_date::date as full_date,
                EXTRACT(YEAR FROM s.sale_date) as year,
                EXTRACT(QUARTER FROM s.sale_date) as quarter,
                EXTRACT(MONTH FROM s.sale_date) as month,
                TO_CHAR(s.sale_date, ''Month'') as month_name,
                EXTRACT(DAY FROM s.sale_date) as day,
                EXTRACT(ISODOW FROM s.sale_date) as weekday,
                TO_CHAR(s.sale_date, ''Day'') as weekday_name,
                EXTRACT(ISODOW FROM s.sale_date) IN (6,7) as is_weekend
            FROM %I.sale s
            WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.dim_date dd
                WHERE dd.full_date = s.sale_date::date
            )',
            branch.schema_name
        );

        EXECUTE format('
            INSERT INTO dwh.fact_sales (
                customer_key, product_key, branch_key, sale_key,
                sale_date_key, sale_uid, sale_item_uid, quantity,
                unit_price, sale_date, source_system
            )
            SELECT
                dc.customer_key,
                dp.product_key,
                %s as branch_key,
                ds.sale_key,
                TO_CHAR(s.sale_date, ''YYYYMMDD'')::int as sale_date_key,
                s.sale_uid,
                si.sale_item_uid,
                si.quantity,
                si.unit_price,
                s.sale_date,
                %L as source_system
            FROM %I.sale_item si
            JOIN %I.sale s ON s.id = si.sale_id
            JOIN %I.customer c ON c.id = s.customer_id
            JOIN %I.product p ON p.id = si.product_id
            JOIN dwh.dim_customer dc ON dc.customer_uid = c.customer_uid
            JOIN dwh.dim_product dp ON dp.product_uid = p.product_uid
            JOIN dwh.dim_sale ds ON ds.sale_uid = s.sale_uid
            WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.fact_sales fs
                WHERE fs.sale_item_uid = si.sale_item_uid
            )',
            branch.branch_key,
            branch.branch_name,
            branch.schema_name,
            branch.schema_name,
            branch.schema_name,
            branch.schema_name
        );

        RAISE NOTICE 'Завершена загрузка из филиала: %', branch.branch_name;
    END LOOP;
END;
$$;

CALL dwh.load_dimensions_from_branches();
CALL dwh.load_facts_from_branches();

DO $$
BEGIN
    RAISE NOTICE 'Загрузка завершена. Статистика:';
    RAISE NOTICE 'Клиенты: %', (SELECT COUNT(*) FROM dwh.dim_customer);
    RAISE NOTICE 'Продукты: %', (SELECT COUNT(*) FROM dwh.dim_product);
    RAISE NOTICE 'Категории: %', (SELECT COUNT(*) FROM dwh.dim_category);
    RAISE NOTICE 'Продажи: %', (SELECT COUNT(*) FROM dwh.dim_sale);
    RAISE NOTICE 'Факты продаж: %', (SELECT COUNT(*) FROM dwh.fact_sales);
END $$;