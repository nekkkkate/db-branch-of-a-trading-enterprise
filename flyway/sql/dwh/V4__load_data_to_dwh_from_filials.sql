CREATE OR REPLACE PROCEDURE dwh.load_from_branches()
LANGUAGE plpgsql
AS $$
DECLARE
    branch RECORD;
    old_search_path text;
BEGIN
    old_search_path := current_setting('search_path');

    FOR branch IN
        SELECT * FROM (
            VALUES
                ('West', 'filial_west'),
                ('East', 'filial_east')
        ) AS t(branch_name, schema_name)
    LOOP
        RAISE NOTICE '==== Загрузка из филиала: % (% схема) ====', branch.branch_name, branch.schema_name;

        PERFORM set_config('search_path', format('%I,public', branch.schema_name), true);

        INSERT INTO dwh.dim_customer (customer_uid, customer_name, source_system)
        SELECT c.customer_uid, c.customer_name, branch.branch_name
        FROM   customer AS c
        WHERE NOT EXISTS (
            SELECT 1 FROM dwh.dim_customer dc
            WHERE dc.customer_uid = c.customer_uid
        );

        INSERT INTO dwh.dim_category (category_uid, category_name, source_system)
        SELECT c.category_uid, c.category_name, branch.branch_name
        FROM   category AS c
        WHERE NOT EXISTS (
            SELECT 1 FROM dwh.dim_category dc
            WHERE dc.category_uid = c.category_uid
        );

        INSERT INTO dwh.dim_product (product_uid, product_name, sku, source_system)
        SELECT p.product_uid, p.product_name, p.sku, branch.branch_name
        FROM   product AS p
        WHERE NOT EXISTS (
            SELECT 1 FROM dwh.dim_product dp
            WHERE dp.product_uid = p.product_uid
        );

        INSERT INTO dwh.dim_product_category (product_key, category_key, source_uid)
        SELECT
            dp.product_key,
            dc.category_key,
            pc.uid_product_category
        FROM product_category AS pc
        JOIN dwh.dim_product dp ON dp.product_uid = (
            SELECT product_uid
            FROM product p2
            WHERE p2.id = pc.product_id
        )
        JOIN dwh.dim_category dc ON dc.category_uid = (
            SELECT category_uid
            FROM category c2
            WHERE c2.id = pc.category_id
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO dwh.dim_sale (sale_uid, sale_number, source_system)
        SELECT s.sale_uid, s.id::text, branch.branch_name
        FROM sale AS s
        WHERE NOT EXISTS (
            SELECT 1 FROM dwh.dim_sale ds
            WHERE ds.sale_uid = s.sale_uid
        );

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
        SELECT
            dc.customer_key,
            dp.product_key,
            (SELECT branch_key FROM dwh.dim_branch b WHERE b.database_name = branch.schema_name),
            ds.sale_key,
            TO_CHAR(s.sale_date, 'YYYYMMDD')::int,
            s.sale_uid,
            si.sale_item_uid,
            si.quantity,
            si.unit_price,
            s.sale_date,
            branch.branch_name
        FROM sale_item AS si
        JOIN sale AS s ON s.id = si.sale_id
        JOIN dwh.dim_customer dc ON dc.customer_uid = (
            SELECT customer_uid FROM customer c WHERE c.id = s.customer_id
        )
        JOIN dwh.dim_product dp ON dp.product_uid = (
            SELECT product_uid FROM product p WHERE p.id = si.product_id
        )
        JOIN dwh.dim_sale ds ON ds.sale_uid = s.sale_uid
        WHERE NOT EXISTS (
            SELECT 1 FROM dwh.fact_sales fs
            WHERE fs.sale_item_uid = si.sale_item_uid
        );

    END LOOP;

    PERFORM set_config('search_path', old_search_path, true);

    RAISE NOTICE 'Загрузка успешно завершена!';
END;
$$;
