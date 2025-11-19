INSERT INTO dwh.dim_branch (branch_key, database_name, branch_name)
VALUES
    (1, 'filial_west', 'filial_west'),
    (2, 'filial_east', 'filial_east');

CREATE OR REPLACE PROCEDURE dwh.load_from_branches()
LANGUAGE plpgsql
AS $$
DECLARE
    branch RECORD;
BEGIN
    FOR branch IN
        SELECT database_name AS schema_name, branch_name
        FROM dwh.dim_branch
    LOOP
        RAISE NOTICE 'Загружаем данные из филиала: %', branch.branch_name;

        EXECUTE format($sql$

            INSERT INTO dwh.fact_sales (
                  customer_key
                , product_key
                , branch_key
                , sale_key
                , sale_date_key
                , sale_uid
                , sale_item_uid
                , quantity
                , unit_price
                , sale_date
                , source_system
            )
            SELECT
                  dc.customer_key
                , dp.product_key
                , br.branch_key
                , ds.sale_key
                , TO_CHAR(s.sale_date, 'YYYYMMDD')::int
                , s.sale_uid
                , si.sale_item_uid
                , si.quantity
                , si.unit_price
                , s.sale_date
                , %L
            FROM %I.sale_item AS si
            JOIN %I.sale AS s
              ON s.id = si.sale_id

            JOIN dwh.dim_customer dc
              ON dc.customer_uid = (
                    SELECT customer_uid
                    FROM %I.customer c
                    WHERE c.id = s.customer_id
              )

            JOIN dwh.dim_product dp
              ON dp.product_uid = (
                    SELECT product_uid
                    FROM %I.product p
                    WHERE p.id = si.product_id
              )

            JOIN dwh.dim_sale ds
              ON ds.sale_uid = s.sale_uid

            JOIN dwh.dim_branch br
              ON br.database_name = %L

            WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.fact_sales fs
                WHERE fs.sale_item_uid = si.sale_item_uid
            );

        $sql$,
        branch.branch_name,
        branch.schema_name,
        branch.schema_name,
        branch.schema_name,
        branch.schema_name,
        branch.schema_name
        );

    END LOOP;

END;
$$;


CALL dwh.load_from_branches();