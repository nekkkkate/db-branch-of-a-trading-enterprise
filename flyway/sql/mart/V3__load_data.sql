SET search_path TO mart;

CREATE OR REPLACE PROCEDURE load_weekly_sales_from_dwh(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_records_processed INTEGER;
BEGIN
    IF p_start_date IS NULL THEN
        p_start_date := (CURRENT_DATE - INTERVAL '35 days')::date;
    END IF;

    IF p_end_date IS NULL THEN
        p_end_date := CURRENT_DATE;
    END IF;

    RAISE NOTICE 'Начало загрузки данных в витрину за период с % по %', p_start_date, p_end_date;

    WITH weekly_sales AS (
        SELECT
            dw.week_key,
            fs.branch_key,
            fs.product_key,
            SUM(fs.quantity) as total_quantity,
            SUM(fs.line_total) as total_revenue,
            COUNT(DISTINCT fs.sale_key) as sales_count
        FROM dwh.fact_sales fs
        JOIN mart.dim_week dw ON fs.sale_date::date BETWEEN dw.week_start_date AND dw.week_end_date
        WHERE fs.sale_date::date BETWEEN p_start_date AND p_end_date
        GROUP BY
            dw.week_key,
            fs.branch_key,
            fs.product_key
    )
    INSERT INTO mart.fact_weekly_sales (
        week_key,
        branch_key,
        product_key,
        total_quantity,
        total_revenue,
        sales_count
    )
    SELECT
        week_key,
        branch_key,
        product_key,
        total_quantity,
        total_revenue,
        sales_count
    FROM weekly_sales
    ON CONFLICT (week_key, branch_key, product_key)
    DO UPDATE SET
        total_quantity = EXCLUDED.total_quantity,
        total_revenue = EXCLUDED.total_revenue,
        sales_count = EXCLUDED.sales_count,
        last_update = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_records_processed = ROW_COUNT;

    RAISE NOTICE 'Загрузка завершена. Обработано записей: %', v_records_processed;
END;
$$;

CREATE OR REPLACE FUNCTION load_weekly_sales_last_n_weeks(
    p_weeks_count INTEGER DEFAULT 5
)
RETURNS TABLE(
    week_start_date DATE,
    branch_name VARCHAR,
    product_name VARCHAR,
    total_quantity BIGINT,
    total_revenue NUMERIC,
    sales_count BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_end_date DATE := CURRENT_DATE;
    v_start_date DATE;
BEGIN
    v_start_date := v_end_date - (p_weeks_count * 7 || ' days')::INTERVAL;

    CALL mart.load_weekly_sales_from_dwh(v_start_date, v_end_date);

    RETURN QUERY
    SELECT
        dw.week_start_date,
        db.branch_name,
        dp.product_name,
        fws.total_quantity,
        fws.total_revenue,
        fws.sales_count
    FROM mart.fact_weekly_sales fws
    JOIN mart.dim_week dw ON fws.week_key = dw.week_key
    JOIN dwh.dim_branch db ON fws.branch_key = db.branch_key
    JOIN dwh.dim_product dp ON fws.product_key = dp.product_key
    WHERE dw.week_start_date >= v_start_date
    ORDER BY dw.week_start_date DESC, fws.total_revenue DESC
    LIMIT 20;
END;
$$;

CREATE OR REPLACE FUNCTION get_mart_statistics()
RETURNS TABLE(
    total_weeks INTEGER,
    total_branches INTEGER,
    total_products INTEGER,
    total_records BIGINT,
    last_update TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        (SELECT COUNT(*) FROM mart.dim_week)::INTEGER as total_weeks,
        (SELECT COUNT(DISTINCT branch_key) FROM mart.fact_weekly_sales)::INTEGER as total_branches,
        (SELECT COUNT(DISTINCT product_key) FROM mart.fact_weekly_sales)::INTEGER as total_products,
        (SELECT COUNT(*) FROM mart.fact_weekly_sales) as total_records,
        (SELECT MAX(last_update) FROM mart.fact_weekly_sales) as last_update;
END;
$$;

DO $$
DECLARE
    v_stats RECORD;
BEGIN
    RAISE NOTICE '=== Загрузка данных в витрину ===';

    CALL mart.load_weekly_sales_from_dwh();

    SELECT * INTO v_stats FROM mart.get_mart_statistics();

    RAISE NOTICE 'Статистика витрины:';
    RAISE NOTICE '  - Недель в календаре: %', v_stats.total_weeks;
    RAISE NOTICE '  - Филиалов: %', v_stats.total_branches;
    RAISE NOTICE '  - Товаров: %', v_stats.total_products;
    RAISE NOTICE '  - Записей в фактах: %', v_stats.total_records;
    RAISE NOTICE '  - Последнее обновление: %', v_stats.last_update;

    RAISE NOTICE '';
    RAISE NOTICE 'Пример данных (топ-5 по выручке):';
END;
$$;