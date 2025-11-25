SET search_path TO mart;

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
        (SELECT MAX(fws.last_update) FROM mart.fact_weekly_sales fws) as last_update;
END;
$$;

CREATE OR REPLACE PROCEDURE load_weekly_sales_from_dwh(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_records_processed INTEGER;
    v_weeks_count INTEGER;
    v_fact_sales_count INTEGER;
    v_dim_week_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_fact_sales_count FROM dwh.fact_sales;
    SELECT COUNT(*) INTO v_dim_week_count FROM mart.dim_week;

    RAISE NOTICE 'Диагностика: fact_sales records: %, dim_week records: %',
        v_fact_sales_count, v_dim_week_count;

    IF p_start_date IS NULL THEN
        p_start_date := (CURRENT_DATE - INTERVAL '35 days')::date;
    END IF;

    IF p_end_date IS NULL THEN
        p_end_date := CURRENT_DATE;
    END IF;

    RAISE NOTICE 'Начало загрузки данных в витрину за период с % по %', p_start_date, p_end_date;

    IF v_dim_week_count = 0 THEN
        RAISE NOTICE 'Таблица dim_week пустая. Заполняем календарь...';
        CALL mart.populate_week_calendar(p_start_date - INTERVAL '1 year', p_end_date + INTERVAL '1 year');
    END IF;

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

    SELECT COUNT(*) INTO v_weeks_count FROM mart.fact_weekly_sales;
    RAISE NOTICE 'Всего записей в fact_weekly_sales: %', v_weeks_count;
END;
$$;

CREATE OR REPLACE PROCEDURE populate_week_calendar(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_date DATE;
    v_week_start DATE;
    v_week_end DATE;
    v_year INT;
    v_iso_week INT;
    v_week_key INT;
BEGIN
    IF p_start_date IS NULL THEN
        p_start_date := CURRENT_DATE - INTERVAL '2 years';
    END IF;

    IF p_end_date IS NULL THEN
        p_end_date := CURRENT_DATE + INTERVAL '2 years';
    END IF;

    RAISE NOTICE 'Заполнение календаря недель с % по %', p_start_date, p_end_date;

    v_current_date := p_start_date;

    v_week_start := v_current_date - EXTRACT(ISODOW FROM v_current_date)::INT + 1;

    WHILE v_week_start <= p_end_date LOOP
        v_week_end := v_week_start + 6;
        v_year := EXTRACT(YEAR FROM v_week_start);
        v_iso_week := EXTRACT(WEEK FROM v_week_start);
        v_week_key := (v_year * 100) + v_iso_week;

        INSERT INTO mart.dim_week (
            week_key,
            week_start_date,
            week_end_date,
            year,
            iso_week
        )
        VALUES (
            v_week_key,
            v_week_start,
            v_week_end,
            v_year,
            v_iso_week
        )
        ON CONFLICT (week_key) DO NOTHING;

        v_week_start := v_week_start + 7;
    END LOOP;

    RAISE NOTICE 'Календарь неделей заполнен';
END;
$$;

CREATE OR REPLACE FUNCTION check_data_quality()
RETURNS TABLE(
    table_name TEXT,
    record_count BIGINT,
    min_date TIMESTAMPTZ,
    max_date TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        'dwh.fact_sales'::TEXT,
        COUNT(*),
        MIN(sale_date),
        MAX(sale_date)
    FROM dwh.fact_sales;

    RETURN QUERY
    SELECT
        'mart.dim_week'::TEXT,
        COUNT(*),
        MIN(week_start_date)::TIMESTAMPTZ,
        MAX(week_end_date)::TIMESTAMPTZ
    FROM mart.dim_week;

    RETURN QUERY
    SELECT
        'mart.fact_weekly_sales'::TEXT,
        COUNT(*),
        MIN(dw.week_start_date)::TIMESTAMPTZ,
        MAX(dw.week_end_date)::TIMESTAMPTZ
    FROM mart.fact_weekly_sales fws
    JOIN mart.dim_week dw ON fws.week_key = dw.week_key;
END;
$$;

CREATE OR REPLACE PROCEDURE rebuild_mart()
LANGUAGE plpgsql
AS $$
DECLARE
    v_stats RECORD;
BEGIN
    RAISE NOTICE '=== ПЕРЕЗАГРУЗКА ВИТРИНЫ ДАННЫХ ===';

    RAISE NOTICE '1. Проверка качества данных...';
    PERFORM * FROM mart.check_data_quality();

    RAISE NOTICE '2. Очистка витрины...';
    TRUNCATE TABLE mart.fact_weekly_sales;

    RAISE NOTICE '3. Заполнение календаря недель...';
    CALL mart.populate_week_calendar();

    RAISE NOTICE '4. Загрузка данных в витрину...';
    CALL mart.load_weekly_sales_from_dwh();

    RAISE NOTICE '5. Финальная статистика...';

    SELECT * INTO v_stats FROM mart.get_mart_statistics();

    RAISE NOTICE 'Статистика витрины:';
    RAISE NOTICE '  - Недель в календаре: %', v_stats.total_weeks;
    RAISE NOTICE '  - Филиалов: %', v_stats.total_branches;
    RAISE NOTICE '  - Товаров: %', v_stats.total_products;
    RAISE NOTICE '  - Записей в фактах: %', v_stats.total_records;
    RAISE NOTICE '  - Последнее обновление: %', v_stats.last_update;

    RAISE NOTICE '=== ПЕРЕЗАГРУЗКА ЗАВЕРШЕНА ===';
END;
$$;

DO $$
DECLARE
    v_fact_sales_count BIGINT;
    v_dim_week_count BIGINT;
    v_stats RECORD;
BEGIN
    RAISE NOTICE '=== Начало загрузки данных в витрину ===';

    SELECT COUNT(*) INTO v_fact_sales_count FROM dwh.fact_sales;
    SELECT COUNT(*) INTO v_dim_week_count FROM mart.dim_week;

    RAISE NOTICE 'Проверка данных: fact_sales records: %, dim_week records: %',
        v_fact_sales_count, v_dim_week_count;

    IF v_fact_sales_count > 0 THEN
        IF v_dim_week_count = 0 THEN
            RAISE NOTICE 'Заполняем календарь недель...';
            CALL mart.populate_week_calendar();
        END IF;

        RAISE NOTICE 'Загружаем данные в витрину...';
        CALL mart.load_weekly_sales_from_dwh();

        SELECT * INTO v_stats FROM mart.get_mart_statistics();

        RAISE NOTICE 'Статистика витрины:';
        RAISE NOTICE '  - Недель в календаре: %', v_stats.total_weeks;
        RAISE NOTICE '  - Филиалов: %', v_stats.total_branches;
        RAISE NOTICE '  - Товаров: %', v_stats.total_products;
        RAISE NOTICE '  - Записей в фактах: %', v_stats.total_records;
        RAISE NOTICE '  - Последнее обновление: %', v_stats.last_update;
    ELSE
        RAISE NOTICE 'В DWH нет данных для загрузки в витрину';
    END IF;

    RAISE NOTICE '=== Загрузка данных завершена ===';
END;
$$;