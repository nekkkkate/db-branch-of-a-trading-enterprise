SET search_path TO mart;

DO $$
DECLARE
  week_start date := ('2023-01-01'::date - EXTRACT(ISODOW FROM '2023-01-01'::date)::int + 1)::date;
  last date := '2025-12-31';
  wk_iso int;
  wk_year int;
  wk_key int;
BEGIN
  WHILE week_start <= last LOOP
    wk_iso := EXTRACT(week FROM week_start)::int;
    wk_year := EXTRACT(isoyear FROM week_start)::int;
    wk_key := wk_year * 100 + wk_iso;

    INSERT INTO dim_week (week_key, week_start_date, week_end_date, year, iso_week)
    VALUES (wk_key, week_start, week_start + INTERVAL '6 days', wk_year, wk_iso)
    ON CONFLICT (week_key) DO NOTHING;

    week_start := week_start + INTERVAL '1 week';
  END LOOP;
END $$;