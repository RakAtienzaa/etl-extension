CREATE OR REPLACE VIEW dw.v_daily_category_sales AS
SELECT d.full_date,
       d.day_of_week,
       p.category,
       SUM(f.line_total) AS sales_php,
       SUM(f.quantity)   AS units_sold
FROM dw.fact_sales f
JOIN dw.dim_date    d ON d.date_key    = f.date_key
JOIN dw.dim_product p ON p.product_key = f.product_key
GROUP BY d.full_date, d.day_of_week, p.category;

CREATE MATERIALIZED VIEW IF NOT EXISTS dw.mv_daily_category_sales AS
SELECT * FROM dw.v_daily_category_sales;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_daily_category_sales
    ON dw.mv_daily_category_sales (full_date, category);
