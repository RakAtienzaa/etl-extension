"""Extension C: hourly refresh of the daily category sales data mart.

dw.v_daily_category_sales      -> plain view (always live)
dw.mv_daily_category_sales     -> materialized copy (refreshed every hour)
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_CONN_ID = "dwh_postgres"

# Same SQL as sql/extension_c.sql. It is repeated here because only dags/ is
# mounted into the Airflow containers. Every statement is safe to run again.
CREATE_OBJECTS_SQL = """
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
"""


@dag(
    dag_id="refresh_daily_category_sales",
    schedule="@hourly",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["cselec1c", "mart"],
)
def refresh_daily_category_sales():
    @task
    def create_objects() -> None:
        """Create the view and the materialized view if they do not exist yet."""
        PostgresHook(postgres_conn_id=DWH_CONN_ID).run(CREATE_OBJECTS_SQL)
        print("View and materialized view are in place")

    @task
    def refresh() -> dict:
        """Rebuild the materialized view from the live view."""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        hook.run("REFRESH MATERIALIZED VIEW dw.mv_daily_category_sales;")
        rows = hook.get_first("SELECT COUNT(*) FROM dw.mv_daily_category_sales")[0]
        total = hook.get_first("SELECT COALESCE(SUM(sales_php), 0) FROM dw.mv_daily_category_sales")[0]
        print(f"Refreshed dw.mv_daily_category_sales: {rows} rows, total sales {total}")
        return {"rows": rows, "total_sales": float(total)}

    create_objects() >> refresh()


refresh_daily_category_sales()
