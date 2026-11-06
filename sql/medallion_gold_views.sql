-- Governed Medallion Gold Layer Analytical Views (Databricks SQL / Snowflake / SQLite Compatible)
-- Reconciles Strategic Customers & Segments KPIs using SQL Joins & CTEs

WITH segment_performance AS (
    SELECT
        f.segment_code,
        d.segment_name,
        f.sales_region,
        f.fiscal_quarter,
        COUNT(DISTINCT f.account_id) AS active_strategic_accounts,
        COUNT(DISTINCT f.invoice_no) AS completed_orders,
        ROUND(SUM(f.net_revenue_usd), 2) AS net_revenue_usd,
        ROUND(SUM(f.gross_margin_usd), 2) AS gross_margin_usd,
        ROUND(SUM(f.gross_margin_usd) * 100.0 / NULLIF(SUM(f.net_revenue_usd), 0), 2) AS gross_margin_pct
    FROM gold_fact_sales_orders f
    INNER JOIN gold_dim_customer_segment d
        ON f.account_id = d.account_id
    GROUP BY f.segment_code, d.segment_name, f.sales_region, f.fiscal_quarter
)
SELECT
    segment_code,
    segment_name,
    sales_region,
    fiscal_quarter,
    active_strategic_accounts,
    completed_orders,
    net_revenue_usd,
    gross_margin_usd,
    gross_margin_pct,
    ROUND(net_revenue_usd / NULLIF(active_strategic_accounts, 0), 2) AS avg_revenue_per_account_usd
FROM segment_performance
ORDER BY fiscal_quarter, net_revenue_usd DESC;
