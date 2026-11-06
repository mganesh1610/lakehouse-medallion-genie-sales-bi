#!/usr/bin/env python3
"""
Governed Data Catalog & Unity Catalog-Style Lineage & Data Quality Contract Validator.
Executes automated schema, primary/foreign key, nullability, and margin conservation checks
across Bronze, Silver, and Gold medallion tables.
"""
import csv
import json
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "outputs", "medallion_lakehouse.db")
CATALOG_JSON = os.path.join(BASE_DIR, "catalog", "governed_data_catalog.json")
AUDIT_CSV = os.path.join(BASE_DIR, "outputs", "data_catalog_lineage_audit.csv")


def run_catalog_governance_checks(db_path: str = DB_PATH) -> dict:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    checks = []

    def add_check(check_id: str, layer: str, rule_name: str, passed: bool, observed_value: str):
        checks.append({
            "check_id": check_id,
            "medallion_layer": layer,
            "governance_rule": rule_name,
            "status": "PASS" if passed else "FAIL",
            "observed_metric": observed_value
        })

    # 1. Bronze Layer Checks
    b_cnt = cur.execute("SELECT COUNT(*) FROM bronze_raw_invoices").fetchone()[0]
    add_check("CAT-01", "Bronze", "Raw ingestion row count > 0", b_cnt > 0, f"rows={b_cnt}")
    b_src = cur.execute("SELECT COUNT(*) FROM bronze_raw_invoices WHERE source_system IS NULL").fetchone()[0]
    add_check("CAT-02", "Bronze", "100% source_system lineage tagging", b_src == 0, f"missing_source={b_src}")

    # 2. Silver Layer Checks
    s_cnt = cur.execute("SELECT COUNT(*) FROM silver_cleansed_transactions").fetchone()[0]
    s_dups = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT invoice_no, product_id, COUNT(*) c
            FROM silver_cleansed_transactions
            GROUP BY invoice_no, product_id HAVING c > 1
        )
    """).fetchone()[0]
    add_check("CAT-03", "Silver", "Zero duplicate (invoice_no, product_id) keys", s_dups == 0, f"duplicates={s_dups}")

    s_untrimmed = cur.execute("""
        SELECT COUNT(*) FROM silver_cleansed_transactions
        WHERE account_id != TRIM(UPPER(account_id))
    """).fetchone()[0]
    add_check("CAT-04", "Silver", "Canonical uppercase trimmed account_id keys", s_untrimmed == 0, f"untrimmed={s_untrimmed}")

    s_neg = cur.execute("""
        SELECT COUNT(*) FROM silver_cleansed_transactions
        WHERE quantity <= 0 OR net_revenue_usd < 0
    """).fetchone()[0]
    add_check("CAT-05", "Silver", "Positive quantity and non-negative net revenue contract", s_neg == 0, f"invalid_rows={s_neg}")

    # 3. Gold Layer Referential Integrity & Metric Conservation Checks
    orphan_acct = cur.execute("""
        SELECT COUNT(*) FROM gold_fact_sales_orders f
        LEFT JOIN gold_dim_customer_segment d ON f.account_id = d.account_id
        WHERE d.account_id IS NULL
    """).fetchone()[0]
    add_check("CAT-06", "Gold", "Zero orphan account_id foreign keys in gold_fact_sales_orders", orphan_acct == 0, f"orphans={orphan_acct}")

    orphan_prod = cur.execute("""
        SELECT COUNT(*) FROM gold_fact_sales_orders f
        LEFT JOIN gold_dim_product p ON f.product_id = p.product_id
        WHERE p.product_id IS NULL
    """).fetchone()[0]
    add_check("CAT-07", "Gold", "Zero orphan product_id foreign keys in gold_fact_sales_orders", orphan_prod == 0, f"orphans={orphan_prod}")

    rev_silver = cur.execute("SELECT ROUND(SUM(net_revenue_usd), 2) FROM silver_cleansed_transactions").fetchone()[0]
    rev_gold_fact = cur.execute("SELECT ROUND(SUM(net_revenue_usd), 2) FROM gold_fact_sales_orders").fetchone()[0]
    rev_gold_mart = cur.execute("SELECT ROUND(SUM(net_revenue_usd), 2) FROM gold_mart_strategic_account_kpis").fetchone()[0]
    parity_ok = abs(rev_silver - rev_gold_fact) < 0.01 and abs(rev_silver - rev_gold_mart) < 0.01
    add_check("CAT-08", "Gold", "100% Net Revenue conservation across Silver, Gold Fact, and Gold KPI Mart", parity_ok, f"delta=${abs(rev_silver - rev_gold_mart):.2f}")

    margin_silver = cur.execute("SELECT ROUND(SUM(gross_margin_usd), 2) FROM silver_cleansed_transactions").fetchone()[0]
    margin_gold_mart = cur.execute("SELECT ROUND(SUM(gross_margin_usd), 2) FROM gold_mart_strategic_account_kpis").fetchone()[0]
    margin_ok = abs(margin_silver - margin_gold_mart) < 0.01
    add_check("CAT-09", "Gold", "100% Gross Margin conservation across Silver and Gold KPI Mart", margin_ok, f"delta=${abs(margin_silver - margin_gold_mart):.2f}")

    # Write catalog metadata & audit CSV
    catalog_spec = {
        "catalog_name": "enterprise_commercial_bi_unity_catalog",
        "owner": "Business Intelligence Architecture",
        "schemas": {
            "bronze": ["bronze_raw_invoices"],
            "silver": ["silver_cleansed_transactions"],
            "gold": [
                "gold_dim_customer_segment",
                "gold_dim_product",
                "gold_fact_sales_orders",
                "gold_mart_strategic_account_kpis"
            ]
        },
        "certified_kpi_definitions": {
            "net_revenue_usd": "Gross invoice revenue less contractual segment discount: Gross_Revenue * (1 - discount_pct)",
            "gross_margin_usd": "Net revenue minus direct cost of goods sold (COGS)",
            "gross_margin_pct": "SUM(gross_margin_usd) * 100.0 / NULLIF(SUM(net_revenue_usd), 0)",
            "revenue_per_account_usd": "SUM(net_revenue_usd) / COUNT(DISTINCT account_id)"
        },
        "quality_checks_executed": len(checks),
        "quality_checks_passed": sum(1 for c in checks if c["status"] == "PASS"),
        "checks": checks
    }

    with open(CATALOG_JSON, "w") as f:
        json.dump(catalog_spec, f, indent=2)

    with open(AUDIT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(checks[0].keys()))
        writer.writeheader()
        writer.writerows(checks)

    conn.close()
    return catalog_spec


if __name__ == "__main__":
    res = run_catalog_governance_checks()
    print(f"Executed {res['quality_checks_executed']} governance checks; Passed: {res['quality_checks_passed']}")
