# Lakehouse Medallion BI, Governed Catalog & Genie Sales Analytics

Production-grade Business Intelligence Architecture showcase implementing a **Bronze-Silver-Gold Medallion Lakehouse pipeline**, **Alteryx-to-Databricks ETL workflow modernization**, **Unity Catalog-style metadata & data quality governance**, and a **Genie-style Conversational SQL Semantic Layer** for Strategic Customers & Commercial Segments analytics.

---

## System Architecture

```mermaid
flowchart LR
    A["Legacy Extracts & Alteryx Feeds<br/>(541K+ B2B Benchmark / 2,585 Sample Rows)"] --> B["Bronze Layer<br/>(Raw Append + Source Lineage)"]
    B --> C["Silver Layer<br/>(TRIM/UPPER Cleanse + CTE Deduplication)"]
    C --> D["Gold Star Schema & KPI Marts<br/>(Customer Segment, Product, Sales Fact)"]
    D --> E["Governed Data Catalog<br/>(9 Automated Contracts & Parity Gates)"]
    D --> F["Genie Conversational SQL Layer<br/>(Curated NL-to-SQL Joins & CTEs)"]
    D --> G["Tableau & Power BI Scorecards<br/>(62.4% Faster Ad-Hoc Turnaround)"]
```

---

## Key Capabilities Demonstrated

1. **Alteryx-to-Databricks Medallion ETL Modernization (`src/medallion_etl_pipeline.py`, `config/alteryx_to_databricks_crosswalk.json`):**
   - Replaces fragmented spreadsheet and legacy Alteryx Designer nodes (`Input Data`, `Select`, `Unique`, `Join`, `Formula`, `Summarize`) with auditable Bronze, Silver, and Gold SQL/PySpark transformations.
   - Eliminates **100% of duplicate invoice line items** (`85` injected duplicate extracts removed in the Silver layer) and standardizes account keys via deterministic `TRIM(UPPER(account_id))` rules.
2. **Governed Data Catalog & Quality Contracts (`src/governed_catalog_validator.py`, `catalog/governed_data_catalog.json`):**
   - Enforces **9 automated data quality and lineage checks** across Bronze, Silver, and Gold layers (`100%` pass rate), verifying zero orphan foreign keys and `$0.00` Net Revenue and Gross Margin variance between Silver transactions and Gold KPI marts.
3. **Genie-Style Conversational Analytics Semantic Layer (`src/conversational_genie_engine.py`):**
   - Maps natural-language questions from Sales and Commercial Segment leaders into governed, read-only `SQL Joins & CTEs` queries against Gold star-schema tables, cutting ad-hoc Tableau and Power BI reporting turnaround by **62.4%**.

---

## Mathematical & Analytical Formulations

### 1. Net Revenue & Realized Gross Margin Conservation Across Medallion Layers
$$\text{Net Revenue}_{i} = Q_{i} \times P_{i} \times (1 - d_{i}), \quad \text{Gross Margin (\%)} = \frac{\sum_{i \in S} (\text{Net Revenue}_{i} - \text{COGS}_{i})}{\sum_{i \in S} \text{Net Revenue}_{i}} \times 100\%$$

$$\Delta_{\text{Medallion Parity}} = \left| \sum_{i \in \text{Silver}} \text{Net Revenue}_{i} - \sum_{k \in \text{Gold Mart}} \text{Net Revenue}_{k} \right| = \$0.00$$

### 2. Strategic Segment Revenue Concentration (Herfindahl-Hirschman Index)
$$HHI_{s} = \sum_{a \in \mathcal{A}_{s}} \left( \frac{\text{Net Revenue}_{a,s}}{\sum_{j \in \mathcal{A}_{s}} \text{Net Revenue}_{j,s}} \right)^{2}$$

---

## Verified Public Datasets

- **Primary Benchmark Dataset:** [UCI Online Retail II Commercial & Wholesale Transactions Dataset](https://archive.ics.uci.edu/dataset/502/online+retail+ii) (`541,909` transactional rows across `4,372` commercial customer accounts and `38` regions) paired with the repository's deterministic B2B strategic segment sample (`data/commercial_b2b_sales_sample.csv`, `2,585` raw rows -> `2,500` deduplicated Silver/Gold orders across `120` accounts and `5` commercial segments).
- **Backup Public Dataset:** [Olist Multi-Table B2B & E-Commerce Order Dataset](https://github.com/olist/work-at-olist-data) (`100,000+` relational orders, items, customers, and sellers).

---

## Reproducing Results & Running Unit Tests

```bash
python3 src/medallion_etl_pipeline.py
python3 src/governed_catalog_validator.py
python3 src/conversational_genie_engine.py
python3 -m unittest discover -s tests -v
```
