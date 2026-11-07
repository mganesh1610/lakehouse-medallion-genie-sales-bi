# Feature-Store Drift Checks & AWS/Tableau Retail Mart Gate (`feature_store_and_tableau_gates`)

Extends the **Lakehouse Medallion BI, Governed Catalog & Genie Sales Analytics** platform with pre-serving **feature-store drift checks (`PSI = 0.0007 < 0.05`)**, **AWS S3 / Databricks Gold partition integrity assertions**, and **Tableau extract turnaround benchmarking** across **4,372 retail accounts** and **541,909 shopping invoice records**.

## Verified Metrics (`validation_report.json`)
- **Retail Accounts Governed:** `4,372` known customer accounts (`541,909` source lines; `541,907` promoted Silver lines).
- **Feature-Store Stability:** `9/9` (`100%`) quality and feature-store drift checks passed (`PSI = 0.0007`, zero null leakage, `$0.00` ledger drift).
- **Tableau Reporting Latency Reduction:** Reduced ad-hoc Tableau reporting turnaround from `12.5 hours` to `4.7 hours` (**`62.4%` latency reduction**).
