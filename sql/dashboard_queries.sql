-- Replace workspace.lakehouse with your catalog.schema. Create each as a dataset in a Databricks AI/BI dashboard.

-- 1) Run overview: how much data passed / was quarantined per run
SELECT run_ts, source, input_records, valid_records, quarantined_records, warned_records,
       ROUND(error_rate * 100, 2) AS error_rate_pct, status
FROM workspace.lakehouse.dq_run_summary
ORDER BY run_ts DESC;

-- 2) Pass rate per rule (latest run per source)
WITH latest AS (
  SELECT source, MAX(run_ts) AS run_ts FROM workspace.lakehouse.dq_metrics GROUP BY source
)
SELECT m.source, m.rule_name, m.rule_type, m.severity, m.failed_records,
       ROUND(m.pass_rate * 100, 2) AS pass_rate_pct
FROM workspace.lakehouse.dq_metrics m
JOIN latest l ON m.source = l.source AND m.run_ts = l.run_ts
ORDER BY m.pass_rate ASC;

-- 3) Pass-rate trend over runs
SELECT run_ts, source, rule_name, ROUND(pass_rate * 100, 2) AS pass_rate_pct
FROM workspace.lakehouse.dq_metrics
ORDER BY run_ts;

-- 4) Top rejection reasons across quarantined records
SELECT source, reason, COUNT(*) AS records
FROM (
  SELECT source, explode(failed_rules) AS reason
  FROM (SELECT source, failed_rules FROM workspace.lakehouse.quarantine_orders
        UNION ALL SELECT source, failed_rules FROM workspace.lakehouse.quarantine_customers
        UNION ALL SELECT source, failed_rules FROM workspace.lakehouse.quarantine_products)
)
GROUP BY source, reason
ORDER BY records DESC;

-- 5) Business view (gold)
SELECT * FROM workspace.lakehouse.gold_daily_sales ORDER BY order_date;
SELECT * FROM workspace.lakehouse.gold_sales_by_category ORDER BY revenue DESC;
