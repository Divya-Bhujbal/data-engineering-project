# Metadata-Driven Lakehouse with a Data Quality Framework

A config-driven Bronze → Silver → Gold pipeline for Databricks (Delta Lake + PySpark).
**Onboard a new source by adding one YAML file — no pipeline code.** Bad records are quarantined with the reasons they
failed, every run writes per-rule quality metrics, and a quality gate stops a bad batch from reaching Silver.

```
landing CSVs (Unity Catalog Volume)
   │  incremental: only files not yet processed successfully
   ▼
BRONZE  raw strings + lineage (_run_id, _source_file, _ingest_ts)          append-only
   ▼  map columns → safe-cast types → evaluate rules
SILVER  validated, typed, deduplicated, MERGE (upsert on primary key)
   │         └─► QUARANTINE  original raw record (JSON) + failed rules + warnings
   │         └─► DQ_METRICS  per run × rule: failed count, pass rate
   │         └─► DQ_RUN_SUMMARY  input / valid / quarantined / error rate / status
   ▼  SQL defined in configs/project.yaml
GOLD    business tables (daily_sales, sales_by_category, customers_by_country)
```

## Features
| Feature | How it works |
|---|---|
| Config-driven onboarding | `configs/sources/<name>.yaml`: columns + type mapping, primary key, rules, quality gate |
| Rule types | `not_null`, `unique`, `regex`, `range`, `allowed_values`, `referential`, `expression` (SQL) |
| Severity | `error` → record quarantined; `warn` → kept in Silver, flagged and counted |
| Safe casting | `try_cast` / `try_to_timestamp`: a bad value becomes a `*_valid_type` failure, never a job crash |
| Duplicate handling | first occurrence kept, later duplicates quarantined as `duplicate_key` |
| Referential checks | validated against another source's Silver table; sources load in dependency order |
| Incremental ingestion | files already loaded with status `SUCCESS` are skipped; failed batches are retried |
| Quality gate | if error rate > `max_error_rate`, Silver is not updated and the run fails loudly |
| Observability | `dq_metrics`, `dq_run_summary`, quarantine tables → `sql/dashboard_queries.sql` |
| Config validation | bad configs fail fast with every problem listed (`tests/test_config.py`) |

## Run it on Databricks Free Edition
1. Create a workspace at databricks.com (Free Edition). Check your catalog name in **Catalog Explorer**
   (usually `workspace`); if different, edit `configs/project.yaml` (catalog and `landing_root`).
2. Upload this project folder to your workspace (Workspace → your user folder → Import, or use a Git folder).
3. Generate sample data on your laptop: `python scripts/generate_sample_data.py --out data/landing`
4. Run the first cell of `notebooks/01_run_pipeline.py` to create the schema and Volume, then upload
   `customers/`, `products/`, `orders/` folders into the Volume (Catalog → your schema → `landing` → Upload).
5. Set the `repo_path` widget to your project folder and **Run all**.
6. Create a dashboard from `sql/dashboard_queries.sql` (AI/BI dashboard).
7. Demo incremental loading: `python scripts/generate_sample_data.py --out data/landing --batch 2`,
   upload `orders_2.csv` to the `orders` folder, run again. Only the new file is processed.
8. Schedule `01_run_pipeline` as a Job (Workflows) to show orchestration.

## Onboard a new source (the interview demo)
Create `configs/sources/shipments.yaml`:
```yaml
source: shipments
landing_subdir: shipments
primary_key: [shipment_id]
columns:
  - {name: shipment_id, source: ShipID, type: string}
  - {name: weight_kg,   source: Weight, type: "decimal(8,2)"}
rules:
  - {name: weight_positive, type: range, column: weight_kg, min: 0.01}
```
Drop CSVs in the Volume folder and rerun. Bronze, Silver, quarantine and metrics tables appear automatically.

## Known limitations / next steps
- Batch only: Databricks Free uses serverless compute, which doesn't support time-based streaming triggers.
  Next step: Auto Loader + `trigger(availableNow=True)` for file-arrival ingestion.
- Silver MERGE assumes the schema is stable; adding a column to a config needs a schema migration.
- A gate-failed batch is re-ingested into Bronze on retry, so Bronze can hold duplicate raw rows
  (distinguishable by `_run_id`).
- Unit tests cover config validation only. Add Spark tests with `pytest` + a local SparkSession.
- Possible additions: SCD Type 2 for customers, Great Expectations comparison, Terraform / Asset Bundles deploy.

## Resume bullets (fill in your real numbers after running)
- Built a metadata-driven lakehouse on Databricks (PySpark, Delta Lake): new sources onboarded through YAML configs
  with automatic Bronze/Silver/Gold tables, schema mapping and safe type casting.
- Designed a data quality framework with 7 rule types, quarantine tables, per-rule run metrics and a quality gate that
  blocks bad batches; surfaced in a dashboard.
- Implemented incremental, idempotent ingestion (processed-file tracking, MERGE upserts, duplicate and referential checks).
