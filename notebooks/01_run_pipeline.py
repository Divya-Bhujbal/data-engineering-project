# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata-driven Lakehouse: run all sources
# MAGIC Reads every config in `configs/sources`, loads new files Bronze -> Silver (with quarantine + metrics), then rebuilds Gold.
# MAGIC
# MAGIC **Before running:** set `repo_path` below to the folder where you uploaded this project, and upload the sample CSVs
# MAGIC to your Volume (see README).

# COMMAND ----------

dbutils.widgets.text("repo_path", "/Workspace/Users/<your-email>/lakehouse", "Path to project folder")
repo_path = dbutils.widgets.get("repo_path")

import sys
sys.path.insert(0, f"{repo_path}/src")

from lakehouse.config import load_sources, load_project
from lakehouse.pipeline import run_all

project = load_project(f"{repo_path}/configs/project.yaml")
sources = load_sources(f"{repo_path}/configs/sources")
print("Sources (dependency order):", [s["source"] for s in sources])

# COMMAND ----------

# Make sure the landing Volume exists (first run only)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {project['catalog']}.{project['schema']}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {project['catalog']}.{project['schema']}.landing")

# COMMAND ----------

results = run_all(spark, project, sources)
for r in results:
    print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect results

# COMMAND ----------

display(spark.table(f"{project['catalog']}.{project['schema']}.dq_run_summary").orderBy("run_ts", ascending=False))

# COMMAND ----------

display(spark.table(f"{project['catalog']}.{project['schema']}.quarantine_orders").limit(50))
