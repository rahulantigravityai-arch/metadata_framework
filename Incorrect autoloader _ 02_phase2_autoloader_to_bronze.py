# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Phase 2 – Generic Auto Loader: files -> Bronze
# MAGIC ONE notebook for ANY file-based table. It knows nothing about "orders".
# MAGIC Everything comes from the config row selected by `table_id`.

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql import functions as F

# 1) Which table are we loading? (Phase 5 will pass this in automatically from a loop)
dbutils.widgets.text("table_id", "1")
dbutils.widgets.text("run_id", "")                      # empty = make a new one
table_id = int(dbutils.widgets.get("table_id"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

# COMMAND ----------

# 2) Read the config row for this table
cfg = (spark.table("mfw.control.ctl_source_config")
            .filter(f"table_id = {table_id}")
            .collect())
assert len(cfg) == 1, f"expected exactly 1 config row for table_id={table_id}, got {len(cfg)}"
cfg = cfg[0]
assert cfg.is_active, f"table {cfg.table_name} is switched off in config"
assert cfg.source_type == "autoloader", f"this notebook only handles autoloader, got {cfg.source_type}"

# Where Auto Loader keeps its memory. Kept OUTSIDE the data folder on purpose.
base = "/Volumes/mfw/control/landing"
checkpoint_path = f"{base}/_checkpoints/{cfg.table_name}"   # remembers which files were already read
schema_path     = f"{base}/_schemas/{cfg.table_name}"       # remembers the inferred schema

print(f"run_id={run_id}")
print(f"{cfg.source_location}  ->  {cfg.bronze_table}")

# COMMAND ----------

# 3) Helpers to write into ctl_run_log (named parameters = safe, no string gluing)
def log_start():
    spark.sql("""INSERT INTO mfw.control.ctl_run_log
                 VALUES (:run_id, :table_id, 'bronze', 'STARTED', NULL, current_timestamp(), NULL, NULL)""",
              args={"run_id": run_id, "table_id": table_id})

def log_end(status, rows=None, error=None):
    spark.sql("""UPDATE mfw.control.ctl_run_log
                 SET status = :status, rows_processed = :rows, end_time = current_timestamp(), error_message = :error
                 WHERE run_id = :run_id AND table_id = :table_id AND layer = 'bronze'""",
              args={"status": status, "rows": rows, "error": error, "run_id": run_id, "table_id": table_id})

# COMMAND ----------

# 4) The actual load
log_start()
try:
    df = (spark.readStream
            .format("cloudFiles")                                   # <- this is Auto Loader
            .option("cloudFiles.format", cfg.source_format)         # csv, from config
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(cfg.source_location)                              # folder, from config
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))

    query = (df.writeStream
               .option("checkpointLocation", checkpoint_path)
               .trigger(availableNow=True)                          # process what's new, then stop
               .toTable(cfg.bronze_table))
    query.awaitTermination()

    progress = query.lastProgress or {}
    rows = progress.get("numInputRows", 0)
    log_end("SUCCESS", rows=rows)
    print(f"SUCCESS: {rows} new rows loaded into {cfg.bronze_table}")
except Exception as e:
    log_end("FAILED", error=str(e)[:2000])
    raise

# COMMAND ----------

# 5) Look at the results
display(spark.sql(f"SELECT count(*) AS rows_in_bronze FROM {cfg.bronze_table}"))
display(spark.sql(f"SELECT * FROM {cfg.bronze_table} ORDER BY order_id LIMIT 5"))
display(spark.sql("SELECT * FROM mfw.control.ctl_run_log ORDER BY start_time DESC"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, status, _source_file FROM mfw.bronze.orders
# MAGIC WHERE order_id = 1005 OR order_id = 1016 ORDER BY order_id;