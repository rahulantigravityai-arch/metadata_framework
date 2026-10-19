# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 – Generic Bronze -> Silver
# MAGIC Same notebook for orders, customers, products (and any table you add later).
# MAGIC Bronze = everything that ever arrived. Silver = ONE current row per primary key.

# COMMAND ----------

import uuid
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

dbutils.widgets.text("table_id", "1")
dbutils.widgets.text("run_id", "")
table_id = int(dbutils.widgets.get("table_id"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

cfg = spark.table("mfw.control.ctl_source_config").filter(f"table_id = {table_id}").collect()
assert len(cfg) == 1, f"expected exactly 1 config row for table_id={table_id}, got {len(cfg)}"
cfg = cfg[0]
assert cfg.is_active, f"table {cfg.table_name} is switched off in config"
assert cfg.load_type == "merge", f"only 'merge' is implemented, got {cfg.load_type}"

pks = [c.strip() for c in cfg.primary_key.split(",")]       # supports composite keys: "a, b"
print(f"run_id={run_id}")
print(f"{cfg.bronze_table} -> {cfg.silver_table}   key={pks}")

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

# Logging helpers now live in 00_utils (shared by every notebook)
log_start, log_end = make_logger(run_id, table_id, "silver")

# COMMAND ----------

log_start()
try:
    bronze = spark.table(cfg.bronze_table)

    # 1) DEDUPE: keep only the latest version of each key.
    #    "Latest" = highest watermark column (if the table has one), then most recently ingested file/batch.
    order_cols = []
    if cfg.watermark_column:
        order_cols.append(F.col(cfg.watermark_column).desc_nulls_last())
    for c in ["_ingested_at", "_source_file"]:
        if c in bronze.columns:
            order_cols.append(F.col(c).desc())

    w = Window.partitionBy(*pks).orderBy(*order_cols)
    latest = bronze.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # 2) MINIMAL CLEANING (the same rules for every table, because the code is generic)
    for f in latest.schema.fields:
        if f.dataType.simpleString() == "string" and not f.name.startswith("_"):
            latest = latest.withColumn(f.name, F.trim(F.col(f.name)))          # trim text columns
    latest = latest.filter(" AND ".join(f"`{c}` IS NOT NULL" for c in pks))   # a row with no key is unusable
    latest = latest.drop("_run_id").withColumn("_silver_updated_at", F.current_timestamp())

    # 3) WRITE: first run creates the table, later runs MERGE (update if key exists, insert if not)
    if not spark.catalog.tableExists(cfg.silver_table):
        latest.write.saveAsTable(cfg.silver_table)
        rows = spark.table(cfg.silver_table).count()
        print(f"created {cfg.silver_table} with {rows} rows")
    else:
        target = DeltaTable.forName(spark, cfg.silver_table)
        cond = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pks)
        (target.alias("t")
               .merge(latest.alias("s"), cond)
               .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")   # only if Bronze has a NEWER version
               .whenNotMatchedInsertAll()
               .execute())
        m = target.history(1).select("operationMetrics").collect()[0][0]
        inserted = int(m.get("numTargetRowsInserted", 0))
        updated = int(m.get("numTargetRowsUpdated", 0))
        rows = inserted + updated
        print(f"merge done: {inserted} inserted, {updated} updated")

    log_end("SUCCESS", rows=rows)
except Exception as e:
    log_end("FAILED", error=str(e)[:2000])
    raise

# COMMAND ----------

# Checks
b = spark.table(cfg.bronze_table).count()
s = spark.table(cfg.silver_table).count()
print(f"Bronze rows: {b}   Silver rows: {s}")

dupes = spark.sql(f"SELECT {', '.join(pks)}, count(*) AS n FROM {cfg.silver_table} GROUP BY {', '.join(pks)} HAVING count(*) > 1")
print("duplicate keys in Silver (should be 0):", dupes.count())

display(spark.sql("SELECT * FROM mfw.control.ctl_run_log WHERE layer = 'silver' ORDER BY start_time DESC LIMIT 10"))
