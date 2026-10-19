# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 – Generic JDBC incremental load: database -> Bronze
# MAGIC ONE notebook for ANY database table listed in the config.
# MAGIC Idea: ask the source only for rows where `watermark_column > last watermark we stored`.

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

dbutils.widgets.text("table_id", "2")
dbutils.widgets.text("run_id", "")
# Connection details. Host/port/db/user are not secret. Password: secret if you have one, else the widget.
dbutils.widgets.text("pg_host", "aws-0-<region>.pooler.supabase.com")
dbutils.widgets.text("pg_port", "5432")
dbutils.widgets.text("pg_db", "postgres")
dbutils.widgets.text("pg_user", "postgres.<project-ref>")
dbutils.widgets.text("pg_password", "")

table_id = int(dbutils.widgets.get("table_id"))
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

def get_password():
    try:                                         # preferred: a Databricks secret (scope "mfw", key "pg_password")
        return dbutils.secrets.get("mfw", "pg_password")
    except Exception:
        return dbutils.widgets.get("pg_password")   # learning fallback; don't share the notebook with this filled in

jdbc_url = (f"jdbc:postgresql://{dbutils.widgets.get('pg_host')}:{dbutils.widgets.get('pg_port')}"
            f"/{dbutils.widgets.get('pg_db')}?sslmode=require")
jdbc_props = {"user": dbutils.widgets.get("pg_user"), "password": get_password(), "driver": "org.postgresql.Driver"}

# COMMAND ----------

# 1) Read the config row for this table
cfg = spark.table("mfw.control.ctl_source_config").filter(f"table_id = {table_id}").collect()
assert len(cfg) == 1, f"expected exactly 1 config row for table_id={table_id}, got {len(cfg)}"
cfg = cfg[0]
assert cfg.is_active, f"table {cfg.table_name} is switched off in config"
assert cfg.source_type == "jdbc", f"this notebook only handles jdbc, got {cfg.source_type}"
assert cfg.watermark_column, "jdbc tables need a watermark_column in config"

# 2) Read where we stopped last time
wm_rows = spark.table("mfw.control.ctl_watermark").filter(f"table_id = {table_id}").collect()
assert len(wm_rows) == 1, f"no watermark row for table_id={table_id} (add one with '1900-01-01 00:00:00')"
last_wm = wm_rows[0].last_value
print(f"run_id={run_id}")
print(f"{cfg.source_location} -> {cfg.bronze_table}   (rows with {cfg.watermark_column} > {last_wm})")

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

# 3) Logging helpers now live in 00_utils (shared by every notebook)
log_start, log_end = make_logger(run_id, table_id, "bronze")

# COMMAND ----------

# 4) The load
log_start()
try:
    # The subquery runs INSIDE Postgres, so only new/changed rows travel over the network.
    query = f"""(SELECT * FROM {cfg.source_location}
                 WHERE {cfg.watermark_column} > '{last_wm}'::timestamp) AS src"""

    df = (spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_props)
               .withColumn("_run_id", F.lit(run_id))
               .withColumn("_ingested_at", F.current_timestamp()))

    df.write.mode("append").saveAsTable(cfg.bronze_table)          # Bronze = append only

    # Measure what we actually wrote (reading our own Bronze rows tagged with this run_id,
    # so rows inserted in Postgres mid-run can't sneak in and corrupt the watermark).
    stats = (spark.table(cfg.bronze_table).filter(F.col("_run_id") == run_id)
                  .agg(F.count("*").alias("n"),
                       F.max(cfg.watermark_column).cast("string").alias("max_wm"))
                  .collect()[0])
    rows, new_wm = stats["n"], stats["max_wm"]

    # Move the watermark ONLY after the write succeeded, and only if we got rows.
    if rows > 0:
        spark.sql("""UPDATE mfw.control.ctl_watermark
                     SET last_value = :v, updated_at = current_timestamp()
                     WHERE table_id = :t""", args={"v": new_wm, "t": table_id})

    log_end("SUCCESS", rows=rows)
    print(f"SUCCESS: {rows} rows loaded into {cfg.bronze_table}; watermark {last_wm} -> {new_wm if rows > 0 else last_wm}")
except Exception as e:
    log_end("FAILED", error=str(e)[:2000])
    raise

# COMMAND ----------

# 5) Look at the results
display(spark.sql(f"SELECT count(*) AS rows_in_bronze FROM {cfg.bronze_table}"))
display(spark.table("mfw.control.ctl_watermark").orderBy("table_id"))
display(spark.sql("SELECT * FROM mfw.control.ctl_run_log ORDER BY start_time DESC LIMIT 10"))
