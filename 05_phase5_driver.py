# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Phase 5 – The driver
# MAGIC Reads the config, and for every active table runs: Bronze notebook (chosen by source_type) -> Silver notebook.
# MAGIC One failing table never stops the others. Everything shares ONE run_id, so the log tells the whole story.

# COMMAND ----------

import uuid

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("only_table_id", "")        # blank = every active table; a number = just that table
dbutils.widgets.text("pg_host", "aws-0-<region>.pooler.supabase.com")
dbutils.widgets.text("pg_port", "5432")
dbutils.widgets.text("pg_db", "postgres")
dbutils.widgets.text("pg_user", "postgres.<project-ref>")
dbutils.widgets.text("pg_password", "")

run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
only = dbutils.widgets.get("only_table_id").strip()
print("run_id =", run_id)

# COMMAND ----------

# 1) Which tables should run?  (the config decides, not this code)
cfgs = spark.table("mfw.control.ctl_source_config").filter("is_active = true")
if only:
    cfgs = cfgs.filter(f"table_id = {int(only)}")
cfgs = cfgs.orderBy("table_id").collect()
print("tables to run:", [c.table_name for c in cfgs])

# 2) Which notebook handles which source type?  (this small map is the only "routing" in the framework)
BRONZE_NOTEBOOK = {
    "autoloader": "./02_phase2_autoloader_to_bronze",
    "jdbc":       "./03_phase3_jdbc_incremental_to_bronze",
}
SILVER_NOTEBOOK = "./04_phase4_bronze_to_silver"
TIMEOUT_SECONDS = 1800

# COMMAND ----------

# 3) Loop. Each notebook logs its own STARTED/SUCCESS/FAILED to ctl_run_log; the driver just keeps going.
results = []
for c in cfgs:
    if c.source_type not in BRONZE_NOTEBOOK:
        results.append((c.table_id, c.table_name, "SKIPPED", f"unknown source_type {c.source_type}"))
        continue

    args = {"table_id": str(c.table_id), "run_id": run_id}
    bronze_args = dict(args)
    if c.source_type == "jdbc":
        for k in ["pg_host", "pg_port", "pg_db", "pg_user", "pg_password"]:
            bronze_args[k] = dbutils.widgets.get(k)

    try:
        print(f"--- {c.table_name}: bronze")
        dbutils.notebook.run(BRONZE_NOTEBOOK[c.source_type], TIMEOUT_SECONDS, bronze_args)
        print(f"--- {c.table_name}: silver")
        dbutils.notebook.run(SILVER_NOTEBOOK, TIMEOUT_SECONDS, args)
        results.append((c.table_id, c.table_name, "OK", ""))
    except Exception as e:
        # bronze failed -> silver is skipped for this table, and we move on to the next table
        results.append((c.table_id, c.table_name, "FAILED", str(e)[:300]))

# COMMAND ----------

# 4) Report
display(spark.createDataFrame(results, "table_id INT, table_name STRING, outcome STRING, detail STRING"))
display(spark.sql("""SELECT table_id, layer, status, rows_processed, start_time, end_time, error_message
                     FROM mfw.control.ctl_run_log WHERE run_id = :r ORDER BY table_id, layer""", args={"r": run_id}))

failed = [r for r in results if r[2] != "OK"]
if failed:
    # Make the whole run RED so a scheduler / alert notices, but only AFTER every table had its turn
    raise Exception(f"{len(failed)} table(s) did not complete: {[(r[1], r[2]) for r in failed]}")