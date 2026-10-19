# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Phase 1 – The three control tables
# MAGIC Import this file into Databricks (Workspace -> Import) or paste each cell into a new notebook.
# MAGIC Safe to re-run: it drops and recreates the control tables.

# COMMAND ----------

# 1) ctl_source_config : ONE ROW PER TABLE WE WANT TO LOAD. This is the "brain".
spark.sql("DROP TABLE IF EXISTS mfw.control.ctl_source_config")
spark.sql("""
CREATE TABLE mfw.control.ctl_source_config (
    table_id         INT      COMMENT 'unique id of the table we load',
    table_name       STRING   COMMENT 'logical name, e.g. orders',
    source_type      STRING   COMMENT 'autoloader (files) or jdbc (database)',
    source_location  STRING   COMMENT 'files: folder path | jdbc: schema.table in the source DB',
    source_format    STRING   COMMENT 'csv/json/... (files only)',
    bronze_table     STRING   COMMENT 'where raw data lands',
    silver_table     STRING   COMMENT 'where clean data lands',
    primary_key      STRING   COMMENT 'column that identifies a row (used to dedupe / merge)',
    watermark_column STRING   COMMENT 'jdbc only: column used to find new/changed rows',
    load_type        STRING   COMMENT 'append or merge (silver step)',
    is_active        BOOLEAN  COMMENT 'false = skip this table',
    notes            STRING
) USING DELTA
""")

# COMMAND ----------

# 2) ctl_watermark : "how far did we get last time?" (only needed for jdbc tables)
spark.sql("DROP TABLE IF EXISTS mfw.control.ctl_watermark")
spark.sql("""
CREATE TABLE mfw.control.ctl_watermark (
    table_id    INT,
    last_value  STRING    COMMENT 'highest watermark value already loaded',
    updated_at  TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# 3) ctl_run_log : "what happened?" one row per table per layer per run
spark.sql("DROP TABLE IF EXISTS mfw.control.ctl_run_log")
spark.sql("""
CREATE TABLE mfw.control.ctl_run_log (
    run_id          STRING,
    table_id        INT,
    layer           STRING     COMMENT 'bronze or silver',
    status          STRING     COMMENT 'STARTED / SUCCESS / FAILED',
    rows_processed  BIGINT,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    error_message   STRING
) USING DELTA
""")

# COMMAND ----------

# 4) Fill the config: 3 tables = 3 rows. THIS is all "adding a table" will ever mean.
spark.sql("""
INSERT INTO mfw.control.ctl_source_config VALUES
 (1, 'orders',    'autoloader', '/Volumes/mfw/control/landing/orders/', 'csv',
     'mfw.bronze.orders',    'mfw.silver.orders',    'order_id',    NULL,         'merge', true,
     'later switch to s3://sftp-test-data/landing/orders/ by updating source_location'),
 (2, 'customers', 'jdbc',       'public.customers', NULL,
     'mfw.bronze.customers', 'mfw.silver.customers', 'customer_id', 'updated_at', 'merge', true, NULL),
 (3, 'products',  'jdbc',       'public.products',  NULL,
     'mfw.bronze.products',  'mfw.silver.products',  'product_id',  'updated_at', 'merge', true, NULL)
""")

# jdbc tables start from the beginning of time, so the first run loads everything
spark.sql("""
INSERT INTO mfw.control.ctl_watermark VALUES
 (2, '1900-01-01 00:00:00', current_timestamp()),
 (3, '1900-01-01 00:00:00', current_timestamp())
""")

# COMMAND ----------

# 5) Look at what we built
display(spark.table("mfw.control.ctl_source_config"))
display(spark.table("mfw.control.ctl_watermark"))
display(spark.table("mfw.control.ctl_run_log"))   # empty for now - Phase 2 starts filling it

# COMMAND ----------

# 6) The first "framework" move: read the config like a program would.
active = spark.table("mfw.control.ctl_source_config").filter("is_active = true").collect()
for r in active:
    print(f"table {r.table_id}: {r.table_name:10s} via {r.source_type:10s} -> {r.bronze_table} -> {r.silver_table}")