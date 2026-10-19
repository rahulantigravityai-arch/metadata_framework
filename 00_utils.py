# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Shared helpers (Phase 5 refactor)
# MAGIC Phases 2, 3 and 4 each had their own copy of the logging code. Now they all load it from here with
# MAGIC `%run ./00_utils`. One place to fix, one place to improve.

# COMMAND ----------

def make_logger(run_id, table_id, layer):
    """Returns (log_start, log_end) that write to mfw.control.ctl_run_log for this run / table / layer."""

    def log_start():
        spark.sql("""INSERT INTO mfw.control.ctl_run_log
                     VALUES (:run_id, :table_id, :layer, 'STARTED', NULL, current_timestamp(), NULL, NULL)""",
                  args={"run_id": run_id, "table_id": table_id, "layer": layer})

    def log_end(status, rows=None, error=None):
        spark.sql("""UPDATE mfw.control.ctl_run_log
                     SET status = :status, rows_processed = :rows, end_time = current_timestamp(), error_message = :error
                     WHERE run_id = :run_id AND table_id = :table_id AND layer = :layer""",
                  args={"status": status, "rows": rows, "error": error,
                        "run_id": run_id, "table_id": table_id, "layer": layer})

    return log_start, log_end