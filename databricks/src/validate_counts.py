# Databricks notebook source
# MAGIC %md
# MAGIC # Validate -- 124 / 124 / 124
# MAGIC
# MAGIC Reconciles the DLT-produced tables, the Databricks equivalent of the local Trino checks.
# MAGIC There is no DLQ table here: in the DLT pipeline the data-quality rules are expectations,
# MAGIC so dropped rows are tracked in the pipeline's event log / UI rather than a side table.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "analytics"
BRONZE = f"{CATALOG}.{SCHEMA}.payments_bronze"
SILVER = f"{CATALOG}.{SCHEMA}.payments_silver"
GOLD = f"{CATALOG}.{SCHEMA}.payment_metrics_gold"
EXPECTED = 124

bronze = spark.table(BRONZE).count()
silver = spark.table(SILVER).count()
g = spark.sql(f"SELECT count(*) AS rows, coalesce(sum(payment_count), 0) AS total FROM {GOLD}").collect()[0]
print(f"bronze={bronze} silver={silver} gold_rows={g['rows']} gold_total={g['total']}")

errors = []
if bronze != EXPECTED:
    errors.append(f"bronze {bronze} != {EXPECTED}")
if silver != EXPECTED:
    errors.append(f"silver {silver} != {EXPECTED}")
if g["total"] != EXPECTED:
    errors.append(f"gold sum(payment_count) {g['total']} != {EXPECTED}")
if errors:
    raise SystemExit("Validation failed:\n  " + "\n  ".join(errors))

print("Validation passed: 124 -> 124 -> 124")
