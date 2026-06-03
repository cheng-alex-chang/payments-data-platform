# Databricks notebook source
# MAGIC %md
# MAGIC # Payments medallion pipeline (Databricks Free Edition)
# MAGIC
# MAGIC Self-contained serverless notebook: setup -> seed -> bronze -> silver -> gold -> validate.
# MAGIC
# MAGIC It is a single notebook on purpose: Free Edition serverless intermittently fails to read
# MAGIC sibling workspace `.py` files via FUSE (`OSError [Errno 5]`), which breaks `import` and
# MAGIC `spark_python_task`. The notebook source itself is delivered by the notebook service, so
# MAGIC keeping everything in one notebook sidesteps that. The pure-Python helpers below mirror
# MAGIC `databricks/src/common.py`, which is unit-tested in `tests/test_databricks_helpers.py`.
# MAGIC
# MAGIC The transform contract matches the local Spark jobs in `config/spark/jobs/*`: the seed
# MAGIC produces Debezium `op='r'` envelopes so the Silver/Gold logic is a 1:1 port. Only the
# MAGIC infrastructure differs (Unity Catalog + Delta + Auto Loader + Volumes vs Iceberg/HDFS/Kafka).

# COMMAND ----------

# MAGIC %md ## Constants & helpers (mirror of common.py)

# COMMAND ----------

import hashlib
import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce, col, count, current_timestamp, date_trunc, from_unixtime,
    get_json_object, lit, lower, regexp_replace, row_number, sum as spark_sum,
    trim, udf, upper, when,
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

CATALOG, SCHEMA, VOLUME = "workspace", "analytics", "landing"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.payments_bronze"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.payments_silver"
DLQ_TABLE = f"{CATALOG}.{SCHEMA}.payments_silver_dlq"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.payment_metrics_gold"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SEED_DIR = f"{VOLUME_ROOT}/payments"
CHECKPOINT_ROOT = f"{VOLUME_ROOT}/_checkpoints"
SCHEMA_ROOT = f"{VOLUME_ROOT}/_schemas"

CDC_TOPIC = "cdc.public.payments"
PII_FIELDS = {"shopper_id"}
EXPECTED_SEED_COUNT = 124

ALLOWED_PAYMENT_METHODS = ("apple_pay", "bank_transfer", "card", "google_pay", "paypal")
ALLOWED_PAYMENT_STATUSES = ("authorized", "cancelled", "chargeback", "failed", "pending", "refunded")

spark = SparkSession.builder.getOrCreate()


def mask_pii_fields(value):
    if value is None:
        return None
    try:
        envelope = json.loads(value)
        for section in ("before", "after"):
            if isinstance(envelope.get(section), dict):
                for field in PII_FIELDS:
                    if field in envelope[section] and envelope[section][field] is not None:
                        raw = str(envelope[section][field]).encode()
                        envelope[section][field] = hashlib.sha256(raw).hexdigest()
        return json.dumps(envelope)
    except (json.JSONDecodeError, TypeError):
        return value


def _micros(moment):
    return int(moment.timestamp() * 1_000_000)


def _payment(payment_id, merchant_id, shopper_id, amount, currency, payment_method,
             payment_status, country_code, created_at, updated_at):
    return {
        "payment_id": payment_id, "merchant_id": merchant_id, "shopper_id": shopper_id,
        "amount": amount, "currency": currency, "payment_method": payment_method,
        "payment_status": payment_status, "country_code": country_code,
        "created_at": created_at, "updated_at": updated_at,
    }


def generate_seed_payments(now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    payments = [
        _payment(1001, 1, 501, 149.99, "EUR", "card", "authorized", "NL",
                 now - timedelta(days=2), now - timedelta(days=2)),
        _payment(1002, 2, 502, 499.50, "USD", "paypal", "failed", "US",
                 now - timedelta(days=1), now - timedelta(days=1)),
        _payment(1003, 1, 503, 89.00, "EUR", "card", "authorized", "BE",
                 now - timedelta(hours=12), now - timedelta(hours=12)),
        _payment(1004, 3, 504, 44.25, "EUR", "card", "refunded", "DE",
                 now - timedelta(hours=6), now - timedelta(hours=2)),
    ]
    currencies = ("EUR", "USD", "GBP", "CAD")
    methods = ("card", "paypal", "apple_pay", "bank_transfer", "google_pay")
    statuses = ("authorized", "failed", "authorized", "pending", "refunded", "authorized", "chargeback", "cancelled")
    countries = ("NL", "US", "DE", "BE", "FR", "GB", "CA", "ES")
    for gs in range(1, 121):
        payment_id = 2000 + gs
        amount = round(25 + ((gs * 17) % 475) + (((gs * 13) % 100) / 100), 2)
        created = (now - timedelta(hours=(gs - 1) % 168)).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=((gs - 1) % 4) * 15)
        updated = created + timedelta(hours=(payment_id % 6) + 1)
        payments.append(_payment(
            payment_id, ((gs - 1) % 10) + 1, 700 + gs, amount,
            currencies[(gs - 1) % 4], methods[(gs - 1) % 5],
            statuses[(gs - 1) % 8], countries[(gs - 1) % 8], created, updated,
        ))
    return payments


def to_debezium_envelope(payment):
    after = dict(payment)
    after["created_at"] = _micros(payment["created_at"])
    after["updated_at"] = _micros(payment["updated_at"])
    return {"op": "r", "before": None, "after": after}


def seed_jsonl(now=None):
    return "\n".join(json.dumps(to_debezium_envelope(p)) for p in generate_seed_payments(now)) + "\n"

# COMMAND ----------

# MAGIC %md ## Setup -- schema + volume (idempotent)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Ready: {CATALOG}.{SCHEMA}, volume {VOLUME_ROOT}")

# COMMAND ----------

# MAGIC %md ## Seed -- write 124 Debezium envelopes to the Volume (replaces Postgres/Debezium/Kafka)

# COMMAND ----------

import os

os.makedirs(SEED_DIR, exist_ok=True)
_target = f"{SEED_DIR}/seed_snapshot.jsonl"
with open(_target, "w", encoding="utf-8") as handle:
    handle.write(seed_jsonl())
print(f"Wrote {EXPECTED_SEED_COUNT} payment envelopes to {_target}")

# COMMAND ----------

# MAGIC %md ## Bronze -- Auto Loader ingest with PII masking

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        kafka_key STRING, kafka_value STRING, kafka_topic STRING,
        kafka_partition INT, kafka_offset BIGINT, kafka_timestamp TIMESTAMP
    ) USING delta CLUSTER BY (kafka_timestamp)
""")

mask_udf = udf(mask_pii_fields, StringType())

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", f"{SCHEMA_ROOT}/bronze")
    .load(SEED_DIR)
    .select(
        get_json_object(col("value"), "$.after.payment_id").alias("kafka_key"),
        col("value").alias("kafka_value"),
        lit(CDC_TOPIC).alias("kafka_topic"),
        lit(0).cast("int").alias("kafka_partition"),
        # Streaming can't use monotonically_increasing_id(); the unique payment_id is a
        # stable Kafka-offset stand-in (Silver only uses it as a dedup tiebreaker).
        coalesce(get_json_object(col("value"), "$.after.payment_id"),
                 get_json_object(col("value"), "$.before.payment_id")).cast("long").alias("kafka_offset"),
        current_timestamp().alias("kafka_timestamp"),
    )
    .withColumn("kafka_value", mask_udf(col("kafka_value")))
)
(
    bronze_stream.writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze")
    .toTable(BRONZE_TABLE)
    .awaitTermination()
)
print(f"Bronze ingest complete: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## Silver -- parse + data-quality + MERGE/delete + DLQ

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DLQ_TABLE} (
        kafka_value STRING, batch_id BIGINT, error_reason STRING, ingested_at TIMESTAMP
    ) USING delta CLUSTER BY (ingested_at)
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        payment_id BIGINT, merchant_id BIGINT, amount DECIMAL(12,2), currency STRING,
        payment_method STRING, payment_status STRING, country_code STRING,
        created_at TIMESTAMP, updated_at TIMESTAMP, ingested_at TIMESTAMP
    ) USING delta CLUSTER BY (created_at)
""")


def _write_to_dlq(records, batch_id, reason):
    (records.select(
        col("kafka_value"), lit(batch_id).cast("long").alias("batch_id"),
        lit(reason).alias("error_reason"), current_timestamp().alias("ingested_at"),
    ).writeTo(DLQ_TABLE).append())


def _build_upserts(batch_df):
    projected = (
        batch_df
        .filter(get_json_object(col("kafka_value"), "$.op").isin("c", "u", "r"))
        .select(
            get_json_object(col("kafka_value"), "$.after.payment_id").cast("long").alias("payment_id"),
            get_json_object(col("kafka_value"), "$.after.merchant_id").cast("long").alias("merchant_id"),
            get_json_object(col("kafka_value"), "$.after.amount").cast("decimal(12,2)").alias("amount"),
            upper(trim(get_json_object(col("kafka_value"), "$.after.currency"))).alias("currency"),
            regexp_replace(lower(trim(get_json_object(col("kafka_value"), "$.after.payment_method"))), r"\s+", "_").alias("payment_method"),
            regexp_replace(lower(trim(get_json_object(col("kafka_value"), "$.after.payment_status"))), r"\s+", "_").alias("payment_status"),
            upper(trim(get_json_object(col("kafka_value"), "$.after.country_code"))).alias("country_code"),
            from_unixtime(get_json_object(col("kafka_value"), "$.after.created_at").cast("double") / 1_000_000).cast("timestamp").alias("created_at"),
            from_unixtime(get_json_object(col("kafka_value"), "$.after.updated_at").cast("double") / 1_000_000).cast("timestamp").alias("updated_at"),
            col("kafka_offset").alias("_kafka_offset"),
            current_timestamp().alias("ingested_at"),
        )
    )
    latest_per_key = Window.partitionBy("payment_id").orderBy(col("updated_at").desc(), col("_kafka_offset").desc())
    return (projected.withColumn("_rn", row_number().over(latest_per_key))
            .filter(col("_rn") == 1).drop("_rn", "_kafka_offset"))


def _validate_upserts(upserts):
    metrics = upserts.select(
        spark_sum(col("payment_id").isNull().cast("int")).alias("null_payment_id"),
        spark_sum(col("merchant_id").isNull().cast("int")).alias("null_merchant_id"),
        spark_sum(col("amount").isNull().cast("int")).alias("null_amount"),
        spark_sum((col("amount") < 0).cast("int")).alias("negative_amount"),
        spark_sum(col("currency").isNull().cast("int")).alias("null_currency"),
        spark_sum((~col("currency").rlike("^[A-Z]{3}$")).cast("int")).alias("invalid_currency"),
        spark_sum(col("payment_method").isNull().cast("int")).alias("null_payment_method"),
        spark_sum((~col("payment_method").isin(*ALLOWED_PAYMENT_METHODS)).cast("int")).alias("invalid_payment_method"),
        spark_sum(col("payment_status").isNull().cast("int")).alias("null_payment_status"),
        spark_sum((~col("payment_status").isin(*ALLOWED_PAYMENT_STATUSES)).cast("int")).alias("invalid_payment_status"),
        spark_sum(col("country_code").isNull().cast("int")).alias("null_country_code"),
        spark_sum((~col("country_code").rlike("^[A-Z]{2}$")).cast("int")).alias("invalid_country_code"),
        spark_sum(col("created_at").isNull().cast("int")).alias("null_created_at"),
        spark_sum(col("updated_at").isNull().cast("int")).alias("null_updated_at"),
        spark_sum((col("updated_at") < col("created_at")).cast("int")).alias("updated_before_created"),
    ).collect()[0].asDict()
    dups = (upserts.groupBy("payment_id").agg(count("*").alias("rc"))
            .filter((col("payment_id").isNotNull()) & (col("rc") > 1)).count())
    failures = {k: v for k, v in metrics.items() if v}
    if dups:
        failures["duplicate_payment_ids"] = dups
    if failures:
        raise ValueError(f"Silver data quality checks failed: {failures}")


def _upsert_to_silver(batch_df, batch_id):
    sp = batch_df.sparkSession
    op_col = get_json_object(col("kafka_value"), "$.op")
    malformed = batch_df.filter(op_col.isNull())
    if not malformed.isEmpty():
        _write_to_dlq(malformed, batch_id, "null_op")
    known_ops = batch_df.filter(op_col.isNotNull())
    unexpected = known_ops.filter(~op_col.isin("c", "u", "r", "d"))
    if not unexpected.isEmpty():
        _write_to_dlq(unexpected, batch_id, "unexpected_op")
    processable = known_ops.filter(op_col.isin("c", "u", "r", "d"))
    upserts = _build_upserts(processable)
    if not upserts.isEmpty():
        _validate_upserts(upserts)
        upserts.createOrReplaceTempView("_silver_upserts")
        sp.sql(f"""
            MERGE INTO {SILVER_TABLE} t USING _silver_upserts s ON t.payment_id = s.payment_id
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)
    deletes = (processable.filter(op_col == "d")
               .select(get_json_object(col("kafka_value"), "$.before.payment_id").cast("long").alias("payment_id"))
               .filter(col("payment_id").isNotNull()))
    if not deletes.isEmpty():
        deletes.createOrReplaceTempView("_silver_deletes")
        sp.sql(f"DELETE FROM {SILVER_TABLE} WHERE payment_id IN (SELECT payment_id FROM _silver_deletes)")


(
    spark.readStream.table(BRONZE_TABLE).writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver")
    .foreachBatch(_upsert_to_silver)
    .start()
    .awaitTermination()
)
print(f"Silver complete: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md ## Gold -- hourly metrics by country and method

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
        payment_hour TIMESTAMP, country_code STRING, payment_method STRING,
        payment_count BIGINT, gross_volume DECIMAL(18,2), auth_rate DOUBLE
    ) USING delta CLUSTER BY (payment_hour)
""")


def _recompute_gold(batch_df, batch_id):
    sp = batch_df.sparkSession
    affected = (
        batch_df.select(
            get_json_object(col("kafka_value"), "$.op").alias("op"),
            get_json_object(col("kafka_value"), "$.after.created_at").cast("double").alias("after_created_at"),
            get_json_object(col("kafka_value"), "$.before.created_at").cast("double").alias("before_created_at"),
        ).select(
            date_trunc("hour", from_unixtime(
                when(col("op") == "d", col("before_created_at")).otherwise(col("after_created_at")) / 1_000_000
            ).cast("timestamp")).alias("payment_hour")
        ).filter(col("payment_hour").isNotNull()).distinct()
    )
    if affected.isEmpty():
        return
    affected.createOrReplaceTempView("_gold_affected_hours")
    sp.sql(f"DELETE FROM {GOLD_TABLE} WHERE payment_hour IN (SELECT payment_hour FROM _gold_affected_hours)")
    sp.sql(f"""
        INSERT INTO {GOLD_TABLE}
        SELECT date_trunc('hour', created_at) AS payment_hour, country_code, payment_method,
               count(*) AS payment_count, CAST(sum(amount) AS DECIMAL(18,2)) AS gross_volume,
               avg(CASE WHEN payment_status = 'authorized' THEN 1.0 ELSE 0.0 END) AS auth_rate
        FROM {SILVER_TABLE}
        WHERE date_trunc('hour', created_at) IN (SELECT payment_hour FROM _gold_affected_hours)
        GROUP BY 1, 2, 3
    """)


(
    spark.readStream.table(BRONZE_TABLE).writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/gold")
    .foreachBatch(_recompute_gold)
    .start()
    .awaitTermination()
)
print(f"Gold complete: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md ## Validate -- 124 / 124 / 124, DLQ empty

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE).count()
silver = spark.table(SILVER_TABLE).count()
dlq = spark.table(DLQ_TABLE).count()
g = spark.sql(f"SELECT count(*) AS rows, coalesce(sum(payment_count), 0) AS total FROM {GOLD_TABLE}").collect()[0]
print(f"bronze={bronze} silver={silver} gold_rows={g['rows']} gold_total={g['total']} dlq={dlq}")

errors = []
if bronze != EXPECTED_SEED_COUNT:
    errors.append(f"bronze {bronze} != {EXPECTED_SEED_COUNT}")
if silver != EXPECTED_SEED_COUNT:
    errors.append(f"silver {silver} != {EXPECTED_SEED_COUNT}")
if g["total"] != EXPECTED_SEED_COUNT:
    errors.append(f"gold sum(payment_count) {g['total']} != {EXPECTED_SEED_COUNT}")
if dlq != 0:
    errors.append(f"DLQ has {dlq} rows (expected 0)")
if errors:
    raise SystemExit("Validation failed:\n  " + "\n  ".join(errors))
print("Validation passed: 124 -> 124 -> 124, DLQ empty")
