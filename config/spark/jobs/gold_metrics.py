from __future__ import annotations

import logging

from pyspark.sql import SparkSession


SILVER_TABLE = "iceberg.analytics.payments_silver"
GOLD_TABLE   = "iceberg.analytics.payment_metrics_gold"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def _build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold-metrics")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",          "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",     "hive")
        .config("spark.sql.catalog.iceberg.uri",      "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.iceberg.warehouse","hdfs://namenode:9000/warehouse")
        .getOrCreate()
    )


def main() -> None:
    LOGGER.info("Starting gold aggregation from %s", SILVER_TABLE)
    spark = _build_spark_session()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            payment_hour   TIMESTAMP,
            country_code   STRING,
            payment_method STRING,
            payment_count  BIGINT,
            gross_volume   DECIMAL(18,2),
            auth_rate      DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(payment_hour))
    """)

    # Full idempotent recompute from silver. INSERT OVERWRITE atomically replaces every
    # row, so hours whose payments were all deleted from silver drop out of gold cleanly.
    # Gold reads only silver (linear bronze -> silver -> gold lineage); it never touches
    # bronze or the raw Debezium envelope.
    spark.sql(f"""
        INSERT OVERWRITE TABLE {GOLD_TABLE}
        SELECT
            date_trunc('hour', created_at)                                     AS payment_hour,
            country_code,
            payment_method,
            count(*)                                                            AS payment_count,
            CAST(sum(amount) AS DECIMAL(18,2))                                  AS gross_volume,
            avg(CASE WHEN payment_status = 'authorized' THEN 1.0 ELSE 0.0 END) AS auth_rate
        FROM {SILVER_TABLE}
        GROUP BY 1, 2, 3
    """)

    LOGGER.info("Gold aggregation completed")
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
