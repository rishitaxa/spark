"""Batch pipeline: clean events -> daily per-user metrics -> enrich with user country.

`optimized=False` is a deliberately naive baseline (Python UDF, shuffle join,
default shuffle partitions, no reuse). `optimized=True` applies standard Spark
tuning. benchmark.py times both so you can measure the difference yourself.
"""
import logging
import time

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType

log = logging.getLogger("pipeline")


def clean_events(events: DataFrame, optimized: bool) -> DataFrame:
    if optimized:
        event_type = F.lower(F.trim(F.col("event_type")))
    else:  # Python UDF: serializes every row between the JVM and Python
        event_type = F.udf(lambda s: s.strip().lower() if s else None, StringType())("event_type")
    return (
        events.withColumn("event_type", event_type)
        .filter(F.col("user_id").isNotNull() & (F.col("amount") >= 0))
        .withColumn("event_date", F.to_date("ts"))
    )


def daily_user_metrics(events: DataFrame) -> DataFrame:
    return events.groupBy("user_id", "event_date").agg(
        F.count("*").alias("events"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0.0)).alias("revenue"),
    )


def enrich(metrics: DataFrame, users: DataFrame, optimized: bool) -> DataFrame:
    if optimized:
        users = F.broadcast(users)  # small dimension table: avoid shuffling the big side
    return metrics.join(users, "user_id", "left")


def run(spark: SparkSession, data_dir: str, out_dir: str, optimized: bool) -> dict:
    spark.conf.set("spark.sql.shuffle.partitions", "16" if optimized else "200")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760" if optimized else "-1")
    spark.conf.set("spark.sql.adaptive.enabled", "false")  # keep the comparison about our changes

    start = time.perf_counter()
    events = spark.read.parquet(f"{data_dir}/events")
    users = spark.read.parquet(f"{data_dir}/users")

    cleaned = clean_events(events, optimized)
    metrics = daily_user_metrics(cleaned)
    result = enrich(metrics, users, optimized)

    writer = result.write.mode("overwrite")
    if optimized:
        writer = writer.partitionBy("event_date")
    writer.parquet(f"{out_dir}/{'optimized' if optimized else 'naive'}")

    elapsed = time.perf_counter() - start
    rows = spark.read.parquet(f"{out_dir}/{'optimized' if optimized else 'naive'}").count()
    log.info("mode=%s seconds=%.2f output_rows=%d", "optimized" if optimized else "naive", elapsed, rows)
    return {"mode": "optimized" if optimized else "naive", "seconds": round(elapsed, 2), "output_rows": rows}
