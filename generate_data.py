"""Generate a synthetic events dataset (Parquet) with a skewed user distribution."""
import argparse
from pyspark.sql import SparkSession, functions as F


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--users", type=int, default=50_000)
    ap.add_argument("--out", default="data")
    args = ap.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("generate-data").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Skew: squaring a uniform value concentrates events on low user ids.
    events = (
        spark.range(args.rows)
        .withColumn("user_id", (F.pow(F.rand(seed=1), F.lit(2.0)) * args.users).cast("long"))
        .withColumn("event_type", F.element_at(
            F.array(F.lit(" View "), F.lit("CLICK"), F.lit("purchase "), F.lit("Share")),
            (F.floor(F.rand(seed=2) * 4) + 1).cast("int")))
        .withColumn("amount", F.round(F.rand(seed=3) * 100, 2))
        .withColumn("ts", F.expr("timestamp'2026-01-01 00:00:00' + make_interval(0,0,0,0,0,0,floor(rand(4)*86400*30))"))
        .drop("id")
    )
    users = (
        spark.range(args.users).withColumnRenamed("id", "user_id")
        .withColumn("country", F.element_at(
            F.array(F.lit("IN"), F.lit("US"), F.lit("DE"), F.lit("BR")),
            (F.floor(F.rand(seed=5) * 4) + 1).cast("int")))
    )
    events.write.mode("overwrite").parquet(f"{args.out}/events")
    users.write.mode("overwrite").parquet(f"{args.out}/users")
    print(f"Wrote {args.rows:,} events and {args.users:,} users to {args.out}/")
    spark.stop()


if __name__ == "__main__":
    main()
