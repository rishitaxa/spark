"""Run naive vs optimized pipeline on the same data and print/save timings."""
import argparse
import json
import logging

from pyspark.sql import SparkSession

from pipeline import run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--out", default="output")
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()

    spark = (SparkSession.builder.master("local[*]").appName("event-pipeline")
             .config("spark.ui.port", "4040").config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print("Spark UI: http://localhost:4040  (inspect stages, shuffles, skew while this runs)")

    results = []
    for mode in (False, True):
        run(spark, args.data, args.out, mode)  # warm-up run, not recorded
        for _ in range(args.repeats):
            results.append(run(spark, args.data, args.out, mode))

    for mode in ("naive", "optimized"):
        times = [r["seconds"] for r in results if r["mode"] == mode]
        print(f"{mode:10s} runs={times} median={sorted(times)[len(times)//2]}s")
    with open("results.json", "w") as f:
        json.dump(results, f, indent=2)
    spark.stop()


if __name__ == "__main__":
    main()
