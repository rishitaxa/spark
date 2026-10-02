# Spark Event Pipeline: batch ETL and performance tuning

A PySpark batch pipeline over a synthetic, skewed events dataset, with a benchmark that compares a
naive baseline against a tuned version, plus unit tests.

**Pipeline:** read Parquet events -> clean/normalize -> daily per-user metrics -> enrich with user
country -> write Parquet.

| | Naive baseline | Optimized |
|---|---|---|
| Normalization | Python UDF | Built-in `lower`/`trim` |
| Join with users | Shuffle join | Broadcast join |
| Shuffle partitions | 200 (default) | 16 |
| Output layout | Unpartitioned | `partitionBy(event_date)` |

## Run it
```bash
pip install -r requirements.txt        # needs Java 11/17/21
python generate_data.py --rows 5000000 --users 50000
python -m pytest -q
python benchmark.py --repeats 3        # open http://localhost:4040 while it runs
```
`benchmark.py` prints the median runtime for each mode and writes `results.json`.

## Results (fill in from YOUR machine)
| Machine / Spark version | Rows | Naive median (s) | Optimized median (s) |
|---|---|---|---|
| | | | |

## What to inspect in the Spark UI (http://localhost:4040)
- **Stages tab:** compare the number of tasks and shuffle read/write between the two modes.
- **SQL tab:** find `BroadcastHashJoin` vs `SortMergeJoin` in the plans.
- **Tasks within a stage:** look for skew (a few tasks much slower than the rest).

## Notes / limitations
- Runs locally (`local[*]`), so the numbers show relative effects of the changes, not cluster performance.
- Adaptive Query Execution is turned off so the comparison reflects the changes made here.
"# spark" 
