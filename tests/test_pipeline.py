import datetime as dt

import pytest
from pyspark.sql import SparkSession

from pipeline import clean_events, daily_user_metrics, enrich


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("tests").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


@pytest.fixture
def events(spark):
    t = dt.datetime(2026, 1, 1, 10, 0, 0)
    return spark.createDataFrame(
        [(1, " View ", 0.0, t), (1, "PURCHASE", 20.0, t), (2, "purchase ", 5.5, t),
         (None, "click", 1.0, t), (3, "click", -4.0, t)],
        "user_id long, event_type string, amount double, ts timestamp",
    )


@pytest.mark.parametrize("optimized", [True, False])
def test_clean_normalizes_and_drops_invalid_rows(events, optimized):
    rows = clean_events(events, optimized).collect()
    assert len(rows) == 3  # null user_id and negative amount removed
    assert {r.event_type for r in rows} == {"view", "purchase"}


def test_daily_metrics_aggregates_revenue(events):
    out = {r.user_id: r for r in daily_user_metrics(clean_events(events, True)).collect()}
    assert out[1].events == 2 and out[1].revenue == 20.0
    assert out[2].revenue == 5.5


@pytest.mark.parametrize("optimized", [True, False])
def test_enrich_keeps_all_metric_rows_and_adds_country(spark, events, optimized):
    metrics = daily_user_metrics(clean_events(events, True))
    users = spark.createDataFrame([(1, "IN")], "user_id long, country string")
    rows = {r.user_id: r.country for r in enrich(metrics, users, optimized).collect()}
    assert rows == {1: "IN", 2: None}
