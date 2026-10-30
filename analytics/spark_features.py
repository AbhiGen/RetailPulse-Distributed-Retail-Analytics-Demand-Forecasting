"""
Spark Feature Engineering Module
================================
What:
    Constructs distributed time-series features (lags, rolling averages, calendar attributes)
    from daily item-store sales data using PySpark SQL Window functions.

Why:
    Time-series machine learning models require historical context and calendar seasonality
    to predict future demand. Calculating these features across distributed partitions
    ensures high throughput without running into single-node memory limitations.
"""
from __future__ import annotations


def add_spark_features(frame):
    """
    Generate Lag and Rolling Window Features Using PySpark
    ------------------------------------------------------
    What:
        Applies PySpark Window specifications to compute:
        1. Historical lag features: lag_1 (yesterday), lag_7 (prior week), lag_28 (prior month).
        2. Rolling window averages: rolling_7 (7-day MA) and rolling_28 (28-day MA).
        3. Calendar features: day_of_week (0-6 index) and month_num (1-12 index).

    Why:
        - Window.partitionBy("item_id", "store_id"): Guarantees that lag and rolling statistics
          are strictly computed per individual product within each specific store, preventing
          cross-series data pollution.
        - Window.orderBy("date"): Ensures strict chronological ordering of rows within each partition.
        - rowsBetween(-7, -1) / rowsBetween(-28, -1): Excludes the target day (index 0) from the
          window calculation to completely eliminate lookahead bias and data leakage.
        - lag_1 captures immediate day-to-day momentum.
        - lag_7 captures weekly seasonality (e.g., Saturday vs Wednesday purchasing patterns).
        - lag_28 captures long-term monthly cycles without overlapping the standard 28-day forecast horizon.
        - day_of_week and month_num enable tree-based algorithms to learn day-of-week and seasonal shifts.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # What: Define window partitioning by item and store, ordered chronologically by date.
    # Why: Isolates each time-series so historical calculations only consider that specific product's sales in that store.
    partition = Window.partitionBy("item_id", "store_id").orderBy("date")

    return (frame
        # What: Shift sales backward by 1 day.
        # Why: Captures immediate yesterday demand (short-term momentum).
        .withColumn("lag_1", F.lag("sales", 1).over(partition))
        # What: Shift sales backward by 7 days.
        # Why: Captures weekly recurring seasonality (e.g. weekend shopping spikes).
        .withColumn("lag_7", F.lag("sales", 7).over(partition))
        # What: Shift sales backward by 28 days (4 weeks).
        # Why: Matches standard 28-day forecast horizon and avoids lookahead bias for multi-step predictions.
        .withColumn("lag_28", F.lag("sales", 28).over(partition))
        # What: Compute 7-day moving average of sales, excluding the current day (rows -7 to -1).
        # Why: Smooths short-term noise and isolates recent demand level without leaking target values.
        .withColumn("rolling_7", F.avg("sales").over(partition.rowsBetween(-7, -1)))
        # What: Compute 28-day moving average of sales, excluding the current day (rows -28 to -1).
        # Why: Captures broader baseline monthly demand trend while preventing data leakage.
        .withColumn("rolling_28", F.avg("sales").over(partition.rowsBetween(-28, -1)))
        # What: Extract zero-indexed day of the week (0 = Sunday/Monday depending on locale offset).
        # Why: Provides a numerical cyclical feature for tree models to split on weekday vs weekend.
        .withColumn("day_of_week", F.dayofweek("date") - 1)
        # What: Extract numerical month (1 through 12) from the date.
        # Why: Enables models to capture annual seasonal demand trends (e.g. holiday surges).
        .withColumn("month_num", F.month("date")))

