
# Allows type hints to refer to types defined later in the file.
from __future__ import annotations

import csv
import json
import sys
from operator import add
from pathlib import Path

# Find the project root directory.
# parents[1] moves two directory levels up from this Python file.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Add the project root to Python's import path if it is not already there.
# This allows Python to find project modules such as analytics and config.
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from analytics.spark_support import create_spark_session
from config import Config


# Store the information and rules for our Department Demand scenario.
# Keeping these settings together makes the analysis easier to understand.
PROFILE = {

    "title": "Department demand",


    # The key used to group sales records.
    "key_description": "dept_id",

    # Convert a parsed record into a key-value pair:
    # (department_id, sales_value).
    "project": lambda row: (row[1], row[5]),

    # Keep records whose sales value is zero or greater.
    # This excludes negative sales values from the analysis.
    "filter": lambda row: row[5] >= 0,
}


def parse_sales(line: str) -> tuple[str, str, str, str, str, float]:
    # Parse a CSV line correctly, including fields containing commas
    # enclosed in quotation marks.
    row = next(csv.reader([line]))

    # Select the required columns and convert sales to a number.
    # The returned tuple has six fields.
    # The original code assumes row[7] contains the sales value.
    return row[1], row[2], row[3], row[4], row[6], float(row[7])


def execute_rdd(raw, profile: dict | None = None) -> dict:

    # ACTION: first() returns the first line of the RDD.
    # We use it to identify the CSV header so it can be excluded.
    header = raw.first()

    # TRANSFORMATION: filter() removes lines equal to the header.
    # TRANSFORMATION: map() parses each remaining CSV line into a tuple.
    # TRANSFORMATION: filter() applies the scenario's sales rule.
    # persist() requests that this cleaned RDD be cached for reuse.
    #
    # Why cache it?
    # We use records in multiple calculations, so caching can avoid
    # repeating the CSV parsing and cleaning work after materialization.
    records = (
        raw.filter(lambda line: line != header)
        .map(parse_sales)
        .filter(profile["filter"])
        .persist()
    )

    # TRANSFORMATION: map() converts each record into a key-value pair.
    # The key is the department ID, and the value is the sales amount.
    # Example: ("HOBBIES_1", 25.0)
    projected = records.map(profile["project"])

    # TRANSFORMATION: flatMap() can emit zero, one, or many output items
    # for every input item. Here, it emits the existing pair as a
    # one-item list, so the data stays the same.
    # This is redundant for this particular calculation, but demonstrates
    # how flatMap() works.
    key_values = projected.flatMap(lambda pair: [(pair[0], pair[1])])

    # TRANSFORMATION: reduceByKey() combines values that have the same key.
    # add combines sales amounts, producing total sales per department.
    # This is a key-value RDD aggregation and can cause a shuffle because
    # values for the same department may be in different partitions.
    totals = key_values.reduceByKey(add)

    # TRANSFORMATION: groupByKey() collects all sales values for each
    # department into an iterable group.
    # This demonstrates grouping, but can use more memory than necessary
    # for large datasets.
    grouped = key_values.groupByKey()

    # TRANSFORMATION: mapValues() changes only the values, not the keys.
    # Count the number of sales records for each department.
    observation_counts = grouped.mapValues(
        lambda values: sum(1 for _ in values)
    )

    # TRANSFORMATION: sortByKey() sorts department IDs by key.
    # ACTION: take(8) returns up to the first eight results to Python.
    # This is an alphabetical/key-order sample, not the top eight by sales.
    sorted_counts = observation_counts.sortByKey().take(8)

    # Sort department totals by their numeric value in descending order.
    # ACTION: take(8) returns up to eight departments with the largest totals.
    top_totals = totals.sortBy(
        lambda pair: pair[1], ascending=False
    ).take(8)

    # TRANSFORMATION: mapPartitionsWithIndex() processes each partition
    # and provides its partition index.
    # Count rows in each partition for a small partition summary.
    #
    # ACTION: collect() brings these small partition summaries to Python.
    # Avoid collecting the entire sales dataset for large datasets.
    partition_preview = records.mapPartitionsWithIndex(
        lambda index, values: [(index, sum(1 for _ in values))]
    ).collect()

    # ACTION: count() evaluates the cleaned RDD and returns its row count.
    # It also materializes the cached RDD partitions as they are computed.
    input_rows = records.count()

    # Return the number of partitions in the cleaned RDD.
    partition_count = records.getNumPartitions()

    return {
        # Scenario information used by the dashboard or report.
        "id": profile["id"],
        "title": profile["title"],
        "question": profile["question"],
        "purpose": profile["purpose"],
        "key_description": profile["key_description"],

        # Results from the RDD analysis.
        "input_rows_after_filter": input_rows,
        "partitions": partition_count,

        # Show how many cleaned records are in each partition.
        "partition_preview": [
            {"partition": key, "rows": value}
            for key, value in partition_preview
        ],

        # Department totals sorted from highest to lowest sales.
        "top_totals": [
            {"key": key, "value": value}
            for key, value in top_totals
        ],

        # A small sample of department observation counts.
        "sorted_key_sample": [
            {"key": key, "observations": value}
            for key, value in sorted_counts
        ],

        "concepts": {
            # List the transformations used in this analysis.
            "transformations": [
                "map",
                "filter",
                "flatMap",
                "reduceByKey",
                "groupByKey",
                "mapValues",
                "sortByKey",
                "mapPartitionsWithIndex",
            ],

            # Actions trigger execution and return results or values.
            "actions": ["first", "count", "take", "collect"],

            # getStorageLevel() reports the configured storage level
            # of the RDD. persist() requests caching; actions materialize it.
            "persistence": str(records.getStorageLevel()),

            # toDebugString() returns a textual description of the RDD's
            # dependency chain. decode() converts the returned bytes to text.
            #
            # This describes records' lineage, from its source through
            # the cleaning transformations. It does not show the complete
            # downstream totals aggregation, so inspect totals separately
            # if you want to demonstrate the reduceByKey shuffle lineage.
            "lineage": records.toDebugString().decode(
                "utf-8", errors="replace"
            ),

            # Narrow transformations usually process data within partitions.
            # Wide transformations such as reduceByKey and groupByKey
            # can require a shuffle across partitions.
            "dag": (
                "map/filter are narrow transformations; reduceByKey, "
                "groupByKey, and sorting can introduce shuffle stages."
            ),

            # Spark uses lineage to recompute lost partitions when possible.
            # This explains fault tolerance; it does not simulate a failure.
            "fault_tolerance": (
                "If a partition is lost, Spark can recompute it from the "
                "CSV and transformation lineage, if the source and "
                "required dependencies remain available."
            ),
        },
    }


def run(
    config: Config | None = None,
    spark=None,
    raw=None,
    output: Path | None = None,
) -> dict:
    """Run the Department Demand RDD analysis."""

    # Python 3.14 requires PySpark 4.x for Python RDD execution.
    # Raise a clear error if Python 3.14 is used with PySpark 3.x.
    if sys.version_info >= (3, 14):
        import pyspark

        if pyspark.__version__.startswith("3."):
            raise RuntimeError(
                "Python 3.14 requires PySpark 4.x for Python RDD execution. "
                "Install the pinned dependencies with: "
                ".venv/bin/pip install -r requirements.txt"
            )

    # Use the project's configuration or create the default configuration.
    config = config or Config()

    # Prepare required directories and project paths.
    config.prepare()

    # Choose where to save the JSON results.
    output = output or (
        config.results / "metrics" / "rdd_lab2_summary.json"
    )

    # Track whether this function created the Spark session.
    # Only stop it automatically if this function created it.
    should_stop = False

    if spark is None:
        # Create the Spark session used to run the RDD operations.
        spark = create_spark_session("RetailDemandLabRDDLab2")
        spark.sparkContext.setLogLevel("WARN")
        should_stop = True

    try:
        # Read the CSV as an RDD unless a caller supplied an RDD already.
        if raw is None:
            raw = spark.sparkContext.textFile(str(config.sales_path))

        # Execute this scenario's transformations and actions.
        profile_res = execute_rdd(raw, PROFILE)

        # Add Spark and input metadata to the saved JSON report.
        summary = {
            "engine": "pyspark-rdd",
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "input": str(config.sales_path),
            "profile_count": 1,
            "profiles": [profile_res],
        }

        # Create the output directory if it does not exist.
        output.parent.mkdir(parents=True, exist_ok=True)

        # Write the results in readable JSON format.
        output.write_text(json.dumps(summary, indent=2))

        # Return the scenario results to the caller.
        return profile_res

    finally:
        # Stop Spark only when this function created the session.
        # If a session was supplied by another part of the application,
        # leave it running for that caller to manage.
        if should_stop:
            spark.stop()


# Run the analysis when this file is executed directly.
if __name__ == "__main__":
    result = run()

    # Print the scenario results in readable JSON format.
    print(json.dumps(result, indent=2))
