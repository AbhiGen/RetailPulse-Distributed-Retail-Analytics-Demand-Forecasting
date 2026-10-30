"""
Spark Support Module
====================
What:
    Provides utility functions for environment detection and configuration
    of the Apache Spark execution engine (PySpark).

Why:
    Decouples Spark session lifecycle management from analytics and pipeline code,
    ensures fail-safe fallback when PySpark is not installed, and tunes local Spark
    settings (memory, partitions, filesystem) for reliable big data processing.
"""
from __future__ import annotations


def spark_available() -> bool:
    """
    Check PySpark Availability
    --------------------------
    What:
        Attempts to import the `pyspark` library and returns a boolean status.

    Why:
        Enables runtime capability detection so the application can gracefully
        fall back to non-Spark pipelines or display user-friendly diagnostics
        instead of failing abruptly with an unhandled ImportError.
    """
    try:
        import pyspark  # noqa: F401
        return True
    except ImportError:
        return False


def create_spark_session(app_name: str = "RetailForecasting"):
    """
    Create or Retrieve Configured SparkSession
    -----------------------------------------
    What:
        Initializes an Apache Spark SQL session with pre-configured memory,
        partitioning, and filesystem parameters tailored for local retail analytics.

    Why:
        1. master("local[*]"): Uses all available CPU cores on the local machine for maximum parallel throughput.
        2. spark.sql.shuffle.partitions = "16": Overrides Spark's default 200 shuffle partitions to prevent
           excessive task scheduling overhead and small-file fragmentation on local datasets.
        3. spark.driver.memory / spark.executor.memory = "4g": Allocates sufficient JVM heap memory to accommodate
           distributed DataFrame joins, window aggregations, and MLlib model training without OutOfMemory (OOM) errors.
        4. spark.hadoop.fs.defaultFS = "file:///": Forces Spark to resolve file paths against the local OS filesystem,
           preventing accidental connection attempts to remote HDFS or HBase clusters.
        5. getOrCreate(): Reuses an existing active SparkSession if already running, avoiding conflicting contexts in the JVM.
    """
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[*]")
            # What: Set the human-readable application name in Spark UI and logs.
            # Why: Helps identify and monitor jobs in the Spark Web UI (port 4040).
            .appName(app_name)
            # What: Set the number of partitions to use when shuffling data for joins or aggregations.
            # Why: Spark's default of 200 partitions creates excessive scheduling overhead for local datasets; 16 matches CPU scale.
            .config("spark.sql.shuffle.partitions", "16")
            # What: Allocate 4 GB of JVM heap memory to the Spark driver process.
            # Why: Prevents driver OOM when collecting metrics or coordinating complex multi-way joins.
            .config("spark.driver.memory", "4g")
            # What: Allocate 4 GB of JVM heap memory to worker executors.
            # Why: Ensures sufficient working memory for window operations and MLlib tree building.
            .config("spark.executor.memory", "4g")
            # What: Point Hadoop's default file system URI to local file scheme (file:///).
            # Why: Keep local CSV analytics independent of any existing HBase/HDFS environment.
            .config("spark.hadoop.fs.defaultFS", "file:///")
            # What: Instantiate the session or attach to the active one.
            # Why: Spark does not support multiple active SparkSessions with conflicting configs in a single JVM.
            .getOrCreate())

