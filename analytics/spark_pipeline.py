"""
Spark Retail Demand Forecasting Pipeline
=========================================
What:
    An end-to-end distributed Big Data batch pipeline that ingests raw retail sales,
    pricing, and calendar data, cleanses and joins datasets, engineers time-series
    lag and rolling features using Spark SQL Window functions, performs chronological
    train/test splitting, trains multiple Spark MLlib regression models (Decision Tree,
    Random Forest, Gradient Boosted Trees), evaluates performance using standard metrics,
    and exports predictions, metrics, and trained model artifacts.

Why:
    Processes large-scale multi-table retail datasets (such as Walmart M5) that exceed
    single-machine memory limits. Utilizes Apache Spark's distributed computing architecture
    and MLlib to provide scalable data transformation, leakage-free feature engineering,
    and parallel machine learning execution.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from config import Config
from analytics.spark_support import create_spark_session
from analytics.spark_features import add_spark_features

# What: Define the complete list of engineered feature column names passed to the ML models.
# Why: Establishes a single source of truth for vector assembly and ensures all rows with
#      missing lag/rolling values are cleanly identified and pruned before model fitting.
FEATURES = [
    "lag_1", "lag_7", "lag_28", "rolling_7", "rolling_28",
    "sell_price", "day_of_week", "month_num", "is_event",
]


def _metric_row(frame, name: str, prediction_column: str) -> dict:
    """
    Calculate Distributed Regression Evaluation Metrics
    --------------------------------------------------
    What:
        Uses native PySpark SQL aggregation expressions to compute:
        - MAE (Mean Absolute Error): Average absolute difference between actual and predicted sales.
        - RMSE (Root Mean Squared Error): Square root of mean squared errors, penalizing larger deviations.
        - WAPE (Weighted Absolute Percentage Error): Total absolute errors divided by total actual sales.
        - test_rows: Total count of evaluated prediction instances.

    Why:
        Executes distributed aggregations directly inside the Spark execution engine,
        avoiding the expensive bottleneck of collecting millions of prediction rows
        into Python driver memory.
    """
    from pyspark.sql import functions as F

    values = frame.select(
        F.lit(name).alias("model"),
        # What: Calculate Mean Absolute Error (MAE).
        # Why: Provides an intuitive, robust average dollar/unit forecast error unaffected by extreme outliers.
        F.avg(F.abs(F.col("sales") - F.col(prediction_column))).alias("mae"),
        # What: Calculate Root Mean Squared Error (RMSE).
        # Why: Strongly penalizes large forecast misses, crucial for inventory planning to avoid stockouts.
        F.sqrt(F.avg(F.pow(F.col("sales") - F.col(prediction_column), 2))).alias("rmse"),
        # What: Calculate Weighted Absolute Percentage Error (WAPE).
        # Why: Standard retail benchmark metric that avoids division-by-zero on intermittent zero-sale days.
        (F.sum(F.abs(F.col("sales") - F.col(prediction_column))) / F.sum(F.abs("sales"))).alias("wape"),
        # What: Count total test rows evaluated.
        # Why: Confirms the evaluation sample size across models.
        F.count("sales").alias("test_rows"),
    ).first()
    return {key: values[key] for key in values.asDict()}


def run(config: Config | None = None) -> dict:
    """
    Execute the Distributed PySpark Training and Evaluation Pipeline
    ----------------------------------------------------------------
    What:
        Runs the full batch pipeline:
        1. Ingests raw CSVs (sales, prices, calendar) into Spark DataFrames.
        2. Casts schemas and joins relational tables on keys (d, store_id, item_id, wm_yr_wk).
        3. Imputes missing prices using median estimation and creates event indicator flags.
        4. Computes windowed lag and rolling demand features.
        5. Performs chronological train/test splitting to prevent lookahead data leakage.
        6. Assembles feature vectors and trains Decision Tree, Random Forest, and GBT models.
        7. Evaluates models, saves predictions/metrics to CSV, and saves the trained model artifact.
        8. Collects execution metadata and safely tears down the SparkSession.

    Why:
        Guarantees reproducible, high-throughput distributed model training while maintaining
        strict temporal integrity and clean memory lifecycle management.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor, RandomForestRegressor
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # What: Initialize default project configuration if none is passed.
    # Why: Ensures file paths, hyperparameters, and directory structures are resolved consistently.
    config = config or Config()
    # What: Create required output directories (results/metrics, results/predictions, models).
    # Why: Prevents file-not-found I/O errors when exporting pipeline outputs.
    config.prepare()

    # What: Start a high-resolution performance counter.
    # Why: Measures total pipeline execution time for benchmarking distributed operations.
    started = time.perf_counter()

    # What: Initialize an Apache Spark session configured for local multi-core execution.
    # Why: Allocates memory and sets up the distributed execution engine.
    spark = create_spark_session("RetailDemandLabSpark")
    # What: Restrict Spark log verbosity to WARN and ERROR only.
    # Why: Silences high-volume JVM INFO logs to keep terminal output readable.
    spark.sparkContext.setLogLevel("WARN")

    

    try:
        # =========================================================================
        # 1. Distributed Data Ingestion
        # =========================================================================
        # What: Load raw sales, pricing, and calendar CSV files into distributed Spark DataFrames.
        # Why: Ingests raw data in parallel across available CPU cores with header and schema detection.
        sales = spark.read.option("header", True).option("inferSchema", True).csv(str(config.sales_path))
        prices = spark.read.option("header", True).option("inferSchema", True).csv(str(config.prices_path))
        calendar = spark.read.option("header", True).option("inferSchema", True).csv(str(config.calendar_path))

        # =========================================================================
        # 2. Schema Standardization & Type Casting
        # =========================================================================
        # What: Explicitly cast columns to strictly typed numeric and date formats.
        # Why: Avoids implicit casting discrepancies, ensures proper arithmetic precision for sales/prices,
        #      and converts date strings into Spark DateType for temporal sorting and filtering.
        sales = sales.withColumn("sales", F.col("sales").cast("double"))
        prices = prices.withColumn("wm_yr_wk", F.col("wm_yr_wk").cast("long")).withColumn("sell_price", F.col("sell_price").cast("double"))
        calendar = calendar.withColumn("wm_yr_wk", F.col("wm_yr_wk").cast("long")).withColumn("date", F.to_date("date"))

        # =========================================================================
        # 3. Distributed Table Joins & Referential Integrity Tracking
        # =========================================================================
        # What: Left-join daily sales with calendar metadata using day identifier 'd'.
        # Why: Enriches each daily sale record with calendar date, weekday, and promotional event names.
        calendar_keys = calendar.select("d", "date", "wm_yr_wk", "weekday", "month", "year", "event_name_1", "event_name_2")
        daily = sales.join(calendar_keys, "d", "left")

        # What: Count sales rows that failed to match any calendar record.
        # Why: Quantifies join quality and detects missing calendar entries early in the pipeline.
        unmatched_calendar = daily.filter(F.col("date").isNull()).count()

        # What: Left-join the sales-calendar dataset with weekly product pricing on (store_id, item_id, wm_yr_wk).
        # Why: Links unit selling prices to historical sales for revenue and price-elasticity modeling.
        price_keys = prices.select("store_id", "item_id", "wm_yr_wk", "sell_price")
        daily = daily.join(price_keys, ["store_id", "item_id", "wm_yr_wk"], "left")

        # What: Count sales rows lacking a price record.
        # Why: Tracks missing price data for data quality monitoring before imputation.
        unmatched_prices = daily.filter(F.col("sell_price").isNull()).count()

        # =========================================================================
        # 4. Outlier-Resistant Price Imputation & Event Flags
        # =========================================================================
        # What: Compute the approximate 50th percentile (median) price across the entire price catalog.
        # Why: Spark MLlib models cannot handle null feature values; median imputation is resilient
        #      against extreme price outliers and provides a realistic fallback.
        global_price = prices.select(F.percentile_approx("sell_price", 0.5).alias("median")).first()["median"]
        daily = daily.withColumn("sell_price", F.coalesce("sell_price", F.lit(float(global_price or 0.0))))

        # What: Create a binary flag (1 or 0) indicating whether promotional event 1 or 2 occurred on that date.
        # Why: Promotional events (e.g. Super Bowl, Thanksgiving) strongly drive retail spikes; a binary indicator
        #      gives tree models a clean split criterion.
        daily = daily.withColumn("is_event", ((F.coalesce("event_name_1", F.lit("None")) != "None") | (F.coalesce("event_name_2", F.lit("None")) != "None")).cast("int"))

        # What: Extract zero-indexed day of week (0=Sunday/Monday) and calendar month (1-12).
        # Why: Encodes weekly consumer shopping cycles and annual seasonal purchasing behavior.
        daily = daily.withColumn("day_of_week", F.dayofweek("date") - 1).withColumn("month_num", F.month("date"))

        # =========================================================================
        # 5. Distributed Time-Series Feature Engineering & Null Removal
        # =========================================================================
        # What: Add window-based lag and rolling average features, then drop rows with nulls in FEATURES.
        # Why: Computing lag_28 leaves the first 28 historical days with null values. Spark MLlib models
        #      strictly require complete, non-null numeric feature vectors to train without errors.
        featured = add_spark_features(daily).dropna(subset=FEATURES)

        # =========================================================================
        # 6. Strict Chronological Train / Test Split
        # =========================================================================
        # What: Collect distinct dates in chronological order to compute split boundary cutoffs.
        # Why: In retail demand forecasting, random splitting causes catastrophic lookahead data leakage.
        #      A strict chronological split holds out the final N days as an unseen test horizon.
        dates = featured.select("date").distinct().orderBy("date")
        date_values = [row["date"] for row in dates.collect()]
        if len(date_values) < config.test_days + config.validation_days:
            raise ValueError("Not enough distinct dates for the configured chronological split")

        test_start = date_values[-config.test_days]
        validation_start = date_values[-(config.test_days + config.validation_days)]

        # What: Split data into training and out-of-time test partitions.
        # Why: Guarantees the models are trained solely on past data and evaluated on subsequent future dates.
        train = featured.filter(F.col("date") < F.lit(validation_start))
        test = featured.filter(F.col("date") >= F.lit(test_start))

        # =========================================================================
        # 7. Spark MLlib Vector Assembly
        # =========================================================================
        # What: Assemble individual numeric columns into a single Spark MLlib Vector column named 'features'.
        # Why: Apache Spark MLlib regression estimators require all predictor features to be packed
        #      into a single unified Vector per row.
        assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
        train_assembled = assembler.transform(train)
        test_assembled = assembler.transform(test)

        # =========================================================================
        # 8. Model 1: Decision Tree Regressor (Spark MLlib)
        # =========================================================================
        # What: Train a single Decision Tree Regressor (maxDepth=6) and clip predictions at 0.0.
        # Why: Provides a fast, interpretable, non-linear baseline model; clipping at 0 ensures no negative sales.
        dt_estimator = DecisionTreeRegressor(featuresCol="features", labelCol="sales", predictionCol="dt_prediction", maxDepth=6, seed=config.random_seed)
        dt_model = dt_estimator.fit(train_assembled)
        predictions = dt_model.transform(test_assembled).withColumn("dt_prediction", F.greatest(F.lit(0.0), F.col("dt_prediction")))

        # =========================================================================
        # 9. Model 2: Random Forest Regressor (Spark MLlib)
        # =========================================================================
        # What: Train an ensemble of 24 decision trees using bootstrap aggregation (bagging).
        # Why: Averages across multiple randomized decision trees to reduce prediction variance and prevent overfitting.
        estimator = RandomForestRegressor(featuresCol="features", labelCol="sales", predictionCol="rf_prediction", numTrees=24, maxDepth=8, minInstancesPerNode=5, seed=config.random_seed)
        model = estimator.fit(train_assembled)
        predictions = model.transform(predictions).withColumn("rf_prediction", F.greatest(F.lit(0.0), F.col("rf_prediction")))

        # =========================================================================
        # 10. Model 3: Gradient Boosted Trees (GBT) Regressor (Spark MLlib)
        # =========================================================================
        # What: Train an ensemble of sequentially boosted trees (15 iterations, maxDepth=4).
        # Why: Progressively optimizes residual errors from previous trees to capture complex non-linear patterns.
        gbt_estimator = GBTRegressor(featuresCol="features", labelCol="sales", predictionCol="gbt_prediction", maxIter=15, maxDepth=4, maxBins=32, seed=config.random_seed)
        gbt_model = gbt_estimator.fit(train_assembled)
        predictions = gbt_model.transform(predictions).withColumn("gbt_prediction", F.greatest(F.lit(0.0), F.col("gbt_prediction")))

        # =========================================================================
        # 11. Model Evaluation & Metrics Aggregation
        # =========================================================================
        # What: Calculate MAE, RMSE, and WAPE across the holdout test set for all 3 models.
        # Why: Produces a direct, standardized performance comparison between single tree, bagged forest, and boosted trees.
        metrics = [
            _metric_row(predictions, "decision_tree_spark", "dt_prediction"),
            _metric_row(predictions, "random_forest_spark", "rf_prediction"),
            _metric_row(predictions, "gradient_boosting_spark", "gbt_prediction"),
        ]

        # =========================================================================
        # 12. Output Export & Model Persistence
        # =========================================================================
        # What: Save evaluation metrics and granular test predictions to CSV files on disk.
        # Why: Makes results accessible for downstream dashboards, visualizations, and business decision support.
        metrics_path = config.results / "metrics" / "spark_model_metrics.csv"
        import pandas as pd
        pd.DataFrame(metrics).to_csv(metrics_path, index=False)
        predictions.select("date", "item_id", "store_id", "sales", "dt_prediction", "rf_prediction", "gbt_prediction").toPandas().to_csv(config.results / "predictions" / "spark_test_predictions.csv", index=False)

        # What: Serialize and save the trained Random Forest model artifact to disk.
        # Why: Allows the trained Spark model to be reloaded later for batch inference or deployment without retraining.
        model_path = config.models / "spark_random_forest"
        model.write().overwrite().save(str(model_path))

        # =========================================================================
        # 13. Pipeline Execution Summary Metadata
        # =========================================================================
        # What: Assemble runtime metrics, data row counts, and Spark cluster configurations into a JSON summary.
        # Why: Provides end-to-end auditability and execution observability for big data governance.
        summary = {
            "engine": "pyspark",
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "sales_rows": sales.count(), "price_rows": prices.count(), "calendar_rows": calendar.count(),
            "daily_rows": daily.count(), "feature_rows": featured.count(), "train_rows": train.count(), "test_rows": test.count(),
            "unmatched_calendar": unmatched_calendar, "unmatched_prices": unmatched_prices,
            "preprocessing_and_training_seconds": time.perf_counter() - started,
            "test_date_start": str(test_start), "test_date_end": str(date_values[-1]),
        }
        (config.results / "metrics" / "spark_run_summary.json").write_text(json.dumps(summary, indent=2))
        return summary
    finally:
        # What: Stop the Apache Spark session and release JVM resources.
        # Why: Ensures memory, CPU thread pools, and file handles are cleanly released, even if an exception occurs.
        spark.stop()

