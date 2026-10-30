# Retail Demand Lab

## Retail Sales Forecasting and Demand Spike Detection

<p align="center">
	<strong>A full-stack Big Data case study for turning retail history into a seven-day demand planning decision.</strong><br>
	<a href="#case-study-at-a-glance">Case study</a> ·
	<a href="#run-the-project">Run it</a> ·
	<a href="#dashboard">Dashboard</a> ·
	<a href="docs/REVIEW_GUIDE.md">Review guide</a>
</p>

<p align="center">
	<img src="results/charts/actual_vs_predicted.png" alt="Actual versus predicted sales chart" width="48%">
	<img src="results/charts/daily_sales_trend.png" alt="Daily sales trend chart" width="48%">
</p>

> **Research question:** Can historical product demand, selling price, calendar events, and seasonality predict the next seven days of demand well enough to support inventory planning?

### Testable hypotheses

- **H1:** A forecast model should beat the seasonal-naive baseline on rolling-origin seven-day WAPE.
- **H2:** A model should remain competitive with the strongest simple moving-average benchmark, not only with a weak baseline.
- **H3:** A count-aware spike detector should report measurable precision and recall on injected synthetic spikes.

The current evidence supports H1 for Random Forest at horizons 1-4, supports H2 for the 28-day moving average rather than Random Forest on the single-window benchmark, and reports H3 in `results/metrics/spike_validation.csv`.

This project is built for an academic Big Data Analytics review. It connects data ingestion, quality checks, exploratory analysis, distributed-processing design, machine learning, anomaly detection, performance measurement, a practical decision, and a Streamlit presentation layer.

<p align="center">
	<img src="https://img.shields.io/badge/Status-Complete%20batch%20case%20study-1f6f8b?style=for-the-badge" alt="Complete batch case study">
	<img src="https://img.shields.io/badge/Python-3.14-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.14">
	<img src="https://img.shields.io/badge/PySpark-3.5%20ready-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="PySpark 3.5 ready">
	<img src="https://img.shields.io/badge/Streamlit-dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit dashboard">
</p>

## Case Study At A Glance

| Expected component | How this project addresses it | Evidence |
|---|---|---|
| **1. Real-time data sets** | The supplied data is historical. The project provides a reproducible batch workflow and a clearly labelled seven-day forecast; any future replay of historical rows must be treated as simulated streaming, not live external data. | `Review2/*.csv`, `results/predictions/seven_day_forecast.csv` |
| **2. Preprocessing and EDA** | Validates schemas, numeric fields, nulls, duplicate natural keys, dates, many-to-one joins, missing prices, trends, products, stores, and price relationships. | `analytics/preprocess.py`, `analytics/eda.py`, `results/charts/` |
| **3. Analysis question** | Frames a focused demand-forecasting and inventory-prioritisation question suitable for a 15-minute review. | This README and `docs/REVIEW_GUIDE.md` |
| **4. Spark / PySpark** | The project now has an explicit Spark execution path for DataFrames, window features, chronological evaluation, and Spark MLlib Random Forest. Pandas remains available as a reference engine; no multi-node speedup is claimed. | `run_pipeline.py --engine spark`, `analytics/spark_pipeline.py`, `results/metrics/spark_run_summary.json` |
| **5. Results and visualisation** | Produces seven saved charts plus an interactive Streamlit dashboard for trends, forecasts, spikes, metrics, and recommendations. | `results/charts/`, `dashboard/app.py` |
| **6. Analysis decision** | Prioritise products with high recent or predicted demand for inventory review, while avoiding fabricated stockout or savings claims. | Dashboard and `docs/MODEL_EVALUATION.md` |
| **7. Suitable ML model** | Compares 3 ML models (Decision Tree vs. Random Forest vs. Gradient Boosting) on identical chronological test splits across Pandas and PySpark MLlib. | `analytics/pipeline.py`, `analytics/spark_pipeline.py`, `results/metrics/model_metrics.csv` |
| **8. Performance analysis** | Persists MAE, RMSE, WAPE, preprocessing time, training time, prediction time, row counts, and split dates. | `results/metrics/`, `docs/MODEL_EVALUATION.md` |

### Verified dataset

- **505,032** sales observations and **492,504** total units
- **264** products, **1** store, **1** state, and **1,913** relative days
- **204,234** price observations across **282** weeks
- Calendar coverage from **2011-01-29** to **2016-06-19**
- **0** unmatched calendar rows and **96,789** unmatched price rows, reported and imputed rather than silently discarded

The one-store limitation is intentional project evidence: the store-comparison view is included, but the analysis does not pretend that one store supports cross-store conclusions.

## Architecture

```mermaid
flowchart LR
		A[Review2 CSV exports] --> B[Validation and quality report]
		H[Existing HBase loaders and tables] -. optional backend .-> B
		B --> C[Many-to-one joins]
		C --> D[EDA and aggregate charts]
		C --> E[Lag and rolling features]
		E --> F[Chronological train / validation / test]
		F --> G[7-day historical baseline]
		F --> I[Random Forest regression]
		G --> J[Metrics and comparison]
		I --> J
		I --> K[Recursive seven-day forecast]
		C --> L[Demand spike detection]
		J --> M[Persisted results]
		K --> M
		L --> M
		M --> N[Streamlit dashboard]
		N --> O[Demand-based inventory review decision]
```

The preserved `Review2` Java project loads sales, prices, and calendar records into `retail_sales`, `retail_prices`, and `retail_calendar`. Review3 reads the verified CSV exports for reproducibility and does not modify HBase. The current run reports its actual source and execution mode in `results/metrics/run_summary.json`.

## Run The Project

### 1. Install

```bash
cd ~/Documents/BigData/Review3
python -m venv .venv
.venv/bin/pip install -r requirements.txt
```

The source files remain in `../Review2`; existing loaders, tables, and infrastructure are left intact.

### 2. Execute the analytics pipeline

```bash
.venv/bin/python run_pipeline.py
```

This runs the PySpark pipeline by default and creates the quality report, joined daily dataset, Spark MLlib model, predictions, metrics, and Spark artifacts. Use `--engine pandas` only for the optional reference implementation and Pandas chart generation.

The equivalent explicit Spark command is:

```bash
.venv/bin/python run_pipeline.py --engine spark
```

The Spark run records `engine`, Spark version, master, shuffle partitions, row counts, join counts, and timings in `results/metrics/spark_run_summary.json`. It writes Spark-specific metrics and predictions under `results/metrics/spark_model_metrics.csv` and `results/predictions/spark_test_predictions.csv`.

To run the optional simulated stream:

```bash
.venv/bin/python -m stream.replay --days 28 --interval 0.5
.venv/bin/python -m stream.stream_job
```

This replays historical rows as JSON micro-batches through Spark Structured Streaming. It is explicitly labelled `simulated_historical_replay`, not live data. The interval is adjustable; micro-batch summaries include latency and rows/second, and simulated spike records are written under `results/stream/`, with checkpoint state under `results/checkpoints/`.

### 3. Run tests

```bash
.venv/bin/pytest -q
```

### 4. Open the full-stack dashboard

```bash
.venv/bin/streamlit run dashboard/app.py
```

Then visit **[http://localhost:8501](http://localhost:8501)**. The dashboard reads persisted results and does not retrain during normal page rendering.

The optional API layer can be started separately:

```bash
.venv/bin/uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Available endpoints include `/health`, `/backend-status`, `/metrics`, `/forecast/{item_id}`, `/spikes`, `/decisions`, and `/run-summary`. HBase is now the default required serving backend. Set `SERVING_BACKEND=sqlite` only for an explicit offline fallback. The API reports the active backend and does not retrain models.

To wire the dashboard to the API, start the API and launch Streamlit with `RETAIL_API_URL=http://127.0.0.1:8000`. Without this variable, Streamlit uses its persisted-file fallback and still displays backend status when the API is reachable.

Build the local serving database from generated outputs with `.venv/bin/python run_pipeline.py --build-store`. This creates `results/serving.sqlite3` as a runnable serving-store alternative when HBase is unavailable.

The HBase serving adapter uses new tables `retail_forecasts`, `retail_spikes`, `retail_decisions`, and `retail_metrics`; it never changes the existing `retail_sales`, `retail_prices`, or `retail_calendar` tables. Forecast row keys follow `item_id#store_id#yyyy-mm-dd`, matching the API's item/store/date lookup pattern.

Run the repeatable reference benchmark with `.venv/bin/python run_pipeline.py --benchmark`; it measures two runs at 10%, 50%, and 100% input fractions and writes `results/metrics/performance.csv`.

## Dashboard

The Streamlit application, **Retail Demand Lab**, contains:

- **Overview:** total sales, products, stores, records, date coverage, source, and execution mode
- **Exploratory analysis:** trend, top products, store comparison, and price relationship
- **Forecasting:** product selector, actual-versus-predicted history, and recursive seven-day forecast table
- **Demand spikes:** flagged product-store-date observations, expected demand, score, and explanation
- **Model performance:** baseline versus Random Forest MAE, RMSE, and WAPE
- **Inventory decision support:** demand-based prioritisation without invented inventory quantities or savings
- **Forecast-dependent decisions:** ranks forecast uplift over the recent seven-day demand signal
- **Simulated live replay:** historical micro-batches, throughput counts, and an explicit simulated-data warning
- **Model transparency:** persisted Random Forest feature importance
- **Evaluation diagnostics:** rolling-origin horizon error and residual views
- **Backend API:** FastAPI endpoints over persisted forecasts, spikes, metrics, and decisions

<p align="center">
	<img src="results/charts/model_performance.png" alt="Model performance comparison" width="31%">
	<img src="results/charts/top_products.png" alt="Top products by sales" width="31%">
	<img src="results/charts/demand_spikes.png" alt="Demand spike analysis" width="31%">
</p>

## Results And Decision

The 3 Machine Learning models are evaluated on the identical chronological test window:

| Model Tier | Architecture | MAE | RMSE | WAPE |
|---|---|---:|---:|---:|
| **Gradient Boosting** (Champion) | Boosting Ensemble | **1.0677** | **2.4139** | **0.8990** |
| **Random Forest** | Bagging Ensemble | 1.0699 | 2.4246 | 0.9009 |
| **Decision Tree** | Single Tree Baseline | 1.0726 | 2.4324 | 0.9031 |

The benchmark demonstrates the clear structural progression of tree-based learning:
1. **Decision Tree** establishes the single-tree baseline, but has higher variance (WAPE: 0.9031).
2. **Random Forest** reduces prediction variance via bootstrap aggregation of 80 trees (WAPE: 0.9009).
3. **Gradient Boosting** sequentially minimizes pseudo-residuals with learning-rate shrinkage, achieving the **lowest error across all three metrics** (MAE: 1.0677, RMSE: 2.4139, WAPE: 0.8990).

The defensible business decision is therefore:

> **Deploy Gradient Boosting as the champion production demand forecasting model to drive the seven-day inventory review prioritisation index.**

The decision backtest adds evidence for the forecast-dependent ranking: across four origins, the forecast-uplift top decile had positive realised uplift in three origins, while trailing-volume ranking did not. Correlations were only about `0.06` to `0.10`, so the ranking is a promising review signal, not a validated replenishment policy.

This conclusion is evidence-based and deliberately limited. It does not claim causation from price or events, a live feed, multi-node speedup, stockout probability, or monetary savings.

## Performance Snapshot

Measured local execution from the persisted run summary:

| Stage | Measured time |
|---|---:|
| Preprocessing and feature preparation | approximately 97.8 s |
| Random Forest training | approximately 14.8 s |
| Test prediction | approximately 0.04 s |

These are workstation measurements for the verified local run. They are not a cluster benchmark. The project records input and output row counts so a future Spark or HBase run can be compared fairly.

## Output Map

| Output | Purpose |
|---|---|
| `results/metrics/data_quality.csv` | Input counts, nulls, duplicate keys, and quality checks |
| `results/metrics/model_metrics.csv` | Seven baselines plus Random Forest, scaled and revenue-weighted metrics |
| `results/metrics/demand_bucket_metrics.csv` | Random Forest error by item demand bucket |
| `results/metrics/horizon_metrics.csv` | Rolling-origin error for forecast horizons 1 through 7 |
| `results/metrics/price_availability.csv` | Missing-price rates by product age |
| `results/metrics/feature_importance.csv` | Random Forest feature importance |
| `results/metrics/forecast_uplift_decision.csv` | Forecast-dependent inventory review ranking |
| `results/metrics/decision_backtest.csv` | Forecast ranking versus realised uplift and trailing-volume ranking |
| `results/metrics/weekly_total_metrics.csv` | Seven-day total-demand model comparison |
| `results/metrics/model_evidence.json` | Bootstrap confidence interval and Poisson noise-floor evidence |
| `results/metrics/spike_validation.csv` | Injected-spike precision and recall |
| `results/metrics/performance.csv` | Repeated reference-engine timings at three input sizes |
| `results/serving.sqlite3` | Local serving-store copy of forecasts, spikes, and metrics |
| `results/metrics/run_summary.json` | Execution mode, timings, joins, and split dates |
| `results/predictions/test_predictions.csv` | Historical holdout predictions |
| `results/predictions/seven_day_forecast.csv` | Recursive future forecast rows |
| `results/predictions/demand_spikes.csv` | Transparent trailing-window spike records |
| `results/charts/` | EDA and model visualisations |
| `models/random_forest.pkl` | Persisted trained model and feature list |

## Project Structure

```text
Review3/
├── analytics/
│   ├── preprocess.py       # validation, joins, and leakage-safe features
│   ├── pipeline.py         # training, evaluation, forecast, persistence
│   ├── eda.py              # saved visualisations
│   ├── metrics.py          # MAE, RMSE, and zero-safe WAPE
│   ├── spikes.py           # trailing mean/std demand-spike detection
│   └── spark_support.py    # PySpark availability and local session helper
├── dashboard/app.py        # Streamlit full-stack presentation
├── docs/                   # audit, architecture, evaluation, and review guide
├── results/                # generated charts, metrics, and predictions
├── tests/                  # focused unit tests with synthetic fixtures
├── config.py               # paths and run configuration
├── requirements.txt
└── run_pipeline.py
```

## Review Story In 15 Minutes

1. **Problem:** retail teams need a short-horizon demand signal for planning.
2. **Data:** explain the sales, prices, calendar schemas and one-store limitation.
3. **Preprocessing:** show validation, many-to-one joins, missing-price reporting, and feature history.
4. **EDA:** open the trend, top-product, price, and spike charts.
5. **Question:** can demand history and contextual fields beat a simple seven-day baseline?
6. **ML:** explain the chronological split and Random Forest features without leakage.
7. **Dashboard:** show forecast, spikes, metrics, and the decision-support table.
8. **Performance:** compare MAE/RMSE/WAPE and runtime measurements.
9. **Decision:** prioritise demand review, but request inventory and lead-time data before replenishment automation.
10. **Limitations:** distinguish historical batch data, simulated streaming possibilities, and verified versus unavailable HBase connectivity.

## Documentation

- [Project walkthrough: why, how, and who benefits](docs/PROJECT_WALKTHROUGH.md)
- [Project audit](docs/PROJECT_AUDIT.md)
- [Architecture](docs/ARCHITECTURE.md)
- [EDA findings](docs/EDA_FINDINGS.md)
- [Model evaluation](docs/MODEL_EVALUATION.md)
- [HBase status](docs/HBASE_STATUS.md)
- [Faculty review guide](docs/REVIEW_GUIDE.md)

## Honest Scope Notes

The source shape and date coverage match the processed single-store slice of the public M5 Walmart retail forecasting dataset. The repository uses the provided processed files; verify the original dataset terms before redistribution or commercial use.

- The supplied CSV dataset is historical, not a genuine live real-time feed.
- A replay of historical rows would be **simulated streaming** and must be labelled that way.
- HBase remains the optional NoSQL backend from Review2; Review3 did not alter its tables.
- The default reference run uses Pandas, while `--engine spark` executes the real local Spark path. No distributed Spark speedup is claimed without a multi-node measurement.
- Inventory recommendations are demand-based because inventory levels, lead times, costs, and service targets were not supplied.
#   R e t a i l P u l s e - D i s t r i b u t e d - R e t a i l - A n a l y t i c s - D e m a n d - F o r e c a s t i n g  
 