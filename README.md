# RetailPulse – Distributed Retail Analytics & Demand Forecasting

RetailPulse is a Big Data analytics and demand forecasting project built using **Python, Apache Spark, and PySpark**. It processes Walmart retail store datasets through ETL workflows, performs distributed data analysis, and applies machine learning models to forecast retail demand.

## Project Overview

Retail businesses generate large volumes of transactional data across stores, products, and time periods. Analyzing this data efficiently helps identify sales patterns and improve demand forecasting.

RetailPulse uses distributed data processing and machine learning to transform raw retail data into meaningful insights and demand predictions.

## Key Features

- **ETL Pipeline:** Extracts, cleans, transforms, and integrates retail datasets.
- **Batch Processing:** Processes Walmart retail datasets using PySpark.
- **Distributed Processing:** Uses Apache Spark RDDs for distributed data operations.
- **SQL Analytics:** Performs data aggregation and querying using Spark SQL.
- **Feature Engineering:** Generates lag features and rolling statistics for demand forecasting.
- **Machine Learning:** Applies Decision Tree, Random Forest, and Gradient-Boosted Tree models.
- **Model Evaluation:** Evaluates forecasting performance using MAE, RMSE, and WAPE.

## Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python |
| Big Data | Apache Spark, PySpark |
| Processing | Spark RDDs, Spark SQL |
| Machine Learning | Spark MLlib |
| Data Processing | ETL, Data Cleaning, Feature Engineering |
| Evaluation | MAE, RMSE, WAPE |

## Dataset

The project uses the **Walmart Retail Store dataset**, containing retail information used for demand analysis and forecasting.

The datasets include:
- Sales data
- Store and product information
- Calendar data
- Pricing data

## Project Workflow

1. **Data Extraction:** Load Walmart retail datasets.
2. **Data Cleaning:** Handle missing values, inconsistencies, and prepare datasets.
3. **Data Integration:** Combine relevant datasets for analysis.
4. **ETL Processing:** Apply transformations using Python and PySpark.
5. **Distributed Analytics:** Perform RDD operations and SQL-based aggregations.
6. **Feature Engineering:** Generate lag-based and rolling statistical features.
7. **Model Training:** Train machine learning models using Spark MLlib.
8. **Evaluation:** Compare model performance using forecasting metrics.

## Machine Learning Models

The following models are used for demand forecasting:

- Decision Tree Regressor
- Random Forest Regressor
- Gradient-Boosted Tree Regressor

## Evaluation Metrics

| Metric | Purpose |
|---|---|
| MAE | Measures average absolute prediction error |
| RMSE | Measures prediction error while penalizing larger errors |
| WAPE | Measures weighted absolute percentage error |

## Project Structure

```text
RetailPulse/
│
├── data/
│   └── Walmart retail datasets
│
├── notebooks/
│   └── Data processing and forecasting notebooks
│
├── src/
│   ├── etl/
│   ├── processing/
│   ├── feature_engineering/
│   └── modeling/
│
├── README.md
└── requirements.txt
```

*Note: Update the folder structure to match the actual files in your repository.*

## Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/AbhiGen/RetailPulse-Distributed-Retail-Analytics-Demand-Forecasting.git
```

### 2. Navigate to the project directory

```bash
cd RetailPulse-Distributed-Retail-Analytics-Demand-Forecasting
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the project

Open the project notebooks and execute the data processing, feature engineering, and model training workflows in sequence.

## Future Improvements

- Integrate Apache Airflow for workflow orchestration.
- Explore real-time data ingestion and streaming.
- Optimize distributed processing performance.
- Extend forecasting models for larger retail datasets.

## Author

**Abhiram Bikkina**

- GitHub: https://github.com/AbhiGen
- LinkedIn: https://www.linkedin.com/in/abhiram-bikkina-96a342282/

---

*RetailPulse – Turning retail data into actionable insights through distributed analytics and machine learning.*