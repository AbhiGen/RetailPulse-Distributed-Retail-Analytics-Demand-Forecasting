# RetailPulse
### Distributed Retail Analytics & Demand Forecasting

**RetailPulse** is a distributed data analytics and demand forecasting project built using **Apache Spark and PySpark**. It processes Walmart retail datasets through ETL workflows, performs distributed analytics, and applies machine learning techniques to identify demand patterns and generate forecasts.

---

## Overview

RetailPulse explores how distributed data processing can be applied to large-scale retail analytics. The project integrates multiple retail datasets, performs data preparation and feature engineering, and evaluates machine learning models for demand forecasting.

The system focuses on scalable data processing, structured data transformation, and analytical model evaluation.

## Key Capabilities

- **ETL Processing:** Data extraction, cleaning, transformation, and integration.
- **Distributed Computing:** Parallel data processing using Apache Spark and PySpark.
- **RDD Analytics:** Distributed transformations and aggregations using Spark RDDs.
- **SQL Analytics:** Structured querying and aggregation using Spark SQL.
- **Feature Engineering:** Lag-based features and rolling statistical calculations.
- **Demand Forecasting:** Machine learning-based retail demand prediction.
- **Model Evaluation:** Performance comparison using MAE, RMSE, and WAPE.

## Architecture

```text
       Walmart Retail Datasets
                  |
                  v
          Data Ingestion
                  |
                  v
           ETL Pipeline
       (Cleaning & Integration)
                  |
                  v
       Distributed Processing
          (Apache Spark)
                  |
          +-------+-------+
          |               |
          v               v
      Spark RDDs       Spark SQL
          |               |
          +-------+-------+
                  |
                  v
          Feature Engineering
       (Lag & Rolling Features)
                  |
                  v
          Machine Learning
             (MLlib)
                  |
                  v
          Model Evaluation
          (MAE, RMSE, WAPE)
                  |
                  v
          Demand Forecasting
```

## Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Python |
| Distributed Processing | Apache Spark, PySpark |
| Data Processing | Spark RDDs, Spark SQL |
| Machine Learning | Spark MLlib |
| Data Workflow | ETL |
| Evaluation | MAE, RMSE, WAPE |

## Dataset

The project uses the **Walmart Retail Store dataset** for retail analytics and demand forecasting.

The data sources include:

- Retail sales records
- Store and product information
- Calendar information
- Pricing information

These datasets are integrated and transformed to prepare a consistent analytical dataset.

## Data Processing Workflow

| Stage | Description |
|---|---|
| Data Ingestion | Load retail datasets into the processing environment. |
| Data Cleaning | Handle missing values and inconsistencies. |
| Data Integration | Combine relevant datasets using common identifiers. |
| ETL Transformation | Prepare structured data for downstream analysis. |
| Distributed Analytics | Perform RDD-based processing and SQL aggregations. |
| Feature Engineering | Generate lag and rolling statistical features. |
| Model Development | Train forecasting models using Spark MLlib. |
| Evaluation | Compare model performance using forecasting metrics. |

## Machine Learning Models

The following regression models are evaluated:

- Decision Tree Regressor
- Random Forest Regressor
- Gradient-Boosted Tree Regressor

### Evaluation Metrics

| Metric | Description |
|---|---|
| MAE | Mean Absolute Error |
| RMSE | Root Mean Squared Error |
| WAPE | Weighted Absolute Percentage Error |

## Repository Structure

```text
RetailPulse/
│
├── data/
│   └── Walmart datasets
│
├── notebooks/
│   └── Analysis and model development
│
├── src/
│   ├── ingestion/
│   ├── etl/
│   ├── processing/
│   ├── feature_engineering/
│   └── modeling/
│
├── requirements.txt
└── README.md
```

*The structure above is illustrative. Update it to reflect the actual repository.*

## Getting Started

### Prerequisites

- Python 3.x
- Apache Spark
- PySpark
- Jupyter Notebook (if using notebooks)

### Installation

**1. Clone the repository**

```bash
git clone https://github.com/AbhiGen/RetailPulse-Distributed-Retail-Analytics-Demand-Forecasting.git
```

**2. Navigate to the project directory**

```bash
cd RetailPulse-Distributed-Retail-Analytics-Demand-Forecasting
```

**3. Install dependencies**

```bash
pip install -r requirements.txt
```

**4. Run the project**

Open the notebooks in the `notebooks/` directory and execute the processing and forecasting workflows in sequence.

## Future Enhancements

- Workflow orchestration using Apache Airflow.
- Real-time ingestion using streaming technologies.
- Data quality monitoring and automated validation.
- Performance optimization for larger datasets.

## Author

**Abhiram Bikkina**

[GitHub](https://github.com/AbhiGen) · [LinkedIn](https://www.linkedin.com/in/abhiram-bikkina-96a342282/)

---

*Built to explore distributed data engineering and machine learning for retail analytics.*