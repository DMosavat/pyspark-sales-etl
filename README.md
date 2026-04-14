# PySpark Sales ETL

A portfolio-ready ETL project built with PySpark.  
This project reads raw sales CSV files, transforms the data, calculates business metrics, and writes analytics-ready Parquet outputs.

## Features

- Read raw CSV data with PySpark
- Clean and transform sales data
- Filter completed orders
- Calculate order totals
- Build customer and country sales KPIs
- Generate product-level sales analytics
- Write partitioned Parquet outputs

## Project Structure

```text
pyspark-sales-etl/
├── data/
│   └── sample/
├── output/
├── src/
│   ├── session.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── pipeline.py
├── tests/
├── .gitignore
├── README.md
├── requirements.txt
└── run.py
```
