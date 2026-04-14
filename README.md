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

## Configuration

The project uses a central configuration file for input and output paths:

src/config.py

## Logging

The pipeline uses Python logging for execution messages.

Running Tests

- pytest

## Tech Stack

- Python
- PySpark
- Parquet
- Delta Lake (next step)

## How to Run

- python3 -m venv venv
- source venv/bin/activate
- pip install -r requirements.txt
- python run.py

## Sample Outputs

- Sales per customer
- Sales per country
- Product sales
- Order summary
