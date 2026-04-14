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
├── output_delta/
├── src/
│   ├── medallion.py
│   ├── config.py
│   ├── incremental.py
│   ├── load_delta.py
│   ├── read_delta.py
│   ├── logger.py
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

## Delta Lake Support

This project also writes analytics outputs in Delta Lake format.

Delta outputs are written to:

output_delta/

## Incremental Delta Load

This project includes a simple incremental load example using Delta Lake merge.

### Example behavior

- Existing customer records are updated
- New customer records are inserted

### Delta merge target

output_delta/customers/

## Medallion Architecture

This project follows a simple medallion architecture:

- Bronze: raw source data
- Silver: cleaned and transformed data
- Gold: business-ready analytics tables

### Medallion Output Structure

```text
medallion/
├── bronze/
├── silver/
└── gold/
```

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

```

```
