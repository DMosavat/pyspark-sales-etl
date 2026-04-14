![Python](https://img.shields.io/badge/Python-3.10-blue)
![PySpark](https://img.shields.io/badge/PySpark-Enabled-orange)
![Delta](https://img.shields.io/badge/Delta-Lake-green)

# PySpark Sales ETL

A portfolio-ready ETL project built with PySpark.  
This project reads raw sales CSV files, transforms the data, builds a medallion architecture (Bronze/Silver/Gold), and writes analytics-ready outputs in both Parquet and Delta Lake formats.

## Project Motivation

This project was built to practice real-world data engineering concepts using PySpark and Delta Lake, and to prepare for working with Azure Databricks.

## Use Case

This project simulates a real-world retail data pipeline where:

- Raw sales data is ingested from CSV files
- Data is cleaned and standardized
- Business metrics are generated
- Incremental updates are applied using Delta Lake

This design mirrors production pipelines in Azure Databricks environments.

## Key Features

- PySpark DataFrame transformations
- Delta Lake integration
- Incremental data processing (merge/upsert)
- Medallion architecture (Bronze/Silver/Gold)
- Partitioned Parquet and Delta outputs
- Logging and configuration management
- Unit testing with pytest

## Architecture

```text
        ┌──────────────┐
        │   Raw Data   │
        │  CSV Files   │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │   Bronze     │
        │ Raw Delta    │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │   Silver     │
        │ Cleaned Data │
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │    Gold      │
        │ Aggregations │
        └──────────────┘
```

## Data Flow

1. Read raw CSV files
2. Store raw data in Bronze layer (Delta)
3. Clean and transform data into Silver layer
4. Generate business metrics in Gold layer
5. Write outputs as Parquet and Delta
6. Perform incremental updates using Delta merge

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

## Folder Structure

- `data/` → sample input data
- `src/` → ETL pipeline code
- `tests/` → unit tests
- `output/` → parquet outputs
- `output_delta/` → Delta outputs
- `medallion/` → Bronze, Silver, Gold layers

## Configuration

The project uses a centralized configuration file for managing input and output paths:

```text
src/config.py
```

## Logging

The pipeline uses Python logging for execution messages.

## Running Tests

```bash
pytest
```

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

## Example Output

### Sales Per Customer

| customer_id | name    | country | total_sales |
| ----------- | ------- | ------- | ----------- |
| 1           | Alice   | Sweden  | 520         |
| 3           | Charlie | Sweden  | 360         |

## Tech Stack

- Python
- PySpark
- Parquet
- Delta Lake

## Quick Start

```bash
git clone https://github.com/your-username/pyspark-sales-etl.git
cd pyspark-sales-etl

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python run.py
```
