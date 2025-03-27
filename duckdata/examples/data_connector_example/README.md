# Data Connector Example

This example demonstrates how to use data connector nodes in PipeLink to connect to various data sources, including:

- Local CSV files
- S3 storage
- DuckDB databases
- PostgreSQL databases
- Apache Arrow Flight servers

The pipeline loads data from multiple sources, performs transformations, and generates reports.

## Pipeline Structure

The pipeline consists of the following nodes:

1. `load_csv_data`: Loads data from a local CSV file
2. `filter_data`: Filters the data using a Python script
3. `load_from_s3`: Loads product data from an S3 bucket
4. `query_duckdb`: Runs a SQL query in DuckDB to join filtered data with product data
5. `connect_to_postgres`: Queries a PostgreSQL database for customer information
6. `generate_report`: Generates a customer spending report
7. `flight_query`: Queries an Apache Arrow Flight server to get VIP customers

## Prerequisites

To run this example, you'll need:

- PipeLink installed with all dependencies
- For PostgreSQL: A running PostgreSQL server with appropriate credentials
- For S3: AWS credentials with access to the S3 bucket
- For Flight: A running Arrow Flight server

## Setup

1. Create the sample CSV data:

```bash
mkdir -p data
cat > data/sample.csv << EOF
id,customer_id,product_id,value,quantity
1,1001,5001,120,2
2,1002,5002,85,1
3,1001,5003,250,3
4,1003,5001,120,1
5,1004,5002,85,5
6,1002,5004,300,2
7,1003,5003,250,1
8,1005,5001,120,4
9,1001,5002,85,2
10,1004,5003,250,2
EOF
```

2. Create a DuckDB database with sample data:

```bash
mkdir -p data
duckdb data/analytics.duckdb << EOF
CREATE TABLE products (
  id INTEGER,
  name VARCHAR,
  price DECIMAL(10,2)
);

INSERT INTO products VALUES
  (5001, 'Widget A', 60.00),
  (5002, 'Widget B', 85.00),
  (5003, 'Widget C', 75.00),
  (5004, 'Widget D', 150.00);

-- Export as Parquet if you want to test S3 functionality
COPY products TO 'data/products.parquet' (FORMAT PARQUET);
EOF
```

3. For PostgreSQL, create the required tables:

```sql
CREATE TABLE customers (
  customer_id INTEGER PRIMARY KEY,
  customer_name VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(20)
);

INSERT INTO customers VALUES
  (1001, 'John Smith', 'john@example.com', '555-1234'),
  (1002, 'Jane Doe', 'jane@example.com', '555-5678'),
  (1003, 'Bob Johnson', 'bob@example.com', '555-9012'),
  (1004, 'Alice Williams', 'alice@example.com', '555-3456'),
  (1005, 'Charlie Brown', 'charlie@example.com', '555-7890');
```

4. For Arrow Flight, you can use the [SQLFlite](https://github.com/voltrondata/sqlflite) project to set up a Flight SQL server with DuckDB.

## Running the Example

Modify the `pipeline.yml` file to adjust connection settings for your environment, then run:

```bash
pipelink pipeline.yml
```

## Notes

- For S3 access, you should use environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) or AWS profiles rather than hardcoded credentials.
- For production use, sensitive connection information should be stored securely and not in the pipeline configuration file.
- This example demonstrates parallel data processing from multiple sources, showing the flexibility of PipeLink's DAG-based execution. 