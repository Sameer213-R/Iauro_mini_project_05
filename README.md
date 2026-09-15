# Iauro_mini_project_05
# Airflow API Data Ingestion & Pandas Transformation

A learning ETL project using Apache Airflow to fetch JSON data from GitHub, store the raw data, process it with Pandas, and save the processed output.

## Architecture

```text
Master DAG
    |
    v
Fetching DAG
    |
    v
Raw JSON File
    |
    v
Pandas Transformation DAG
    |
    v
Processed JSON File
