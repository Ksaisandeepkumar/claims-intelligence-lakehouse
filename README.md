# Healthcare Claims Intelligence Lakehouse

A real-world healthcare data engineering project that simulates a claims lakehouse pipeline using Python, PySpark, Parquet, data quality validation, deduplication, and Bronze/Silver/Gold architecture.

## Business Problem

Healthcare organizations process large volumes of claims data from multiple source systems such as EDI 837 feeds, claims platforms, provider systems, and payment systems. Raw claim records can contain missing identifiers, duplicate claim IDs, invalid financial values, and inconsistent source-system records.

This project demonstrates how a data engineer can build a reliable claims pipeline that converts raw operational data into validated, analytics-ready datasets.

## Project Objective

Build a PySpark healthcare claims lakehouse that:

- Ingests raw healthcare claims data
- Stores raw data in a Bronze layer
- Applies data quality validation in a Silver layer
- Removes duplicate claim records
- Filters invalid claim amounts and missing keys
- Creates Gold-level claim metrics for reporting

## Architecture

```text
Raw Claims CSV
        ↓
Bronze Layer
Raw claims stored as Parquet
        ↓
Silver Layer
Validated and deduplicated claims
        ↓
Gold Layer
Claims summary by claim status and source system
```

## Tech Stack

- Python 3.11
- PySpark
- Spark SQL DataFrame API
- Pandas
- Faker
- Parquet
- Data quality validation
- Bronze/Silver/Gold lakehouse design
- Git/GitHub

## Dataset

The generated claims dataset includes:

- claim_id
- member_id
- provider_id
- diagnosis_code
- procedure_code
- service_date
- claim_amount
- allowed_amount
- paid_amount
- claim_status
- source_system
- ingestion_timestamp

The generator also creates intentional duplicate and invalid records to test data quality handling.

## Pipeline Layers

### Bronze Layer

Stores raw claim records with minimal transformation and adds a load timestamp.

### Silver Layer

Applies validation rules:

- claim_id must not be null
- member_id must not be null
- provider_id must not be null
- claim_amount must be greater than zero
- allowed_amount must be non-negative
- paid_amount must be non-negative
- duplicate claim_id records are removed

### Gold Layer

Creates analytics-ready claim metrics:

- claim count
- total claim amount
- total paid amount
- average claim amount
- grouped by claim_status and source_system

## How to Run

```bash
python3.11 -m venv venv
source venv/bin/activate
pip install pyspark pandas faker

python src/generate_claims.py
python src/pyspark_claims_lakehouse.py
python src/project_summary.py
```

## Expected Outputs

```text
data/output/bronze_claims_parquet
data/output/silver_claims_validated_parquet
data/output/gold_claims_summary_parquet
```

## Project Summary Script

`src/project_summary.py` validates the pipeline output by showing:

- raw record count
- Bronze count
- Silver count
- Gold count
- duplicate claim_id proof before and after deduplication
- invalid record proof before and after validation
- sample Gold output

## Key Data Engineering Concepts Demonstrated

- PySpark batch processing
- Explicit schema enforcement
- Data quality validation
- Duplicate record handling
- Lakehouse-style layered architecture
- Healthcare claims domain modeling
- Parquet-based analytical outputs
- Gold reporting layer creation

## Resume Bullet

Built a PySpark healthcare claims lakehouse that ingests raw claim records, validates missing identifiers and financial fields, removes duplicate claim IDs, and publishes Bronze, Silver, and Gold Parquet layers for claims analytics and reporting.
