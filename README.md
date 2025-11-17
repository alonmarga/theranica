# CMS ETL Pipeline  

---
Assignment Context: This project demonstrates a complete ETL pipeline for healthcare provider data, built for evaluation at Theranica

---  
Healthcare provider data pipeline that extracts clinician information from the CMS API, validates and cleans the data, then loads it into BigQuery with complete audit trails.

## Overview

1. **Extracts** provider data from CMS API with configurable filters
2. **Validates** data quality (NPI format, state codes, zip codes)
3. **Loads** cleaned data into BigQuery
4. **Archives** raw API data to Cloud Storage for compliance
5. **Tracks** every run with metrics and statistics

---

## Requirements

### Google Cloud Project

You need a GCP project with:
- BigQuery API enabled
- Cloud Storage API enabled
- Service account with appropriate permissions (BigQuery admin, Storage admin)
- Service account JSON credentials file saved as `credentials.json` in the project app folder

### CMS API

The pipeline connects to the Centers for Medicare & Medicaid Services (CMS) provider data API:
- **Endpoint:** `https://data.cms.gov/resource/mj5m-pzi6.json`
- **Data:** National healthcare provider registry (NPIs, credentials, practice locations, specialties)
- **Access:** Public endpoint, no authentication required
- **Rate limiting:** Respects API rate limits with batch processing (max 1500 per request)

### Environment Configuration

Create `.env` file from `env.template` with your GCP project details. For this assignment, some values are hardcoded in the code (e.g., API endpoint, table schemas) rather than externalized. In production, these would be moved to configuration.

---

## Project Structure

```
theranica/
├── app/
│   ├── __init__.py
│   ├── main.py              # Entry point, orchestrates pipeline
│   ├── config.py            # Configuration from .env
│   ├── extract.py           # CMS API extraction module
│   ├── transform.py         # Data cleaning and validation
│   ├── load.py              # BigQuery loading and metrics
│   ├── export_data.py       # CSV export module
|   ├── credentials.json         # GCP service account (not in repo)
|   |schemas_and_columns_mapping/
│   |    ├── api_mapping/
│   |    │   └── api_columns.json # Column name mappings
│   |    ├── schemas_and_columns_mapping/
│   |    │   └── gcp_schemas/     # BigQuery table schemas
│   └── logs/
│       └── pipeline.log     # Execution logs
├── docker-compose.yaml      # Docker container orchestration
├── Dockerfile               # Container image definition
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (not in repo)
├── env.template             # Template for .env file
└── README.md                <-- you're here
```

---

## Running the Pipeline

### Basic Execution

```bash
# Extract, transform, load only
docker compose run cms-etl-pipeline

# View all available options
docker compose run cms-etl-pipeline python -m app.main --help
```

### With Flags

#### `--upload-raw`

Saves original CMS API responses to Cloud Storage before transformation. Creates timestamped archive: `gs://bucket/raw_data/YYYYMMDD/data_HHMMSS.json`

Useful for compliance, audit trails, and data recovery.

```bash
docker compose run cms-etl-pipeline python -m app.main --upload-raw
```

#### `--with-export`

Exports sample CSV files to `data_export/` for "feeling" the data more easly.

```bash
docker compose run cms-etl-pipeline python -m app.main --with-export
```

#### `--include-invalid`

Loads records that fail data validation checks (bad NPI, invalid state code, improper zip). Default: invalid records are marked but not loaded.

```bash
docker compose run cms-etl-pipeline python -m app.main --include-invalid
```

#### Combining Flags

```bash
docker compose run cms-etl-pipeline python -m app.main --upload-raw --with-export --include-invalid
```

---

## Data Flow

```
START
  ↓
EXTRACT (CMS API)
  └─ Fetch clinician and location records based on filters
  └─ Add filter_combination metadata to each record
  ↓
(Optional) UPLOAD RAW TO CLOUD STORAGE
  └─ Save original JSON responses with load_id
  ↓
TRANSFORM
  ├─ Standardize column names (API names → business names)
  ├─ Convert data types (dates, numbers, text)
  ├─ Validate quality (NPI, state code, zip code)
  ├─ Mark invalid records as is_valid_record=false
  └─ Create separate clinicians and practice_locations DataFrames
  ↓
LOAD TO BIGQUERY
  ├─ Create dataset if needed
  ├─ Create/append to clinicians table
  ├─ Create/append to practice_locations table
  └─ Log metrics to etl_processes table
  ↓
(Optional) EXPORT TO CSV
  └─ Sample CSV files for stakeholder review
  ↓
LOG METRICS
  └─ Record run timestamp, record counts, status, duration
  ↓
END
```

---

## BigQuery Tables

**clinicians**
- Provider demographic and credential information (npi,first name, last name gender, credential etc)
- Grows with each pipeline run (data appends, never overwrites)
- Includes `is_valid_record` flag, `load_id` for tracing origin

**practice_locations**
- Information about where providers practice (npi,address, phone, organization name, state etc)
- Accumulates across runs (historical data preserved)
- Includes `is_valid_record` flag, `load_id` for tracing origin

**etl_processes**
- Audit trail and metrics for every pipeline execution
- One row per run with: timestamp, records extracted/loaded, validation counts, status, duration, error messages, filters used
- Allows monitoring pipeline health and history



## Cloud Storage

When using the --upload-raw flag, raw API responses are archived to Cloud Storage as NDJSON files organized by date. This provides compliance documentation and enables data recovery if BigQuery tables are modified or corrupted.

---

## Entry Point

**Main** (`app/main.py`)
- Orchestrates the complete pipeline execution
- Parses command-line flags (`--upload-raw`, `--with-export`, `--include-invalid`)
- Initializes configuration from `.env` file
- Creates instances of extractor, transformer, loader, and exporter components
- Handles error management and logging

---

## Components

**Extractor** (`app/extract.py`)
- Connects to CMS API
- Builds filter combinations
- Fetches data in batches with pagination
- Tags each record with filter metadata for data lineage

**Transformer** (`app/transform.py`)
- Maps API column names to standard business names
- Converts data types appropriately
- Validates records against quality rules
- Creates separate clinicians and locations tables
- Deduplicates records

**Loader** (`app/load.py`)
- Creates BigQuery dataset and tables (if needed)
- Appends data without overwriting
- Logs comprehensive metrics
- Optionally archives raw data to Cloud Storage

**Exporter** (`app/export_data.py`)
- Exports sample data from BigQuery tables
- Saves as CSV for sharing and analysis
- Excludes etl_processes table from exports

---

## Architecture & Design Patterns

The pipeline is built using **Object-Oriented Programming (OOP)** principles:

**Encapsulation:** Each component (Extractor, Transformer, Loader, Exporter) is a separate class with well-defined responsibilities. Internal details are hidden; only necessary methods are exposed.

**Separation of Concerns:** Each class handles one specific part of the pipeline:
- Extraction concerns (API connectivity, pagination)
- Transformation concerns (data cleaning, validation)
- Loading concerns (BigQuery schema, data appending)
- Exporting concerns (CSV generation)

**Modularity:** Components are independent and can be tested, modified, or replaced without affecting others. The main orchestrator coordinates them using a clear interface.

**Configuration Management:** The `Config` class centralizes all environment variables and settings, making the pipeline configurable without code changes.

This design makes the code maintainable, testable, and easy to extend with new features.

---