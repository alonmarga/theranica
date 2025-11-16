# CMS ETL Pipeline - Theranica Assignment

Extract, transform, and load healthcare provider data from CMS API to BigQuery.

## API & Filters

**Endpoint**: `https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0`

**Filters** (configurable in `.env`):
- States: ny,tx
- Specialties: Neurology,INTERNAL MEDICINE
- Filter combinations: 2 states × 2 specialties = 4 API requests with pagination 

Each request (batch) has limit 1500 records (configurable) and max record of 3000 (configurable as well)

---

## ETL Flow

```
CMS API → Extract (pagination) → Transform (validation) → Load (BigQuery)
```

### Extract
- POST requests with filter conditions
- Pagination: `$limit` (batch size)
- Adds `filter_combination` field for data lineage

### Transform
1. **Column standardization**: Map API names to readable names
2. **Type conversion**: String, integer, boolean, date handling
3. **Data quality validation**: NPI (10 digits), State (2-letter), Zip (5/9 digit formats)
4. **Table split**: Normalize into 2 tables (clinicians + practice_locations)
5. **Lineage**: Add `load_id` (batch ID) and `filter_combination`

### Load
- `WRITE_APPEND` mode (accumulate data, no truncation)
- Time-partition by `ingestion_timestamp` (daily)
- Verify data with aggregate queries

---

## Design Decisions

| Decision | Reason |
|---|---|
| **Two tables** | Normalize: clinicians (dimension) + practice_locations (fact). One NPI has multiple locations. |
| **WRITE_APPEND** | Historical tracking. Each run appends, doesn't overwrite. |
| **load_id + filter_combination** | Full audit trail: when loaded and which filter fetched each record. |
| **is_valid_record flag** | Mark invalid (don't delete) for quality analysis. |
| **Time-partitioning** | Query performance on recent data. |

---

## Running

### Setup

```bash
# 1. Configure .env
BASE_URL=https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0
STATES=ny,tx
SPECIALTIES=Neurology,INTERNAL MEDICINE
GCP_PROJECT_ID=your-project
DATASET_ID=cms_etl_pipeline
GCP_CREDENTIALS_PATH=./app/gcp-credentials.json

# 2. Place GCP credentials
# Download service account JSON → ./app/gcp-credentials.json
```

### How to run?
For the purpose of the assigment, I assumed the ETL is one time action, so run it with docker compose run.  
With this, you can send flags as describe below. On production envirment the implemention and assumptions would be different.

```bash
# Normal ETL (extract → transform → load)
docker compose run cms-etl-pipeline

# ETL + Export to CSV
docker compose run cms-etl-pipeline python -m app.main --with-export

# Export only (no ETL, uses existing BQ data)
docker compose run cms-etl-pipeline python -m app.main --export-only

# Include invalid records (for testing)
docker compose run cms-etl-pipeline python -m app.main --include-invalid

# Combine flags
docker compose run cms-etl-pipeline python -m app.main --with-export --include-invalid
```

### Alternative: `docker compose up`
---

## Command-Line Flags

| Flag | Description | Example |
|---|---|---|
| `--with-export` | Run ETL pipeline, then export first 100 rows to CSV | `docker compose run cms-etl-pipeline python -m app.main --with-export` |
| `--export-only` | Skip ETL; only export existing BigQuery data to CSV (fast) | `docker compose run cms-etl-pipeline python -m app.main --export-only` |
| `--include-invalid` | Keep records marked as invalid (is_valid_record=False) instead of filtering them out | `docker compose run cms-etl-pipeline python -m app.main --include-invalid` |

**Combinations:**
```bash
# ETL + Export + Keep invalid records
docker compose run cms-etl-pipeline python -m app.main --with-export --include-invalid

# Export only
docker compose run cms-etl-pipeline python -m app.main --export-only
```

**Output files:**
- `data_export/sample_clinicians.csv`
- `data_export/sample_practice_locations.csv`

---

## BigQuery Schema

### `clinicians` (Dimension)
- **Key**: `npi` (deduplicated: 1 provider per NPI)
- Fields: npi, first_name, last_name, credentials, medical_specialty, gender, is_valid_record
- Lineage: load_id, filter_combination, ingestion_timestamp (partition key)

### `practice_locations` (Fact)
- **Key**: npi + state + city + zip_code (one provider can practice in multiple locations)
- Fields: npi, state, city, zip_code, street_address, phone, organization_name, enrollment_status, accepts_medicare, accepts_medicaid, is_valid_record
- Lineage: load_id, filter_combination, ingestion_timestamp (partition key)

---

## Validation Rules

| Field | Valid | Invalid |
|---|---|---|
| NPI | 10 digits, non-zero | < 10 digits, zero |
| State | 2-letter uppercase (NY, PA, CA) | 3+ letters, lowercase |
| Zip | 5 digits OR 9 digits OR XXXXX-XXXX | < 5 digits |


---

## Files Generated

- `data_export/sample_clinicians.csv` - First N (configurable) rows (clinicians)
- `data_export/sample_practice_locations.csv` - First N (configurable) rows (locations)
- `pipeline.log` - Full execution log

---

---

## Production Considerations

This assignment uses **CLI flags** (`--with-export`, `--export-only`, `--include-invalid`) with `docker compose run` for one-off execution.

**For production, the architecture should change:**

### Production Approach
Instead of CLI flags, implement a **REST API server** that:
- Runs as a long-running service (`docker compose up`)
- Exposes HTTP endpoints to **trigger pipeline variations**
- Accepts configuration as **JSON request body**

**Example production API:**
```python
# main_server.py (Flask/FastAPI)
from flask import Flask, request

app = Flask(__name__)

@app.route('/api/etl/run', methods=['POST'])
def run_etl():
    """
    POST /api/etl/run
    {
      "with_export": true,
      "include_invalid": false,
      "max_records": 10000
    }
    """
    config = request.json
    pipeline = ETLPipeline(Config())
    success = pipeline.run(
        with_export=config.get('with_export', False),
        include_invalid=config.get('include_invalid', False)
    )
    return {'status': 'success' if success else 'failed'}

@app.route('/api/etl/status', methods=['GET'])
def get_status():
    """Check pipeline status"""
    return {'status': 'idle'} # or 'running', 'completed', etc.

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

**Production docker-compose:**
```yaml
services:
  cms-etl-pipeline:
    build: .
    ports:
      - "5000:5000"
    environment:
      - FLASK_ENV=production
    volumes:
      - ./logs:/app/logs
    restart: always
```

**Production usage:**
```bash
# Trigger ETL with export
curl -X POST http://localhost:5000/api/etl/run \
  -H "Content-Type: application/json" \
  -d '{"with_export": true, "include_invalid": false}'

# Check status
curl http://localhost:5000/api/etl/status
```

This allows:
- Scheduling (cron job → HTTP call)
- Monitoring & dashboards
- Queuing/backpressure handling
- Authentication/authorization
- Better error handling & retries

---

## Project Structure

```
app/
  ├── config.py          # Config + GCP credentials setup
  ├── extract.py         # CMS API extraction
  ├── transform.py       # Data cleaning & validation
  ├── load.py            # BigQuery loading
  ├── export_data.py     # CSV export
  └── main.py            # Pipeline orchestration (CLI args)
```