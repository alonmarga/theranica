"""
Config file to set in one central place ETL configuration
"""

import os

# GCP Credentials, not best or even good practice, for this assignment only.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/app/gcp-credentials.json'


class Config:
    """Centralized configuration for the ETL pipeline."""

    base_url:str = os.environ['BASE_URL']

    # Filter settings
    FILTERS:dict = {
        "state": [s.strip().upper() for s in os.environ['STATES'].split(',')],
        "pri_spec": [s.strip() for s in os.environ['SPECIALTIES'].split(',')]
    }

    # Pagination settings
    BATCH_SIZE:int = int(os.environ['BATCH_SIZE'])

    # Handle MAX_RECORDS - can be None or an integer
    _max_records_env:str = os.environ.get('MAX_RECORDS', '').strip()
    if _max_records_env and _max_records_env.upper() != 'NONE':
        try:
            MAX_RECORDS = int(_max_records_env)
        except ValueError:
            MAX_RECORDS = None
    else:
        MAX_RECORDS = None

    # GCP Configuration
    PROJECT_ID:str = os.environ["GCP_PROJECT_ID"]
    DATASET_ID:str = os.environ["DATASET_ID"]
    DATASET_LOCATION:str = os.environ['DATASET_LOCATION']
    DATASET_DESCRIPTION:str = "Theranica ETL home assignment for DE"

    # BigQuery table names
    CLINICIANS_TABLE:list = os.environ['CLINICIANS_TABLE']
    LOCATIONS_TABLE:list = os.environ['LOCATIONS_TABLE']

    # Data quality settings
    DEDUPLICATE_ON:list = [s.strip() for s in os.environ['DEDUPLICATE_ON'].split(',')]
    VALIDATION_RULES:dict = {
        "npi": "must be numeric and 10 digits",
        "state": "must be 2-letter state code",
        "zip_code": "must be 5 digits or XXXXX-XXXX format"
    }

    SAMPLE_SIZE_TO_EXPORT:str = os.environ['SAMPLE_SIZE_TO_EXPORT']
    DATA_DIR_TO_EXPORT:str = f'/app/{os.environ.get("DATA_DIR_TO_EXPORT", "data_export")}'

    @classmethod
    def validate(cls):
        # Example of validation for project id
        if not cls.PROJECT_ID:
            raise ValueError(
                "GCP_PROJECT_ID environment variable is not set. "
                "Please set it before running the pipeline."
            )

        return True
