"""
Config file to set in one central place ETL configuration
"""

import logging
import os

logger = logging.getLogger(__name__)


class Config:
    base_url: str = os.environ.get('BASE_URL')

    # Filter settings
    FILTERS: dict = {
        "state": [s.strip().upper() for s in os.environ.get('STATES').split(',')],
        "pri_spec": [s.strip() for s in os.environ.get('SPECIALTIES').split(',')]
    }

    # Pagination settings
    BATCH_SIZE: int = int(os.environ.get('BATCH_SIZE') or 1000)

    # Handle MAX_RECORDS - can be None or an integer
    _max_records_env: str = os.environ.get('MAX_RECORDS', '').strip()
    if _max_records_env and _max_records_env.upper() != 'NONE':
        try:
            MAX_RECORDS = int(_max_records_env)
        except ValueError:
            MAX_RECORDS = None
    else:
        MAX_RECORDS = None

    # GCP Configuration
    PROJECT_ID: str = os.environ.get("GCP_PROJECT_ID")
    DATASET_ID: str = os.environ.get("DATASET_ID")
    DATASET_LOCATION: str = os.environ.get('DATASET_LOCATION')
    DATASET_DESCRIPTION: str = "Theranica ETL home assignment for DE"
    GCS_BUCKET = os.environ.get('GCS_BUCKET')  # NEW

    # BigQuery table names
    CLINICIANS_TABLE: str = os.environ.get('CLINICIANS_TABLE')
    LOCATIONS_TABLE: str = os.environ.get('LOCATIONS_TABLE')

    # Data quality settings
    DEDUPLICATE_ON: list = [s.strip() for s in os.environ.get('DEDUPLICATE_ON').split(',')]
    VALIDATION_RULES: dict = {
        "npi": "must be numeric and 10 digits",
        "state": "must be 2-letter state code",
        "zip_code": "must be 5 digits or XXXXX-XXXX format"
    }

    SAMPLE_SIZE_TO_EXPORT: str = os.environ.get('SAMPLE_SIZE_TO_EXPORT') or "100"
    DATA_DIR_TO_EXPORT: str = f'/app/{os.environ.get("DATA_DIR_TO_EXPORT", "data_export")}'
    API_COLUMNS_FILE:str = os.environ.get('API_COLUMNS_FILE') or 'api_columns.json'
    GCP_SCHEMAS_DIR:str = os.environ.get('GCP_SCHEMAS_DIR') or 'gcp_schemas'
    SCHEMAS_AND_COLUMNS_MAPPING_DIR: str =os.environ.get('SCHEMAS_AND_COLUMNS_MAPPING_DIR') or 'schemas_and_columns_mapping'


    @classmethod
    def validate(cls):
        # Example of validation for project id
        if not cls.PROJECT_ID:
            raise ValueError(
                "GCP_PROJECT_ID environment variable is not set. "
                "Please set it before running the pipeline."
            )

        return True

    @classmethod
    def setup_gcp_credentials(cls) -> str:
        # Setup GCP credentials from file path in environment variable.
        credentials_path = os.environ.get('GCP_CREDENTIALS_PATH')
        if not credentials_path:
            raise ValueError(
                "GCP_CREDENTIALS_PATH environment variable not set. "
                "Please configure it in your .env file."
            )

        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"GCP credentials file not found at: {credentials_path}")

        logger.info(f"GCP credentials loaded from: {credentials_path}")
        return credentials_path
