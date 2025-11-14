import os

from dotenv import load_dotenv

load_dotenv()

# GCP Credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\alonm\Downloads\cms-pipeline-16bfeddf7657.json"


class Config:
    """Centralized configuration for the ETL pipeline."""

    base_url = os.environ['BASE_URL']

    # Filter settings
    STATES = [s.strip().upper() for s in os.environ['STATES'].split(',')]
    SPECIALTIES = [s.strip() for s in os.environ['SPECIALTIES'].split(',')]

    # Pagination settings
    BATCH_SIZE = int(os.environ['BATCH_SIZE'])

    # Handle MAX_RECORDS - can be None or an integer
    _max_records_env = os.environ.get('MAX_RECORDS', '').strip()
    if _max_records_env and _max_records_env.upper() != 'NONE':
        try:
            MAX_RECORDS = int(_max_records_env)
        except ValueError:
            MAX_RECORDS = None
    else:
        MAX_RECORDS = None

    # GCP Configuration
    PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    DATASET_ID = os.environ["DATASET_ID"]
    DATASET_LOCATION = os.environ['DATASET_LOCATION']
    DATASET_DESCRIPTION = "Theranic ETL home task for DE"

    # BigQuery table names
    CLINICIANS_TABLE = os.environ['CLINICIANS_TABLE']
    LOCATIONS_TABLE = os.environ['LOCATIONS_TABLE']

    # Data quality settings
    DEDUPLICATE_ON = [s.strip() for s in os.environ['DEDUPLICATE_ON'].split(',')]
    VALIDATION_RULES = {
        "npi": "must be numeric and 10 digits",
        "state": "must be 2-letter state code",
        "zip_code": "must be 5 digits or XXXXX-XXXX format"
    }

    # Logging
    LOG_LEVEL = "INFO"



    @classmethod
    def validate(cls):
        """Validate required configuration."""
        if not cls.PROJECT_ID:
            raise ValueError(
                "GCP_PROJECT_ID environment variable is not set. "
                "Please set it before running the pipeline."
            )
        if not cls.STATES or not cls.SPECIALTIES:
            raise ValueError("STATES and SPECIALTIES must not be empty.")
        return True
