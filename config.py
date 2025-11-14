import os

# Set your credentials here
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\alonm\Downloads\cms-pipeline-16bfeddf7657.json"
os.environ['GCP_PROJECT_ID'] = 'cms-pipeline'
"""
Configuration settings for the CMS ETL Pipeline.
"""

import os
from typing import List


class Config:
    """Centralized configuration for the ETL pipeline."""

    # Data Source - CSV Download (SODA API deprecated)
    CSV_URL = "https://data.cms.gov/provider-data/sites/default/files/resources/mj5m-pzi6_0.csv"

    # Filter scope - choose your states and specialties
    STATES = ["NY", "FL"]  # New York and Florida
    SPECIALTIES = ["INTERNAL MEDICINE", "ANESTHESIOLOGY"]  # Specialties as they appear in pri_spec

    # Pagination settings
    BATCH_SIZE = 1500  # New CMS API limit is 1,500 records per request (was 50,000)
    MAX_RECORDS = 5000  # Limit to 5000 records for testing (set to None for unlimited)

    # GCP Configuration
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")  # Set via environment variable
    DATASET_ID = "cms_clinicians"
    DATASET_LOCATION = "US"
    DATASET_DESCRIPTION = "CMS Doctors and Clinicians National Downloadable File"

    # BigQuery table names
    CLINICIANS_TABLE = "clinicians"
    LOCATIONS_TABLE = "practice_locations"

    # Data quality settings
    DEDUPLICATE_ON = ["npi", "state", "city", "zip_code"]
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