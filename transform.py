"""
Transform module for cleaning and standardizing CMS data.
Includes deduplication, data type conversions, and data quality validation.
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms and cleans CMS clinician data."""

    def __init__(self, config):
        self.config = config

    def transform(self, raw_data: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main transformation pipeline.

        Args:
            raw_data: Raw records from CMS API

        Returns:
            Tuple of (clinicians_df, locations_df)
        """
        logger.info("Starting data transformation...")

        # Convert to DataFrame
        df = pd.DataFrame(raw_data)
        logger.info(f"Converted {len(df)} records to DataFrame")
        logger.info(f"Actual columns from API: {df.columns.tolist()}")
        logger.debug(f"First row sample: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")

        # Clean and standardize
        df = self._standardize_columns(df)
        df = self._clean_data_types(df)
        df = self._validate_data_quality(df)

        # Separate into two tables based on grain
        clinicians_df = self._create_clinicians_table(df)
        locations_df = self._create_locations_table(df)

        logger.info(f"Transformation complete: {len(clinicians_df)} clinicians, "
                   f"{len(locations_df)} locations")

        return clinicians_df, locations_df

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names from API format."""
        logger.info("Standardizing column names...")

        # Map of API column names to standardized names (from the provided request/response)
        column_mapping = {
            'npi': 'npi',
            'provider_first_name': 'first_name',
            'provider_last_name': 'last_name',
            'provider_middle_name': 'middle_name',
            'cred': 'credentials',
            'pri_spec': 'medical_specialty',
            'gndr': 'gender',
            'ind_assgn': 'accepts_medicare',
            'grp_assgn': 'accepts_medicaid',
            'facility_name': 'organization_name',
            'adr_ln_1': 'street_address',
            'adr_ln_2': 'street_address_2',
            'citytown': 'city',
            'state': 'state',
            'zip_code': 'zip_code',
            'telephone_number': 'phone',
            'enrollment_status': 'enrollment_status',
            'enrollment_date': 'enrollment_date',
            'last_update_date': 'last_update_date'
        }

        # Only keep columns that exist in the data
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_columns)
        df = df[[v for v in existing_columns.values()]]

        logger.info(f"Standardized to {len(df.columns)} columns")
        return df

    def _clean_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate data types."""
        logger.info("Converting data types...")

        # String columns - strip whitespace and convert to uppercase where appropriate
        string_cols = ['first_name', 'last_name', 'middle_name', 'credentials',
                      'medical_specialty', 'gender', 'organization_name',
                      'street_address', 'street_address_2', 'city', 'state']

        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                # Keep state and specialty uppercase
                if col in ['state', 'medical_specialty']:
                    df[col] = df[col].str.upper()

        # NPI should be numeric (10 digits)
        if 'npi' in df.columns:
            df['npi'] = pd.to_numeric(df['npi'], errors='coerce').fillna(0).astype('int64')

        # Boolean/Yes-No columns
        bool_cols = ['accepts_medicare', 'accepts_medicaid', 'sole_proprietor']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper().isin(['Y', 'YES', 'TRUE', '1'])

        # Date columns
        date_cols = ['enrollment_date', 'last_update_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Phone number - clean formatting
        if 'phone' in df.columns:
            df['phone'] = df['phone'].astype(str).str.strip()

        # Zip code - standardize format
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(str).str.strip()

        logger.info("Data type conversion complete")
        return df

    def _validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add validation fields and log quality issues."""
        logger.info("Validating data quality...")

        issues = {}

        # Validate NPI (should be non-zero)
        if 'npi' in df.columns:
            invalid_npi = (df['npi'] == 0) | (df['npi'].isna())
            issues['invalid_npi'] = invalid_npi.sum()
            df = df[~invalid_npi]

        # Validate state (should be 2 characters)
        if 'state' in df.columns:
            invalid_state = ~df['state'].str.match(r'^[A-Z]{2}$', na=False)
            issues['invalid_state'] = invalid_state.sum()
            df = df[~invalid_state]

        # Validate zip code (5 or 9 digits)
        if 'zip_code' in df.columns:
            invalid_zip = ~df['zip_code'].str.match(r'^\d{5}(-\d{4})?$', na=False)
            issues['invalid_zip'] = invalid_zip.sum()
            df = df[~invalid_zip]

        # Log quality metrics
        for issue, count in issues.items():
            if count > 0:
                logger.warning(f"Data quality issue - {issue}: {count} records removed")

        # Add validation flag
        df['is_valid_record'] = True

        logger.info(f"Data quality validation complete. Retained {len(df)} valid records")
        return df

    def _create_clinicians_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create the clinicians dimension table."""
        logger.info("Creating clinicians table...")
        # Use only columns that exist
        clinician_cols = [col for col in [
            'npi', 'first_name', 'last_name', 'middle_name', 'credentials',
            'medical_specialty', 'gender'
        ] if col in df.columns]

        clinicians = df[clinician_cols].drop_duplicates(subset=['npi'], keep='first')

        # Add record metadata
        clinicians['ingestion_timestamp'] = pd.Timestamp.now()
        clinicians['record_id'] = clinicians['npi'].astype(str) + '_' + clinicians.index.astype(str)
        clinicians['is_valid_record'] = True

        logger.info(f"Clinicians table: {len(clinicians)} unique providers")
        return clinicians.reset_index(drop=True)

    def _create_locations_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create the practice locations table."""
        logger.info("Creating practice locations table...")

        # Use only columns that exist
        location_cols = [col for col in [
            'npi', 'state', 'city', 'zip_code', 'street_address', 'street_address_2',
            'phone', 'organization_name', 'enrollment_status', 'enrollment_date',
            'accepts_medicare', 'accepts_medicaid', 'last_update_date'
        ] if col in df.columns]

        locations = df[location_cols].drop_duplicates(subset=['npi', 'state', 'city', 'zip_code'], keep='first')

        # Add record metadata
        locations['ingestion_timestamp'] = pd.Timestamp.now()
        locations['record_id'] = (
            locations['npi'].astype(str) + '_' +
            locations['state'].astype(str) + '_' +
            locations.index.astype(str)
        )
        locations['is_valid_record'] = True

        logger.info(f"Locations table: {len(locations)} unique locations")
        return locations.reset_index(drop=True)