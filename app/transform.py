"""
Transform module for manipulation and clean data from CMS API.
T step in ETL/ELT
"""

import logging
from typing import List, Dict, Any, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Class to transforms and cleans CMS clinician data"""

    def __init__(self, config):
        self.config = config

    def transform(self, raw_data: List[Dict[str, Any]], include_invalid_records: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main transformation pipeline.

        Args:
            raw_data: Raw records from CMS API
            include_invalid_records: If True, keep invalid records for testing

        Returns:
            Tuple of (clinicians_df, locations_df)
        """
        logger.info("Starting data transformation...")

        df = pd.DataFrame(raw_data)
        logger.info(f"Converted {len(df)} records to DataFrame")
        logger.info(f"Actual columns from API: {df.columns.tolist()}")

        df = self._standardize_columns(df)
        df = self._clean_data_types(df)
        df = self._validate_data_quality(df, include_invalid_records=include_invalid_records)

        clinicians_df = self._create_clinicians_table(df)
        locations_df = self._create_locations_table(df)

        logger.info(f"Transformation complete: {len(clinicians_df)} clinicians, {len(locations_df)} locations")

        return clinicians_df, locations_df

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Standardize column names from API format.
        logger.info("Standardizing column names...")

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
            'last_update_date': 'last_update_date',
            'filter_combination': 'filter_combination'  # Keep filter tracking column
        }

        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_columns)
        df = df[[v for v in existing_columns.values()]]

        logger.info(f"Standardized to {len(df.columns)} columns")
        return df

    def _clean_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate data types."""
        logger.info("Converting data types...")

        string_cols = ['first_name', 'last_name', 'middle_name', 'credentials',
                       'medical_specialty', 'gender', 'organization_name',
                       'street_address', 'street_address_2', 'city', 'state',
                       'filter_combination']

        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                if col in ['state', 'medical_specialty']:
                    df[col] = df[col].str.upper()

        if 'npi' in df.columns:
            df['npi'] = pd.to_numeric(df['npi'], errors='coerce').fillna(0).astype('int64')

        bool_cols = ['accepts_medicare', 'accepts_medicaid', 'sole_proprietor']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper().isin(['Y', 'YES', 'TRUE', '1'])

        date_cols = ['enrollment_date', 'last_update_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'phone' in df.columns:
            df['phone'] = df['phone'].astype(str).str.strip()

        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(str).str.strip()

        logger.info("Data type conversion complete")
        return df

    def _validate_data_quality(self, df: pd.DataFrame, include_invalid_records: bool = False) -> pd.DataFrame:
        """Add validation fields based on config VALIDATION_RULES."""
        logger.info("Validating data quality...")
        logger.info(f"Applying validation rules: {self.config.VALIDATION_RULES}")

        if include_invalid_records:
            logger.warning("TESTING MODE: Including invalid records for testing purposes")

        initial_count = len(df)
        df = df.copy()
        df['is_valid_record'] = True

        for field, rule_description in self.config.VALIDATION_RULES.items():
            if field not in df.columns:
                logger.debug(f"Field '{field}' not in dataframe, skipping validation")
                continue

            invalid_mask = pd.Series(False, index=df.index)

            if field == 'npi':
                invalid_mask = (df['npi'] == 0) | (df['npi'].isna()) | (df['npi'].astype(str).str.len() != 10)

            elif field == 'state':
                invalid_mask = ~df['state'].str.match(r'^[A-Z]{2}$', na=False)

            elif field == 'zip_code':
                invalid_mask = ~(
                    df['zip_code'].str.match(r'^\d{5}$', na=False) |
                    df['zip_code'].str.match(r'^\d{9}$', na=False) |  # 9-digit (CMS format)
                    df['zip_code'].str.match(r'^\d{5}-\d{4}$', na=False)
                )

            if invalid_mask.sum() > 0:
                df.loc[invalid_mask, 'is_valid_record'] = False
                logger.warning(f"Data quality issue - {field}: {invalid_mask.sum()} records marked invalid")

                if not include_invalid_records:
                    df = df[~invalid_mask].copy()
                    logger.debug(f"Removed {invalid_mask.sum()} invalid records")

        valid_count = (df['is_valid_record'] == True).sum()
        invalid_count = (df['is_valid_record'] == False).sum()

        logger.info(f"Data quality validation complete. Valid: {valid_count}, Invalid: {invalid_count}, Total: {len(df)} of {initial_count}")

        return df

    def _create_clinicians_table(self, df: pd.DataFrame) -> pd.DataFrame:
        # Create the clinicians dimension table
        logger.info("Creating clinicians table...")

        clinician_cols = [col for col in [
            'npi', 'first_name', 'last_name', 'middle_name', 'credentials',
            'medical_specialty', 'gender', 'is_valid_record', 'filter_combination'
        ] if col in df.columns]

        clinicians = df[clinician_cols].drop_duplicates(subset=['npi'], keep='first').copy()
        logger.info("Deduplicating clinicians on: npi")

        clinicians['ingestion_timestamp'] = pd.Timestamp.now()
        clinicians['load_id'] = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        clinicians['record_id'] = clinicians['npi'].astype(str) + '_' + clinicians.index.astype(str)

        valid = (clinicians['is_valid_record'] == True).sum()
        invalid = (clinicians['is_valid_record'] == False).sum()

        logger.info(f"Clinicians table: {len(clinicians)} providers (valid: {valid}, invalid: {invalid})")

        return clinicians.reset_index(drop=True)

    def _create_locations_table(self, df: pd.DataFrame) -> pd.DataFrame:
        # Create the practice locations table
        logger.info("Creating practice locations table...")

        location_cols = [col for col in [
            'npi', 'state', 'city', 'zip_code', 'street_address', 'street_address_2',
            'phone', 'organization_name', 'enrollment_status', 'enrollment_date',
            'accepts_medicare', 'accepts_medicaid', 'last_update_date', 'is_valid_record',
            'filter_combination'
        ] if col in df.columns]

        dedup_cols = [col for col in self.config.DEDUPLICATE_ON if col in df.columns]

        if not dedup_cols:
            logger.warning(f"No dedup columns found. Using default: npi, state, city, zip_code")
            dedup_cols = ['npi', 'state', 'city', 'zip_code']

        logger.info(f"Deduplicating locations on: {dedup_cols}")
        locations = df[location_cols].drop_duplicates(subset=dedup_cols, keep='first').copy()

        locations['ingestion_timestamp'] = pd.Timestamp.now()
        locations['load_id'] = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        locations['record_id'] = (
            locations['npi'].astype(str) + '_' +
            locations['state'].astype(str) + '_' +
            locations.index.astype(str)
        )

        valid = (locations['is_valid_record'] == True).sum()
        invalid = (locations['is_valid_record'] == False).sum()

        logger.info(f"Locations table: {len(locations)} locations (valid: {valid}, invalid: {invalid})")

        return locations.reset_index(drop=True)