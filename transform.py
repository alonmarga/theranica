import logging
from typing import List, Dict, Any, Tuple

import pandas as pd

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
        """Add validation fields based on config VALIDATION_RULES."""
        logger.info("Validating data quality...")
        logger.info(f"Applying validation rules: {self.config.VALIDATION_RULES}")

        issues = {}
        initial_count = len(df)

        # Apply validation rules from config
        for field, rule_description in self.config.VALIDATION_RULES.items():
            if field not in df.columns:
                logger.debug(f"Field '{field}' not in dataframe, skipping validation")
                continue

            invalid_mask = pd.Series(False, index=df.index)

            if field == 'npi':
                # NPI must be numeric and 10 digits
                invalid_mask = (df['npi'] == 0) | (df['npi'].isna()) | (df['npi'].astype(str).str.len() != 10)
                issues['invalid_npi'] = invalid_mask.sum()

            elif field == 'state':
                # State must be 2-letter code
                invalid_mask = ~df['state'].str.match(r'^[A-Z]{2}$', na=False)
                issues['invalid_state'] = invalid_mask.sum()

            elif field == 'zip_code':
                # ZIP code validation:
                # Accept: 5 digits (12345), 9 digits (123456789), XXXXX-XXXX (12345-6789)
                # Reject: empty strings, null values
                invalid_mask = ~(
                        df['zip_code'].str.match(r'^\d{5}$', na=False) |  # 5-digit
                        df['zip_code'].str.match(r'^\d{9}$', na=False) |  # 9-digit (CMS format)
                        df['zip_code'].str.match(r'^\d{5}-\d{4}$', na=False)  # XXXXX-XXXX format
                )
                issues['invalid_zip'] = invalid_mask.sum()

            # Remove invalid records
            if invalid_mask.sum() > 0:
                logger.warning(
                    f"Data quality issue - {field} ({rule_description}): "
                    f"{invalid_mask.sum()} records removed"
                )
                df = df[~invalid_mask].copy()

        # Log quality metrics
        removed_count = initial_count - len(df)
        if removed_count > 0:
            logger.warning(f"Total records removed during validation: {removed_count}")

        # Ensure df is a copy to avoid SettingWithCopyWarning
        df = df.copy()

        # Add validation flag (True for all remaining records after filtering)
        df['is_valid_record'] = True

        logger.info(
            f"Data quality validation complete. "
            f"Retained {len(df)} valid records out of {initial_count} "
            f"({(len(df) / initial_count * 100):.1f}%)"
        )
        return df

    def _create_clinicians_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create the clinicians dimension table."""
        logger.info("Creating clinicians table...")

        # Use only columns that exist
        clinician_cols = [col for col in [
            'npi', 'first_name', 'last_name', 'middle_name', 'credentials',
            'medical_specialty', 'gender'
        ] if col in df.columns]

        # Deduplicate on NPI only for clinicians table
        clinicians = df[clinician_cols].drop_duplicates(subset=['npi'], keep='first').copy()
        logger.info(f"Deduplicating clinicians on: ['npi']")

        # Add record metadata
        clinicians['ingestion_timestamp'] = pd.Timestamp.now()
        clinicians['record_id'] = clinicians['npi'].astype(str) + '_' + clinicians.index.astype(str)
        clinicians['is_valid_record'] = True

        logger.info(f"Clinicians table: {len(clinicians)} unique providers")
        return clinicians.reset_index(drop=True)

    def _create_locations_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create the practice locations table using DEDUPLICATE_ON config."""
        logger.info("Creating practice locations table...")

        # Use only columns that exist
        location_cols = [col for col in [
            'npi', 'state', 'city', 'zip_code', 'street_address', 'street_address_2',
            'phone', 'organization_name', 'enrollment_status', 'enrollment_date',
            'accepts_medicare', 'accepts_medicaid', 'last_update_date'
        ] if col in df.columns]

        # Get deduplication columns from config and filter to available columns
        dedup_cols = [col for col in self.config.DEDUPLICATE_ON if col in df.columns]

        if not dedup_cols:
            logger.warning(
                f"No deduplication columns from config {self.config.DEDUPLICATE_ON} "
                f"found in dataframe. Using default: ['npi', 'state', 'city', 'zip_code']"
            )
            dedup_cols = ['npi', 'state', 'city', 'zip_code']

        logger.info(f"Deduplicating locations on: {dedup_cols}")
        locations = df[location_cols].drop_duplicates(subset=dedup_cols, keep='first').copy()

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