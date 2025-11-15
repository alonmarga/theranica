"""
Load module for writing data to BigQuery.
Handles schema definition, table creation, and data loading.
"""

import logging
from typing import Dict, Any

import pandas as pd
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Loads transformed data into BigQuery."""

    def __init__(self, config):
        self.config = config
        self.client = bigquery.Client(project=config.PROJECT_ID)
        self.dataset_id = config.DATASET_ID

    def create_dataset(self):
        """Create BigQuery dataset if it doesn't exist."""
        logger.info(f"Creating or verifying dataset: {self.dataset_id}")

        dataset_id_full = f"{self.config.PROJECT_ID}.{self.dataset_id}"
        dataset = bigquery.Dataset(dataset_id_full)
        dataset.location = self.config.DATASET_LOCATION
        dataset.description = self.config.DATASET_DESCRIPTION

        try:
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Dataset created: {dataset_id_full}")
        except Conflict:
            logger.info(f"Dataset already exists: {dataset_id_full}")
        except Exception as e:
            logger.error(f"Error creating dataset: {str(e)}")
            raise

    def get_schema(self, table_name: str) -> list:
        """Get BigQuery schema for the specified table."""
        schemas = {
            'clinicians': [
                bigquery.SchemaField('record_id', 'STRING', mode='REQUIRED',
                                     description='Unique record identifier'),
                bigquery.SchemaField('npi', 'INTEGER', mode='REQUIRED',
                                     description='National Provider Identifier'),
                bigquery.SchemaField('first_name', 'STRING', mode='NULLABLE',
                                     description='Provider first name'),
                bigquery.SchemaField('last_name', 'STRING', mode='NULLABLE',
                                     description='Provider last name'),
                bigquery.SchemaField('middle_name', 'STRING', mode='NULLABLE',
                                     description='Provider middle name'),
                bigquery.SchemaField('credentials', 'STRING', mode='NULLABLE',
                                     description='Provider credentials (MD, DO, DDS, etc.)'),
                bigquery.SchemaField('medical_specialty', 'STRING', mode='NULLABLE',
                                     description='Primary medical specialty'),
                bigquery.SchemaField('gender', 'STRING', mode='NULLABLE',
                                     description='Provider gender (M/F)'),
                bigquery.SchemaField('is_valid_record', 'BOOLEAN', mode='NULLABLE',
                                     description='Data quality validation flag'),
                bigquery.SchemaField('ingestion_timestamp', 'TIMESTAMP', mode='REQUIRED',
                                     description='Timestamp when record was ingested'),
            ],
            'practice_locations': [
                bigquery.SchemaField('record_id', 'STRING', mode='REQUIRED',
                                     description='Unique record identifier'),
                bigquery.SchemaField('npi', 'INTEGER', mode='REQUIRED',
                                     description='National Provider Identifier'),
                bigquery.SchemaField('state', 'STRING', mode='REQUIRED',
                                     description='State code (2-letter abbreviation)'),
                bigquery.SchemaField('city', 'STRING', mode='NULLABLE',
                                     description='City'),
                bigquery.SchemaField('zip_code', 'STRING', mode='NULLABLE',
                                     description='Zip code (5 or 9-digit format)'),
                bigquery.SchemaField('street_address', 'STRING', mode='NULLABLE',
                                     description='Primary street address'),
                bigquery.SchemaField('street_address_2', 'STRING', mode='NULLABLE',
                                     description='Secondary street address (suite, apt, etc.)'),
                bigquery.SchemaField('phone', 'STRING', mode='NULLABLE',
                                     description='Contact phone number'),
                bigquery.SchemaField('organization_name', 'STRING', mode='NULLABLE',
                                     description='Organization/practice name'),
                bigquery.SchemaField('enrollment_status', 'STRING', mode='NULLABLE',
                                     description='Current enrollment status'),
                bigquery.SchemaField('enrollment_date', 'DATE', mode='NULLABLE',
                                     description='Date of enrollment with CMS'),
                bigquery.SchemaField('accepts_medicare', 'BOOLEAN', mode='NULLABLE',
                                     description='Whether provider accepts Medicare'),
                bigquery.SchemaField('accepts_medicaid', 'BOOLEAN', mode='NULLABLE',
                                     description='Whether provider accepts Medicaid'),
                bigquery.SchemaField('last_update_date', 'DATE', mode='NULLABLE',
                                     description='Date of last update from CMS'),
                bigquery.SchemaField('is_valid_record', 'BOOLEAN', mode='NULLABLE',
                                     description='Data quality validation flag'),
                bigquery.SchemaField('ingestion_timestamp', 'TIMESTAMP', mode='REQUIRED',
                                     description='Timestamp when record was ingested'),
            ]
        }

        return schemas.get(table_name, [])

    def load_data(self, df: pd.DataFrame, table_name: str,
                  description: str = '') -> str:
        """
        Load a DataFrame to BigQuery.

        Args:
            df: DataFrame to load
            table_name: Target table name
            description: Table description for metadata

        Returns:
            Full table ID (project.dataset.table)
        """
        logger.info(f"Loading data to table: {table_name}")

        table_id = f"{self.config.PROJECT_ID}.{self.dataset_id}.{table_name}"
        schema = self.get_schema(table_name)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingestion_timestamp"
            )
        )

        try:
            load_job = self.client.load_table_from_dataframe(
                df,
                table_id,
                job_config=job_config
            )
            load_job.result()

            destination_table = self.client.get_table(table_id)
            logger.info(f"Loaded {destination_table.num_rows} rows to {table_id}")
            logger.info(f"Table schema verified with {len(schema)} fields")

            # self.verify_data(table_name)
            return table_id

        except GoogleCloudError as e:
            logger.error(f"BigQuery error loading {table_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading {table_id}: {str(e)}")
            raise

    def verify_data(self, table_name: str) -> Dict[str, Any]:
        """Verify loaded data and return statistics."""
        logger.info(f"Verifying data in table: {table_name}")

        table_id = f"{self.config.PROJECT_ID}.{self.dataset_id}.{table_name}"

        try:
            # Get table info
            table = self.client.get_table(table_id)

            # Run sample query
            query = f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT npi) as unique_npis
                FROM `{table_id}`
            """

            query_job = self.client.query(query)
            results = query_job.result().to_dict_list()

            verification = {
                'table_id': table_id,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'query_results': results[0] if results else {}
            }

            logger.info(f"Verification complete: {verification}")
            return verification

        except Exception as e:
            logger.error(f"Error verifying table {table_id}: {str(e)}")
            raise
