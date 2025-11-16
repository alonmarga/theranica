"""
Load module for writing data to BigQuery.
Handles schema definition loading from gcp_schema/ folder JSON files, table creation, and data loading.
Schemas are auto-discovered from gcp_schema/ folder - add new tables by adding new JSON files.
"""

import logging
import os
from typing import Dict, Any

import pandas as pd
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """
    Loads data to BigQuery with schemas managed in gcp_schema/ folder.

    Schema Discovery:
    - Automatically scans gcp_schema/ for JSON files
    - File naming: {table_name}_schema.json
    - To add new table: Create new JSON file in gcp_schema/
    - No code changes needed!
    """

    def __init__(self, config):
        self.config = config
        config.validate()
        self.client = bigquery.Client(project=config.PROJECT_ID)
        self.dataset_id = config.DATASET_ID

        # ========== SCHEMA DIRECTORY SETUP ==========
        # Points to app/gcp_schema/ folder where all schema JSON files are stored
        self.schema_dir = os.path.join(os.path.dirname(__file__), 'gcp_schemas')

        # Validate schema directory exists
        if not os.path.exists(self.schema_dir):
            raise FileNotFoundError(
                f"Schema directory not found: {self.schema_dir}. "
                f"Please create app/gcp_schema/ folder with schema JSON files."
            )

        logger.info(f"BigQueryLoader initialized with schema directory: {self.schema_dir}")

    def get_available_tables(self) -> list:
        """
        Get list of available tables (based on schema files).

        Returns:
            List of table names (extracted from {table_name}_schema.json files)
        """
        try:
            schema_files = os.listdir(self.schema_dir)
            tables = []

            for file in schema_files:
                if file.endswith('_schema.json'):
                    table_name = file.replace('_schema.json', '')
                    tables.append(table_name)

            return sorted(tables)

        except Exception as e:
            logger.error(f"Error getting available tables: {str(e)}")
            return []

    def load_schema_from_file(self, table_name: str) -> list:
        """
        ========== THIS IS WHERE JSON FILES ARE READ ==========

        Load BigQuery schema from a JSON file in gcp_schema/ folder.

        PROCESS:
        1. Constructs path to JSON file: app/gcp_schema/{table_name}_schema.json
        2. Checks if file exists
        3. Uses Google's schema_from_json() to READ the JSON file
        4. Converts JSON to BigQuery SchemaField objects

        Uses Google's recommended schema_from_json() method.

        Args:
            table_name: Name of the table (e.g., 'clinicians', 'practice_locations')
                       Will look for {table_name}_schema.json in gcp_schema/

        Returns:
            List of bigquery.SchemaField objects (schema for the table)

        Raises:
            FileNotFoundError: If schema JSON file not found

        Example:
            schema = loader.load_schema_from_file('clinicians')
            # Reads from: app/gcp_schema/clinicians_schema.json
            # Returns: [SchemaField('record_id', 'STRING', ...), SchemaField('npi', 'INTEGER', ...), ...]
        """

        # ========== BUILD PATH TO JSON FILE ==========
        # Example: /app/gcp_schema/clinicians_schema.json
        schema_file = os.path.join(self.schema_dir, f'{table_name}_schema.json')

        # ========== CHECK IF FILE EXISTS ==========
        if not os.path.exists(schema_file):
            available = self.get_available_tables()
            raise FileNotFoundError(
                f"Schema file not found: {schema_file}\n"
                f"Available tables: {available}\n"
                f"To add a new table: Create {os.path.basename(schema_file)} in {self.schema_dir}/"
            )

        logger.info(f"Loading schema from: {schema_file}")

        try:
            # ========== READ JSON FILE AND CONVERT TO SCHEMA ==========
            # THIS IS THE KEY LINE - READS THE JSON FILE
            # self.client.schema_from_json() is Google BigQuery's method to read JSON schema files
            #
            # The JSON file format is:
            # [
            #   {"name": "field1", "type": "STRING", "mode": "REQUIRED", "description": "..."},
            #   {"name": "field2", "type": "INTEGER", "mode": "NULLABLE", "description": "..."},
            #   ...
            # ]
            #
            # schema_from_json converts this JSON into BigQuery SchemaField objects
            schema = self.client.schema_from_json(schema_file)

            logger.info(f"Loaded schema for '{table_name}': {len(schema)} fields")
            return schema

        except Exception as e:
            logger.error(f"Error loading schema from {schema_file}: {str(e)}")
            raise

    def create_dataset(self):
        # Create BigQuery dataset if it doesn't exist
        logger.info(f"Creating or verifying dataset: {self.dataset_id}")

        dataset_id_full = f"{self.config.PROJECT_ID}.{self.dataset_id}"
        dataset = bigquery.Dataset(dataset_id_full)
        dataset.location = self.config.DATASET_LOCATION
        dataset.description = self.config.DATASET_DESCRIPTION

        try:
            self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Dataset created: {dataset_id_full}")
        except Conflict:
            logger.info(f"Dataset already exists: {dataset_id_full}")
        except Exception as e:
            logger.error(f"Error creating dataset: {str(e)}")
            raise

    def load_data(self, df: pd.DataFrame, table_name: str,
                  description: str = '') -> str:
        """
        Load a DataFrame to BigQuery.

        Schema is automatically loaded from gcp_schema/{table_name}_schema.json

        Args:
            df: DataFrame to load
            table_name: Target table name (also name of schema file without _schema.json)
            description: Table description for metadata

        Returns:
            Full table ID (project.dataset.table)
        """
        logger.info(f"Loading data to table: {table_name}")

        table_id = f"{self.config.PROJECT_ID}.{self.dataset_id}.{table_name}"

        # ========== THIS CALLS THE METHOD ABOVE TO READ JSON FILES ==========
        # Dynamically load schema for this table from gcp_schema/ folder
        # This internally calls load_schema_from_file() which reads the JSON
        schema = self.load_schema_from_file(table_name)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
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
            logger.info(f"Loaded {len(df)} rows to {table_id}")
            logger.info(f"Table now contains {destination_table.num_rows} total rows")
            logger.info(f"Table schema verified with {len(schema)} fields")

            # Verify data and log results
            try:
                verification = self.verify_data(table_name)
                logger.info(f"Data verification successful: {verification['query_results']}")
            except Exception as verify_error:
                logger.warning(f"Data verification warning (non-critical): {str(verify_error)}")

            return table_id

        except GoogleCloudError as e:
            logger.error(f"BigQuery error loading {table_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading {table_id}: {str(e)}")
            raise

    def load_metrics(self, metrics: Dict[str, Any]) -> str:
        """
        ========== LOG ETL PROCESS METRICS TO BIGQUERY ==========

        Load ETL process metrics and statistics to etl_processes table.
        This creates an audit trail of every ETL run with key metrics.

        Args:
            metrics: Dictionary with ETL metrics:
                - run_timestamp: When the run started
                - process_name: Name of the process
                - filters_applied: Filters used
                - records_extracted: Total records extracted
                - clinicians_records: Records in clinicians table
                - locations_records: Records in locations table
                - valid_records: Records that passed validation
                - invalid_records: Records that failed validation
                - status: success/failed/partial
                - duration_seconds: Execution time
                - error_message: Error if failed
                - load_id: Batch identifier
                - include_invalid: Whether invalid records included

        Returns:
            Full table ID (project.dataset.table)
        """
        logger.info("Loading ETL process metrics to etl_processes table")

        # Create a DataFrame with the metrics (one row)
        metrics_df = pd.DataFrame([metrics])

        table_id = f"{self.config.PROJECT_ID}.{self.dataset_id}.etl_processes"

        # Load schema for etl_processes table
        schema = self.load_schema_from_file('etl_processes')

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        try:
            load_job = self.client.load_table_from_dataframe(
                metrics_df,
                table_id,
                job_config=job_config
            )
            load_job.result()

            logger.info(f"Loaded metrics to {table_id}")
            logger.info(f"Process: {metrics.get('process_name')}, Status: {metrics.get('status')}, Duration: {metrics.get('duration_seconds'):.2f}s")

            return table_id

        except Exception as e:
            logger.error(f"Error loading metrics to {table_id}: {str(e)}")
            raise

    def verify_data(self, table_name: str) -> Dict[str, Any]:
        # Verify loaded data and return statistics
        logger.info(f"Verifying data in table: {table_name}")

        table_id = f"{self.config.PROJECT_ID}.{self.dataset_id}.{table_name}"

        try:
            # Get table info
            table = self.client.get_table(table_id)

            # Run sample query
            query = f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT npi) as unique_npis,
                    COUNT(DISTINCT load_id) as distinct_loads,
                    COUNT(DISTINCT filter_combination) as distinct_filters
                FROM `{table_id}`
            """

            query_job = self.client.query(query)
            results = [dict(row) for row in query_job.result()]

            verification = {
                'table_id': table_id,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'query_results': results[0] if results else {}
            }

            logger.info(f"Verification complete for {table_name}")
            logger.info(f"  --Total records: {verification['num_rows']}")
            logger.info(f"  --Query results: {verification['query_results']}")

            return verification

        except Exception as e:
            logger.error(f"Error verifying table {table_id}: {str(e)}")
            raise