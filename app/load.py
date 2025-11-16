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

    def __init__(self, config):
        self.config = config
        config.validate()
        self.client = bigquery.Client(project=config.PROJECT_ID)
        self.dataset_id = config.DATASET_ID

        self.schema_dir = os.environ.get('SCHEMA_DIR')

        # Validate schema directory exists
        if not os.path.exists(self.schema_dir):
            raise FileNotFoundError(
                f"Schema directory not found: {self.schema_dir}. "
                f"Please create app/gcp_schema/ folder with schema JSON files."
            )

        logger.info(f"BigQueryLoader initialized with schema directory: {self.schema_dir}")

    def get_available_tables(self) -> list:

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
            raise

    def load_schema_from_file(self, table_name: str) -> list:

        schema_file = os.path.join(self.schema_dir, f'{table_name}_schema.json')

        if not os.path.exists(schema_file):
            available = self.get_available_tables()
            raise FileNotFoundError(
                f"Schema file not found: {schema_file}\n"
                f"Available tables: {available}\n"
                f"To add a new table: Create {os.path.basename(schema_file)} in {self.schema_dir}/"
            )

        logger.info(f"Loading schema from: {schema_file}")

        try:
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

    def load_data(self, df: pd.DataFrame, table_name: str, description: str = '') -> str:

        logger.info(f"Loading data to table: {table_name}")

        table_id = f"{self.config.PROJECT_ID}.{self.dataset_id}.{table_name}"

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

        logger.info("Loading ETL process metrics to etl_processes table")
        metrics_df = pd.DataFrame([metrics])

        table_id = f"{self.config.PROJECT_ID}.{self.dataset_id}.etl_processes"


        self.load_schema_from_file('etl_processes')

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