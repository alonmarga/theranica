"""
Utility script to export sample data from BigQuery to CSV.
Useful for validation and including in repository.
"""

import logging
import pandas as pd
from google.cloud import bigquery
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def export_sample_data(table_name: str, sample_size: int = 100):
    """
    Export sample data from BigQuery to CSV.

    Args:
        table_name: Name of the table to export
        sample_size: Number of rows to export
    """
    config = Config()
    Config.validate()

    client = bigquery.Client(project=config.PROJECT_ID)
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.{table_name}"

    logger.info(f"Exporting {sample_size} rows from {table_id}...")

    query = f"""
        SELECT * 
        FROM `{table_id}`
        LIMIT {sample_size}
    """

    query_job = client.query(query)
    df = query_job.to_dataframe()

    output_file = f"data/sample_{table_name}.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Exported {len(df)} rows to {output_file}")

    return df


def export_all_samples():
    """Export sample data from all tables."""
    tables = ['clinicians', 'practice_locations']

    for table in tables:
        try:
            export_sample_data(table)
        except Exception as e:
            logger.error(f"Error exporting {table}: {str(e)}")


if __name__ == "__main__":
    export_all_samples()