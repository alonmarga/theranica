"""
Export module to export csv sample file
"""

import logging
import os

from google.cloud import bigquery

from app.config import Config

logger = logging.getLogger(__name__)


def get_all_tables() -> list:
    config = Config()
    client = bigquery.Client(project=config.PROJECT_ID)

    try:
        tables = client.list_tables(config.DATASET_ID)
        table_names = [table.table_id for table in tables]
        logger.info(f"Found {len(table_names)} tables in dataset: {table_names}")
        return table_names
    except Exception as e:
        logger.error(f"Error fetching tables from dataset: {str(e)}")
        return []


def export_sample_data(table_name: str, sample_size: int = 100) -> None:
    """Export sample data from BigQuery to CSV."""
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

    data_dir = config.DATA_DIR_TO_EXPORT
    os.makedirs(data_dir, exist_ok=True)

    output_file = f"{data_dir}/sample_{table_name}.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Exported {len(df)} rows to {output_file}")


def export_all_samples() -> None:
    """Export sample data from all tables in the dataset."""
    config = Config()

    # Get all tables dynamically
    tables = get_all_tables()

    if not tables:
        logger.warning("No tables found in dataset")
        return

    logger.info(f"Starting export of sample data from {len(tables)} table(s)...")

    for table in tables:
        try:
            sample_size = getattr(config, 'SAMPLE_SIZE_TO_EXPORT', 100)
            export_sample_data(table_name=table, sample_size=sample_size)
        except Exception as e:
            logger.error(f"Error exporting {table}: {str(e)}")

    logger.info("Export of all samples completed")
