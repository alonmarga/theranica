"""
CMS Doctors and Clinicians ETL Pipeline
Main orchestration script for extracting, transforming, and loading data.
"""

import logging
import sys
from datetime import datetime

from config import Config
from extract import CmsDataExtractor
from load import BigQueryLoader
from transform import DataTransformer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Orchestrates the ETL pipeline execution."""

    def __init__(self, config: Config):
        self.config = config
        self.extractor = CmsDataExtractor(config)
        self.transformer = DataTransformer(config)
        self.loader = BigQueryLoader(config)
        self.start_time = None
        self.end_time = None

    def run(self):
        """Execute the full ETL pipeline."""
        try:
            self.start_time = datetime.now()
            logger.info("*" * 80)
            logger.info("Starting CMS ETL Pipeline")
            logger.info(f"Configuration: States={self.config.STATES}, Specialties={self.config.SPECIALTIES}")
            logger.info("*" * 80)

            # Extract
            logger.info("PHASE 1: Extracting data from CMS API...")
            raw_data = self.extractor.fetch_all_data()
            logger.info(f"Extracted {len(raw_data)} records from API")

            if not raw_data:
                logger.warning("No data extracted. Pipeline terminated.")
                return False

            # Transform
            logger.info("PHASE 2: Transforming and cleaning data...")
            clinicians_df, locations_df = self.transformer.transform(raw_data)
            logger.info(f"Transformed into {len(clinicians_df)} clinician records "
                        f"and {len(locations_df)} location records")

            # Load
            logger.info("PHASE 3: Loading data to BigQuery...")
            self.loader.create_dataset()
            clinicians_table_id = self.loader.load_data(
                clinicians_df,
                table_name='clinicians',
                description='Clinician/provider records with NPI and credentials'
            )
            locations_table_id = self.loader.load_data(
                locations_df,
                table_name='practice_locations',
                description='Practice location and enrollment information'
            )

            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()

            logger.info("*" * 80)
            logger.info("ETL Pipeline Completed Successfully")
            logger.info(f"Clinicians table: {clinicians_table_id}")
            logger.info(f"Locations table: {locations_table_id}")
            logger.info(f"Total duration: {duration:.2f} seconds")

            return True

        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
            self.end_time = datetime.now()
            return False


if __name__ == "__main__":
    config = Config()
    pipeline = ETLPipeline(config)
    success = pipeline.run()
    sys.exit(0 if success else 1)
