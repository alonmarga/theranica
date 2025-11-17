"""
CMS Doctors and Clinicians ETL Pipeline
Main orchestration script for extracting, transforming, and loading data.
"""

import argparse
import logging
import os
import sys
from datetime import datetime

from app.config import Config
from app.export_data import export_all_samples
from app.extract import CmsDataExtractor
from app.load import BigQueryLoader
from app.transform import DataTransformer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Setup GCP credentials before Config class
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Config.setup_gcp_credentials()


class ETLPipeline:
    # Orchestrates the ETL pipeline execution

    def __init__(self, config: Config):
        self.config = config
        self.extractor = CmsDataExtractor(config)
        self.transformer = DataTransformer(config)
        self.loader = BigQueryLoader(config)
        self.start_time = None
        self.end_time = None
        self.load_id = None

    def run(self, with_export=False, export_only=False, include_invalid=False,
            upload_raw=False):

        # with_export: Run ETL and then export to CSV
        # export_only: Only export from BigQuery (no ETL)
        # include_invalid: Include invalid records in load
        # upload_raw: Upload raw API data to Cloud Storage

        try:
            self.start_time = datetime.now()
            self.load_id = self.start_time.strftime('%Y%m%d_%H%M%S')

            # Metric for logging in etl_processes table
            metrics = {
                'run_timestamp': self.start_time,
                'process_name': 'CMS_PROVIDERS_ETL',
                'filters_applied': str(self.config.FILTERS),
                'records_extracted': 0,
                'clinicians_records': 0,
                'locations_records': 0,
                'valid_records': 0,
                'invalid_records': 0,
                'status': 'failed',
                'error_message': None,
                'include_invalid': include_invalid,
                'upload_raw': upload_raw,
            }

            # Export only mode
            if export_only:
                logger.info("*" * 100)
                logger.info("Starting Export Only")
                logger.info("*" * 100)
                self._export_data()
                self.end_time = datetime.now()
                duration = (self.end_time - self.start_time).total_seconds()
                logger.info("*" * 100)
                logger.info("Export Completed Successfully")
                logger.info(f"Total duration: {duration:.2f} seconds")
                return True

            # Normal ETL mode, no other flags
            logger.info("*" * 100)
            logger.info("Starting CMS ETL Pipeline")
            logger.info(f"Load ID: {self.load_id}")
            logger.info(f"Configuration: Filters={self.config.FILTERS}")
            logger.info(f"Export after ETL: {with_export}")
            logger.info(f"Upload raw to GCS: {upload_raw}")
            logger.info("*" * 100)

            # STEP 1: Extract
            logger.info("STEP 1: Extracting data from CMS API...")
            raw_data = self.extractor.fetch_all_data()
            logger.info(f"Extracted {len(raw_data)} records from API")

            metrics['records_extracted'] = len(raw_data)

            # No data found (filters for example)
            if not raw_data:
                logger.warning("No data extracted. Pipeline terminated.")
                metrics['status'] = 'failed'
                metrics['error_message'] = 'No data extracted from API'
                return False

            # STEP 1.1: Upload raw data to GCS
            if upload_raw:
                logger.info("STEP 1.5: Uploading raw data to Cloud Storage...")
                try:
                    gcs_path = self.loader.upload_raw_data_to_gcs(raw_data, self.load_id)
                    logger.info(f"Raw data uploaded to: {gcs_path}")
                    metrics['raw_data_gcs_path'] = gcs_path
                except Exception as e:
                    logger.error(f"Failed to upload raw data to GCS: {str(e)}")
                    raise

            # STEP 2: Transform
            logger.info("STEP 2: Transforming and cleaning data...")
            clinicians_df, locations_df = self.transformer.transform(
                raw_data,
                include_invalid_records=include_invalid
            )

            # transformation metrics
            metrics['clinicians_records'] = len(clinicians_df)
            metrics['locations_records'] = len(locations_df)
            metrics['valid_records'] = len(clinicians_df[clinicians_df['is_valid_record'] == True])
            metrics['invalid_records'] = len(clinicians_df[clinicians_df['is_valid_record'] == False])

            # STEP 3: Load
            logger.info("STEP 3: Loading data to BigQuery...")
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

            logger.info(f"Clinicians table: {clinicians_table_id}")
            logger.info(f"Locations table: {locations_table_id}")

            # STEP 4: Export sample (optional)
            if with_export:
                logger.info("STEP 4: Exporting data to CSV...")
                self._export_data()

            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()

            metrics['status'] = 'success'
            metrics['load_id'] = self.load_id
            metrics['duration_seconds'] = duration

            logger.info("*" * 100)
            logger.info("Pipeline Completed Successfully")
            logger.info(f"Total duration: {duration:.2f} seconds")
            logger.info("*" * 100)

            try:
                self.loader.load_metrics(metrics)
                logger.info("ETL process metrics logged to etl_processes table")
            except Exception as e:
                logger.warning(f"Could not log metrics to BigQuery: {str(e)}")

            return True

        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()

            # error metrics
            metrics['status'] = 'failed'
            metrics['error_message'] = str(e)
            metrics['duration_seconds'] = duration
            metrics['load_id'] = self.load_id

            # Try to log the failure
            try:
                self.loader.load_metrics(metrics)
                logger.info("ETL failure metrics logged to etl_processes table")
            except Exception as log_error:
                logger.warning(f"Could not log failure metrics: {str(log_error)}")

            return False

    def _export_data(self):
        # Export sample data from BigQuery to CSV
        try:
            export_all_samples()
            logger.info("Export completed successfully")
        except Exception as e:
            logger.error(f"Export failed: {str(e)}")
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='CMS ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
  docker compose run cms-etl-pipeline
    - Normal: extract, transform, load

  docker compose run cms-etl-pipeline python -m app.main --upload-raw
    - Extract, upload raw to GCS, transform, load

  docker compose run cms-etl-pipeline python -m app.main --with-export
    - Full: extract, transform, load, export sample

  docker compose run cms-etl-pipeline python -m app.main --upload-raw --with-export
    - Extract, upload raw, transform, load raw data, export

  docker compose run cms-etl-pipeline python -m app.main --export-only
    - Export only (no ETL)

  docker compose run cms-etl-pipeline python -m app.main --include-invalid
    - Include invalid records (is_valid_record=False)

  docker compose run cms-etl-pipeline python -m app.main --upload-raw --include-invalid
    - Upload raw + include invalid records

  docker compose run cms-etl-pipeline python -m app.main --upload-raw --with-export --include-invalid
    - Full pipeline with raw upload, export, and invalid records
        """
    )
    parser.add_argument(
        '--upload-raw',
        action='store_true',
        help='Upload raw API data to Cloud Storage'
    )
    parser.add_argument(
        '--with-export',
        action='store_true',
        help='Run ETL and export to CSV'
    )
    parser.add_argument(
        '--export-only',
        action='store_true',
        help='Export only (no ETL)'
    )
    parser.add_argument(
        '--include-invalid',
        action='store_true',
        help='Include invalid records for testing (is_valid_record=False)'
    )

    args = parser.parse_args()

    config = Config()
    pipeline = ETLPipeline(config)

    if args.export_only:
        success = pipeline.run(with_export=False, export_only=True, upload_raw=False)
    elif args.with_export:
        success = pipeline.run(
            with_export=True,
            export_only=False,
            include_invalid=args.include_invalid,
            upload_raw=args.upload_raw
        )
    else:
        success = pipeline.run(
            with_export=False,
            export_only=False,
            include_invalid=args.include_invalid,
            upload_raw=args.upload_raw
        )

    sys.exit(0 if success else 1)
