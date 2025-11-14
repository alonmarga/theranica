"""
Extract module for retrieving data from CMS API.
Uses the new POST API with conditions.
"""

import logging
from typing import List, Dict, Any

import requests

from config import Config

logger = logging.getLogger(__name__)


class CmsDataExtractor:

    def __init__(self, config: Config):
        self.config = config
        # API endpoint
        self.base_url = config.base_url
        self.total_count = None

    def build_conditions(self) -> List[Dict[str, Any]]:
        """Build filter conditions for the API."""
        conditions = []

        # Add state filter
        # Only add first state as single filter for now
        if self.config.STATES:
            conditions.append({
                "resource": "t",
                "property": "state",
                "value": self.config.STATES[0],  # Start with just NY
                "operator": "="
            })

        # Add specialty filter - INTERNAL MEDICINE OR ANESTHESIOLOGY
        # Only add first specialty for now
        if self.config.SPECIALTIES:
            conditions.append({
                "resource": "t",
                "property": "pri_spec",
                "value": self.config.SPECIALTIES[0],  # Start with just INTERNAL MEDICINE
                "operator": "="
            })

        return conditions

    def fetch_batch(self, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Fetch a batch of records from the API.

        Args:
            offset: Record offset for pagination

        Returns:
            List of records from API
        """
        conditions = self.build_conditions()

        payload = {
            "conditions": conditions,
            "limit": self.config.BATCH_SIZE,
            "offset": offset
        }

        try:
            logger.debug(f"Fetching batch at offset {offset}")
            logger.debug(f"Payload: {payload}")

            response = requests.post(
                self.base_url,
                json=payload,
                timeout=60,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()

            result = response.json()
            logger.debug(f"API response keys: {result.keys() if isinstance(result, dict) else 'list'}")

            data = result["results"]

            # Extract total count from first batch response
            if self.total_count is None and "count" in result:
                self.total_count = result["count"]
                if self.config.MAX_RECORDS:
                    logger.info(f"API total count: {self.total_count} | Limit: {self.config.MAX_RECORDS}")
                else:
                    logger.info(f"API total count available: {self.total_count}")

            logger.info(f"Fetched {len(data)} records at offset {offset}")
            return data

        except requests.exceptions.HTTPError as e:
            logger.error(f"API HTTP error at offset {offset}: {e.response.status_code}")
            logger.error(f"Response: {e.response.text[:500]}")
            raise
        except Exception as e:
            logger.error(f"API request failed at offset {offset}: {str(e)}")
            raise

    def fetch_all_data(self) -> List[Dict[str, Any]]:
        """
        Fetch all data from the API using pagination.

        Returns:
            Complete list of all records matching filters
        """
        all_records = []
        offset = 0
        batch_count = 0

        while True:
            batch = self.fetch_batch(offset)

            if not batch:
                logger.info("No more records to fetch. Pagination complete.")
                break

            all_records.extend(batch)
            batch_count += 1

            # Progress logging based on MAX_RECORDS setting
            if self.config.MAX_RECORDS:
                # Show progress relative to MAX_RECORDS limit
                percentage = (len(all_records) / self.config.MAX_RECORDS) * 100
                remaining = max(0, self.config.MAX_RECORDS - len(all_records))
                progress_msg = f"Batch {batch_count}: {len(batch)} records | Progress: {len(all_records)}/{self.config.MAX_RECORDS} ({percentage:.1f}%) | Remaining: {remaining}"
            else:
                # Show progress relative to API total count
                if self.total_count:
                    remaining = max(0, self.total_count - len(all_records))
                    percentage = (len(all_records) / self.total_count) * 100
                    progress_msg = f"Batch {batch_count}: {len(batch)} records | Progress: {len(all_records)}/{self.total_count} ({percentage:.1f}%) | Remaining: {remaining}"
                else:
                    progress_msg = f"Batch {batch_count}: {len(batch)} records | Total so far: {len(all_records)}"

            logger.info(progress_msg)

            # Check if we've reached the limit
            if self.config.MAX_RECORDS and len(all_records) >= self.config.MAX_RECORDS:
                all_records = all_records[:self.config.MAX_RECORDS]
                logger.info(f"Reached maximum records limit: {self.config.MAX_RECORDS}")
                break

            # Stop if we got fewer records than the batch size (last page)
            if len(batch) < self.config.BATCH_SIZE:
                logger.info("Received fewer records than batch size. Final batch reached.")
                break

            offset += self.config.BATCH_SIZE

        # Final log message
        if self.config.MAX_RECORDS:
            final_msg = f"Data extraction complete. Total records fetched: {len(all_records)} (limit: {self.config.MAX_RECORDS})"
        else:
            final_msg = f"Data extraction complete. Total records fetched: {len(all_records)}"
            if self.total_count:
                final_msg += f" out of {self.total_count} available"

        logger.info(final_msg)

        return all_records
