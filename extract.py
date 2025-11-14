"""
Extract module for retrieving data from CMS API.
Uses the new POST API with conditions.
"""

import logging
import requests
from typing import List, Dict, Any
from config import Config

logger = logging.getLogger(__name__)


class CmsDataExtractor:
    """Extracts data from CMS Doctors and Clinicians API using new POST format."""

    def __init__(self, config: Config):
        self.config = config
        # New API endpoint
        self.base_url = "https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0"

    def build_conditions(self) -> List[Dict[str, Any]]:
        """Build filter conditions for the API."""
        conditions = []

        # Add state filter - NY OR FL
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

            # Handle nested response format - API returns {results: [...], count: ..., schema: ..., query: ...}
            if isinstance(result, dict) and "results" in result:
                data = result["results"]
            elif isinstance(result, list):
                data = result
            else:
                data = []

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
            logger.info(f"Batch {batch_count}: Retrieved {len(batch)} records (Total: {len(all_records)})")

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

        logger.info(f"Data extraction complete. Total records: {len(all_records)}")
        return all_records