"""
Extract module for retrieving data from CMS API.
Uses the POST API with conditions to fetch data for multiple filter combinations.
"""

import logging
from typing import List, Dict, Any
from itertools import product

import requests

from app.config import Config

logger = logging.getLogger(__name__)


class CmsDataExtractor:

    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.base_url
        self.total_records_fetched = 0

    def build_filter_combinations(self) -> List[Dict[str, Any]]:
        # Generate combinations of filter values.
        # Skips empty/None values and removes empty filters.
        #  Return: List of filter dictionaries, one for each combination

        clean_filters = {}
        for key, values in self.config.FILTERS.items():
            # Filter out empty strings and None values
            non_empty_values = [v for v in values if v and str(v).strip()]
            if non_empty_values:
                clean_filters[key] = non_empty_values
                logger.debug(f"Filter '{key}': {non_empty_values}")
            else:
                logger.debug(f"Filter '{key}': skipped (empty values)")

        if not clean_filters:
            logger.warning("No valid filters found after removing empty values. Will fetch all data.")
            return [{}]  # Return empty dict if no filters

        # Generate all combinations
        filter_keys = list(clean_filters.keys())
        filter_values_list = [clean_filters[key] for key in filter_keys]

        combinations = []
        for values_combo in product(*filter_values_list):
            combo_dict = {key: value for key, value in zip(filter_keys, values_combo)}
            combinations.append(combo_dict)

        return combinations

    def build_conditions(self, filter_dict: Dict[str, str]) -> List[Dict[str, Any]]:
        # Build API conditions from a filter dictionary (combinations)
        conditions = []
        for property_name, value in filter_dict.items():
            conditions.append({
                "resource": "t",
                "property": property_name,
                "value": value,
                "operator": "="
            })
        return conditions

    def fetch_all_data(self) -> List[Dict[str, Any]]:
        # Fetch all data for all filter combinations.

        all_records = []
        filter_combinations = self.build_filter_combinations()
        total_combinations = len(filter_combinations)

        logger.info(f"Starting extraction for {total_combinations} filter combinations")
        logger.info(f"Filters config: {self.config.FILTERS}")

        for current_combination, filter_combo in enumerate(filter_combinations, 1):
            # Create readable filter name
            if filter_combo:
                filter_name = " + ".join([f"{k}={v}" for k, v in sorted(filter_combo.items())])
            else:
                filter_name = "All data (no filters)"

            logger.info(f"\n[{current_combination}/{total_combinations}] Fetching: {filter_name}")

            try:
                combination_records = self._fetch_for_combination(filter_combo, filter_name)
                all_records.extend(combination_records)
            except Exception as e:
                logger.error(f"Failed to fetch {filter_name}: {str(e)}")
                continue

        self.total_records_fetched = len(all_records)
        logger.info(f"\n{'*'*60}")
        logger.info(f"Extraction completed. Total records fetched: {self.total_records_fetched}")
        logger.info(f"{'*'*60}\n")

        return all_records

    def _fetch_for_combination(self, filter_dict: Dict[str, str], filter_name: str) -> List[Dict[str, Any]]:
        # Fetch all data for a specific filter combination.
        records = []
        offset = 0
        batch_number = 0
        total_count = None

        # Calculate effective batch size  (MAX_RECORDS limit)
        effective_batch_size = self.config.BATCH_SIZE
        if self.config.MAX_RECORDS and self.config.MAX_RECORDS < self.config.BATCH_SIZE:
            effective_batch_size = self.config.MAX_RECORDS
            logger.info(f"{filter_name}: Adjusted batch size from {self.config.BATCH_SIZE} to {effective_batch_size} (MAX_RECORDS limit)")

        while True:
            batch_number += 1

            conditions = self.build_conditions(filter_dict)

            payload = {
                "conditions": conditions,
                "limit": effective_batch_size,
                "offset": offset
            }

            try:
                response = requests.post(
                    self.base_url,
                    json=payload,
                    timeout=60,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()

                result = response.json()
                batch = result.get("results", [])

                # Add filter_combination to each record for data lineage
                for record in batch:
                    record['filter_combination'] = filter_name

                # Extract total count from first batch response
                if total_count is None and "count" in result:
                    total_count = result["count"]
                    if self.config.MAX_RECORDS:
                        records_to_fetch = min(self.config.MAX_RECORDS, total_count)
                        logger.info(f"{filter_name}: Getting {records_to_fetch} rows out of {total_count} available")
                    else:
                        logger.info(f"{filter_name}: API total available: {total_count}")

                if not batch:
                    logger.info(f"{filter_name}: No more records.")
                    break

                records.extend(batch)

                # Progress logging with total count
                if total_count:
                    percentage = (len(records) / total_count) * 100
                    remaining = max(0, total_count - len(records))
                    logger.info(f"{filter_name}: Batch {batch_number} fetched {len(batch)} records | Progress: {len(records)}/{total_count} ({percentage:.1f}%) | Remaining: {remaining}")
                else:
                    logger.info(f"{filter_name}: Batch {batch_number} fetched {len(batch)} records (total so far: {len(records)})")

                # Check if we've reached the limit
                if self.config.MAX_RECORDS and len(records) >= self.config.MAX_RECORDS:
                    records = records[:self.config.MAX_RECORDS]
                    if total_count and self.config.MAX_RECORDS < total_count:
                        logger.info(f"{filter_name}: Reached maximum records limit: {self.config.MAX_RECORDS} (total available: {total_count})")
                    else:
                        logger.info(f"{filter_name}: Reached maximum records limit: {self.config.MAX_RECORDS}")
                    break

                # Stop if we got fewer records than the batch size (last page)
                if len(batch) < effective_batch_size:
                    logger.info(f"{filter_name}: Received fewer records than batch size. Final batch reached.")
                    break

                offset += effective_batch_size

            except requests.exceptions.HTTPError as e:
                logger.error(f"{filter_name}: HTTP error at offset {offset}: {e.response.status_code}")
                logger.error(f"Response: {e.response.text[:500]}")
                raise
            except Exception as e:
                logger.error(f"{filter_name}: Request failed at offset {offset}: {str(e)}")
                raise

        if total_count:
            if len(records) < total_count:
                logger.info(f"{filter_name}: Extraction completed. Fetched {len(records)} out of {total_count} available records ({(len(records)/total_count)*100:.1f}%)")
            else:
                logger.info(f"{filter_name}: Extraction completed. Fetched all {len(records)} records")
        else:
            logger.info(f"{filter_name}: Extraction complete. Total records: {len(records)}")

        return records