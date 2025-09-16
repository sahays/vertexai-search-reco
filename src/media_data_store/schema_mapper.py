"""Schema mapping functionality for Media Data Store."""

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from dateutil.parser import parse as parse_datetime

from .config import ConfigManager
from .utils import setup_logging, MediaDataStoreError, save_output, load_json_file
from .google_media_validator import GoogleMediaValidator


class MediaSchemaMapper:
    """Handles schema mapping and transformation for media data."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = setup_logging()

    def _validate_rfc3339_datetime(self, value: str) -> str:
        """Validates and formats a string to RFC 3339 datetime."""
        if not value:
            return ""
        try:
            dt = parse_datetime(value)
            return dt.isoformat().replace('+00:00', 'Z')
        except (ValueError, TypeError):
            self.logger.warning(f"Could not parse datetime: {value}. Leaving as is.")
            return value

    def _convert_count_to_duration(self, episode_count: Any) -> str:
        """Converts an episode count to a Google duration string."""
        try:
            count = int(episode_count)
            total_minutes = count * 5  # Assuming 5 minutes per episode as per mapping file
            if total_minutes < 60:
                return f"{total_minutes}m"
            else:
                hours = total_minutes // 60
                minutes = total_minutes % 60
                if minutes > 0:
                    return f"{hours}h{minutes}m"
                return f"{hours}h"
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid episode_count: {episode_count}. Defaulting to empty duration.")
            return ""

    def _transform_record(self, record: Dict[str, Any], mapping_config: Dict[str, Any], include_original: bool) -> Dict[str, Any]:
        """Transforms a single data record based on the mapping configuration."""
        transformed_record = {}
        field_mapping = mapping_config.get("schema_mapping", {}).get("source_to_google", {})
        value_transformations = mapping_config.get("value_transformations", {})
        field_processing = mapping_config.get("field_processing", {})

        # 1. Apply basic field mapping
        for source_field, google_field in field_mapping.items():
            if source_field in record and record[source_field] is not None:
                # Skip person fields for now, they will be handled by consolidation
                if google_field == "persons":
                    continue
                transformed_record[google_field] = record[source_field]

        # 2. Handle field consolidations (e.g., persons)
        consolidation_rules = field_processing.get("consolidate_person_fields", {})
        if consolidation_rules.get("enabled"):
            target_field = consolidation_rules.get("target_field")
            persons = []
            for source_field in consolidation_rules.get("merge_fields", []):
                role = source_field.rstrip('s') # 'actors' -> 'actor'
                if source_field in record and isinstance(record[source_field], list):
                    for person_name in record[source_field]:
                        if isinstance(person_name, str) and person_name.strip():
                             persons.append({"name": person_name.strip(), "role": role})
            if persons:
                transformed_record[target_field] = persons


        # 3. Apply value transformations
        for source_field, transform_config in value_transformations.items():
            google_field = field_mapping.get(source_field)
            if google_field and google_field in transformed_record:
                value = transformed_record[google_field]
                transform_type = transform_config.get("type")

                if transform_type == "validate_rfc3339_datetime":
                    transformed_record[google_field] = self._validate_rfc3339_datetime(value)
                elif transform_type == "convert_count_to_duration_estimate":
                    transformed_record[google_field] = self._convert_count_to_duration(value)

        if include_original:
            transformed_record["original_payload"] = json.dumps(record)

        # 4. Add a unique ID if not present
        source_id_field = self.config_manager.schema.field_mappings.id_source_field
        if source_id_field in record and record[source_id_field] is not None:
            doc_id = str(record[source_id_field])
            transformed_record["id"] = doc_id
            transformed_record["_id"] = doc_id  # Add the required _id field
        else:
            import uuid
            doc_id = str(uuid.uuid4())
            transformed_record["id"] = doc_id
            transformed_record["_id"] = doc_id  # Add the required _id field
            self.logger.warning(f"Source data missing '{source_id_field}'. Generated new UUID: {doc_id}")

        return transformed_record

    def map_schema_fields(self, data: List[Dict[str, Any]], mapping_config: Dict[str, Any],
                         output_dir: Optional[Path] = None, subcommand: str = "transform",
                         include_original: bool = False) -> List[Dict[str, Any]]:
        """Apply schema field mappings to media data using the mapping file."""
        self.logger.info("Starting customer data transformation and schema mapping using mapping file.")

        if not isinstance(data, list):
            self.logger.error("Input data must be a list of records.")
            raise MediaDataStoreError("Input data for transformation must be a list of records.")

        self.logger.info(f"Transforming batch of {len(data)} customer records.")
        
        mapped_data = [self._transform_record(record, mapping_config, include_original) for record in data]

        mapping_log = {
            "transformation_type": "dynamic_mapping_file",
            "input_record_count": len(data),
            "output_record_count": len(mapped_data),
            "timestamp": datetime.now().isoformat(),
            "applied_mapping_file": self.config_manager.get_mapping_file_path().name
        }

        self.logger.info("Customer data transformation completed successfully.")

        if output_dir:
            output_file = save_output(mapped_data, output_dir, "transformed_customer_data.json", subcommand)
            mapping_log_file = save_output(mapping_log, output_dir, "transformation_log.json", subcommand)
            self.logger.info(f"Transformed data saved to: {output_file}")
            self.logger.info(f"Transformation log saved to: {mapping_log_file}")

        return mapped_data
