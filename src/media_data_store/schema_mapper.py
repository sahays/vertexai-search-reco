"""Schema mapping functionality for Media Data Store."""

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from .config import ConfigManager, MediaSchemaConfig
from .utils import setup_logging, MediaDataStoreError, save_output, load_json_file
from .google_media_validator import GoogleMediaValidator
from .custom_data_transformer import CustomDataTransformer


class MediaSchemaMapper:
    """Handles schema mapping and transformation for media data."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = setup_logging()
    
    def validate_data(self, data: Dict[str, Any], output_dir: Optional[Path] = None, subcommand: str = "validate") -> Dict[str, Any]:
        """Validate media data against Google's official media schema requirements."""
        self.logger.info("Starting media data validation")
        self.logger.debug(f"Validating data with {len(data)} fields")
        
        # For custom schema, transform customer data first, then validate
        schema_config = self.config_manager.schema
        
        if isinstance(data, list):
            self.logger.info(f"Validating batch of {len(data)} customer records")
            # Transform first record to get structure for validation
            if data:
                try:
                    sample_transformed = CustomDataTransformer.transform_customer_record(data[0])
                    validation_data = sample_transformed
                except Exception as e:
                    self.logger.error(f"Failed to transform sample record for validation: {e}")
                    validation_data = data[0] if data else {}
            else:
                validation_data = {}
        else:
            self.logger.info("Validating single customer record")
            try:
                validation_data = CustomDataTransformer.transform_customer_record(data)
            except Exception as e:
                self.logger.warning(f"Failed to transform data for validation, using raw data: {e}")
                validation_data = data
        
        # Use Google's official validator on transformed data
        required_validation = GoogleMediaValidator.validate_required_fields(validation_data)
        optional_validation = GoogleMediaValidator.validate_optional_fields(validation_data)
        
        validation_results = {
            "valid": required_validation["valid"],
            "errors": required_validation["errors"],
            "warnings": required_validation["warnings"] + optional_validation["warnings"],
            "google_compliance": {
                "required_fields": required_validation["required_fields_status"],
                "optional_fields": optional_validation["optional_fields_found"]
            },
            "customer_data_analysis": {
                "source_fields_found": list(data.keys()) if isinstance(data, dict) else f"{len(data)} records",
                "transformation_applied": True
            },
            "stats": {}
        }
        
        self.logger.debug(f"Google validation - Valid: {required_validation['valid']}")
        self.logger.debug(f"Required fields status: {required_validation['required_fields_status']}")
        self.logger.debug(f"Optional fields found: {optional_validation['optional_fields_found']}")
        
        if validation_results["errors"]:
            self.logger.warning(f"Validation errors: {validation_results['errors']}")
        if validation_results["warnings"]:
            self.logger.info(f"Validation warnings: {validation_results['warnings']}")
        
        # Generate statistics using customer source data
        source_data = data[0] if isinstance(data, list) and data else data
        validation_results["stats"] = {
            "total_source_fields": len(source_data) if isinstance(source_data, dict) else 0,
            "has_title_source": schema_config.field_mappings.title_source_field in source_data if isinstance(source_data, dict) else False,
            "has_uri_source": schema_config.field_mappings.uri_source_field in source_data if isinstance(source_data, dict) else False,
            "searchable_fields_present": len([f for f in schema_config.searchable_fields if f in source_data]) if isinstance(source_data, dict) else 0,
            "validation_timestamp": datetime.now().isoformat()
        }
        
        self.logger.info(f"Validation completed. Valid: {validation_results['valid']}")
        
        if output_dir:
            output_file = save_output(validation_results, output_dir, "validation_results.json", subcommand)
            self.logger.info(f"Validation results saved to: {output_file}")
        
        return validation_results
    
    def map_schema_fields(self, data: Dict[str, Any], mapping_config: Dict[str, Any], 
                         output_dir: Optional[Path] = None, subcommand: str = "transform") -> Dict[str, Any]:
        """Apply schema field mappings to media data using customer transformer."""
        self.logger.info("Starting customer data transformation and schema mapping")
        
        # Handle both single record and array of records
        if isinstance(data, list):
            self.logger.info(f"Transforming batch of {len(data)} customer records")
            mapped_data = CustomDataTransformer.transform_batch(data)
        else:
            self.logger.info("Transforming single customer record")
            try:
                mapped_data = CustomDataTransformer.transform_customer_record(data)
            except Exception as e:
                self.logger.error(f"Failed to transform customer record: {e}")
                raise MediaDataStoreError(f"Customer data transformation failed: {e}")
        
        mapping_log = {
            "transformation_type": "customer_data_transformer",
            "input_record_count": len(data) if isinstance(data, list) else 1,
            "output_record_count": len(mapped_data) if isinstance(mapped_data, list) else 1,
            "timestamp": datetime.now().isoformat(),
            "applied_transformations": [
                "title_validation",
                "image_to_uri_conversion", 
                "genre_to_categories_mapping",
                "release_date_to_rfc3339",
                "episode_count_to_duration",
                "language_code_normalization",
                "persons_consolidation",
                "extended_metadata_extraction"
            ]
        }
        
        self.logger.info(f"Customer data transformation completed successfully")
        
        if output_dir:
            output_file = save_output(mapped_data, output_dir, "transformed_customer_data.json", subcommand)
            mapping_log_file = save_output(mapping_log, output_dir, "transformation_log.json", subcommand)
            self.logger.info(f"Transformed data saved to: {output_file}")
            self.logger.info(f"Transformation log saved to: {mapping_log_file}")
        
        return mapped_data
    
    def _apply_value_transformations(self, data: Dict[str, Any], 
                                   transformations: Dict[str, Any]) -> Dict[str, Any]:
        """Apply value transformations to mapped data."""
        for field, transform_config in transformations.items():
            if field not in data:
                continue
                
            transform_type = transform_config.get("type")
            
            if transform_type == "to_array":
                # Convert comma-separated string to array
                if isinstance(data[field], str):
                    separator = transform_config.get("separator", ",")
                    data[field] = [item.strip() for item in data[field].split(separator) if item.strip()]
            
            elif transform_type == "to_string":
                # Convert value to string
                data[field] = str(data[field])
            
            elif transform_type == "normalize_media_type":
                # Normalize media type values
                type_mapping = {
                    "video": "VIDEO",
                    "audio": "AUDIO", 
                    "image": "IMAGE",
                    "doc": "DOCUMENT",
                    "document": "DOCUMENT"
                }
                if isinstance(data[field], str):
                    data[field] = type_mapping.get(data[field].lower(), data[field].upper())
            
            self.logger.debug(f"Applied {transform_type} transformation to {field}")
        
        return data
    
    def generate_field_config(self, data: Dict[str, Any], output_dir: Optional[Path] = None) -> Dict[str, Any]:
        """Generate field configuration for Vertex AI Search schema."""
        self.logger.info("Generating field configuration")
        
        schema_config = self.config_manager.schema
        field_config = {
            "field_settings": {},
            "generated_timestamp": datetime.now().isoformat()
        }
        
        # Configure searchable fields (these are customer source field names)
        for field in schema_config.searchable_fields:
            if field in data:
                field_config["field_settings"][field] = {
                    "searchable": True,
                    "retrievable": field in schema_config.retrievable_fields,
                    "indexable": field in schema_config.indexable_fields,
                    "completable": field in schema_config.completable_fields,
                    "dynamic_facetable": field in schema_config.dynamic_facetable_fields
                }
        
        # Configure Google required fields (after transformation)
        google_required_fields = ["title", "uri", "categories", "available_time", "duration"]
        for field in google_required_fields:
            if field not in field_config["field_settings"]:
                field_config["field_settings"][field] = {
                    "searchable": field in ["title", "categories"],
                    "retrievable": True,
                    "indexable": field in ["title", "categories"],
                    "completable": field == "title",
                    "dynamic_facetable": field == "categories"
                }
        
        self.logger.info(f"Generated configuration for {len(field_config['field_settings'])} fields")
        
        if output_dir:
            output_file = save_output(field_config, output_dir, "field_config.json")
            self.logger.info(f"Field configuration saved to: {output_file}")
        
        return field_config