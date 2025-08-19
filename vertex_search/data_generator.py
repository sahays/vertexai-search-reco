"""Data generator for creating sample data based on JSON schemas."""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
from faker import Faker

from .interfaces import DataGeneratorInterface


class DataGenerator(DataGeneratorInterface):
    """Generates sample data based on JSON schema specifications."""
    
    def __init__(self, locale: str = 'en_US'):
        self.fake = Faker(locale)
        
    def generate_sample_data(
        self, 
        schema: Dict[str, Any], 
        count: int,
        seed: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Generate sample data conforming to the given schema."""
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)
        
        schema_analysis = self.analyze_schema(schema)
        data = []
        
        for _ in range(count):
            record = self._generate_record(schema, schema_analysis)
            data.append(record)
        
        return data
    
    def analyze_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze schema to understand field types and constraints."""
        analysis = {
            'properties': {},
            'required_fields': schema.get('required', []),
            'field_types': {},
            'enums': {},
            'constraints': {}
        }
        
        properties = schema.get('properties', {})
        
        for field_name, field_spec in properties.items():
            field_type = field_spec.get('type')
            field_format = field_spec.get('format')
            field_enum = field_spec.get('enum')
            
            analysis['properties'][field_name] = field_spec
            analysis['field_types'][field_name] = field_type
            
            if field_enum:
                analysis['enums'][field_name] = field_enum
            
            # Extract constraints
            constraints = {}
            if 'minimum' in field_spec:
                constraints['minimum'] = field_spec['minimum']
            if 'maximum' in field_spec:
                constraints['maximum'] = field_spec['maximum']
            if 'minLength' in field_spec:
                constraints['minLength'] = field_spec['minLength']
            if 'maxLength' in field_spec:
                constraints['maxLength'] = field_spec['maxLength']
            if 'minItems' in field_spec:
                constraints['minItems'] = field_spec['minItems']
            if 'maxItems' in field_spec:
                constraints['maxItems'] = field_spec['maxItems']
            
            if constraints:
                analysis['constraints'][field_name] = constraints
        
        return analysis
    
    def _generate_record(self, schema: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a single record based on schema analysis."""
        record = {}
        properties = analysis['properties']
        required_fields = analysis['required_fields']
        
        for field_name, field_spec in properties.items():
            # Generate required fields always, optional fields 80% of the time
            should_generate = (
                field_name in required_fields or 
                random.random() < 0.8
            )
            
            if should_generate:
                value = self._generate_field_value(field_name, field_spec, analysis)
                if value is not None:
                    record[field_name] = value
        
        return record
    
    def _generate_field_value(
        self, 
        field_name: str, 
        field_spec: Dict[str, Any], 
        analysis: Dict[str, Any]
    ) -> Any:
        """Generate a value for a specific field based on its specification."""
        field_type = field_spec.get('type')
        field_format = field_spec.get('format')
        field_enum = field_spec.get('enum')
        constraints = analysis['constraints'].get(field_name, {})
        
        # Handle enum values first
        if field_enum:
            return random.choice(field_enum)
        
        # Handle different types
        if field_type == 'string':
            return self._generate_string_value(field_name, field_format, constraints)
        elif field_type == 'integer':
            return self._generate_integer_value(constraints)
        elif field_type == 'number':
            return self._generate_number_value(constraints)
        elif field_type == 'boolean':
            return random.choice([True, False])
        elif field_type == 'array':
            return self._generate_array_value(field_spec, analysis)
        elif field_type == 'object':
            return self._generate_object_value(field_spec)
        else:
            return None
    
    def _generate_string_value(
        self, 
        field_name: str, 
        field_format: Optional[str], 
        constraints: Dict[str, Any]
    ) -> str:
        """Generate string values based on field name hints and format."""
        min_length = constraints.get('minLength', 1)
        max_length = constraints.get('maxLength', 200)
        
        # Handle specific formats
        if field_format == 'date-time':
            base_time = datetime.now() - timedelta(days=random.randint(0, 365))
            return base_time.isoformat()
        elif field_format == 'date':
            base_date = datetime.now() - timedelta(days=random.randint(0, 365))
            return base_date.date().isoformat()
        elif field_format == 'email':
            return self.fake.email()
        elif field_format == 'uri':
            return self.fake.url()
        
        # Generate based on field name patterns
        field_lower = field_name.lower()
        
        if 'id' in field_lower:
            return str(uuid.uuid4())
        elif any(word in field_lower for word in ['title', 'name']):
            return self.fake.sentence(nb_words=random.randint(2, 6)).rstrip('.')
        elif 'description' in field_lower:
            return self.fake.paragraph(nb_sentences=random.randint(1, 3))
        elif 'email' in field_lower:
            return self.fake.email()
        elif 'phone' in field_lower:
            return self.fake.phone_number()
        elif 'address' in field_lower:
            return self.fake.address()
        elif 'city' in field_lower:
            return self.fake.city()
        elif 'country' in field_lower:
            return self.fake.country()
        elif 'url' in field_lower or 'link' in field_lower:
            return self.fake.url()
        elif 'tag' in field_lower:
            return self.fake.word()
        else:
            # Generate random string within constraints
            target_length = min(max_length, max(min_length, random.randint(5, 50)))
            return self.fake.text(max_nb_chars=target_length).strip()
    
    def _generate_integer_value(self, constraints: Dict[str, Any]) -> int:
        """Generate integer value within constraints."""
        minimum = constraints.get('minimum', 0)
        maximum = constraints.get('maximum', 1000000)
        return random.randint(minimum, maximum)
    
    def _generate_number_value(self, constraints: Dict[str, Any]) -> float:
        """Generate number value within constraints."""
        minimum = constraints.get('minimum', 0.0)
        maximum = constraints.get('maximum', 1000000.0)
        return round(random.uniform(minimum, maximum), 2)
    
    def _generate_array_value(
        self, 
        field_spec: Dict[str, Any], 
        analysis: Dict[str, Any]
    ) -> List[Any]:
        """Generate array value based on items specification."""
        items_spec = field_spec.get('items', {})
        min_items = field_spec.get('minItems', 0)
        max_items = field_spec.get('maxItems', 10)
        
        array_length = random.randint(min_items, min(max_items, 5))
        array_values = []
        
        for _ in range(array_length):
            if isinstance(items_spec, dict):
                # Single item type
                item_value = self._generate_field_value('item', items_spec, analysis)
                if item_value is not None:
                    array_values.append(item_value)
            elif isinstance(items_spec, list):
                # Multiple item types (tuple validation)
                for item_spec in items_spec[:array_length]:
                    item_value = self._generate_field_value('item', item_spec, analysis)
                    if item_value is not None:
                        array_values.append(item_value)
                        break
        
        return array_values
    
    def _generate_object_value(self, field_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Generate object value based on properties specification."""
        properties = field_spec.get('properties', {})
        required = field_spec.get('required', [])
        
        obj = {}
        for prop_name, prop_spec in properties.items():
            # Generate required properties and 70% of optional ones
            should_generate = prop_name in required or random.random() < 0.7
            
            if should_generate:
                # Create simplified analysis for nested object
                nested_analysis = {
                    'properties': {prop_name: prop_spec},
                    'required_fields': required,
                    'field_types': {prop_name: prop_spec.get('type')},
                    'enums': {},
                    'constraints': {}
                }
                
                if prop_spec.get('enum'):
                    nested_analysis['enums'][prop_name] = prop_spec['enum']
                
                value = self._generate_field_value(prop_name, prop_spec, nested_analysis)
                if value is not None:
                    obj[prop_name] = value
        
        return obj