"""
ETL Pipeline Implementation
Core ETL processing logic with AI-powered schema inference
"""

import logging
import uuid
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import pandas as pd
from dataclasses import dataclass

from src.schema_inference.engine import SchemaInferenceEngine, DataType

logger = logging.getLogger(__name__)

@dataclass
class ETLJob:
    """ETL Job representation"""
    job_id: str
    source_id: str
    status: str
    created_at: datetime
    completed_at: Optional[datetime]
    records_count: int
    schema_version: str
    target_format: str
    error_message: Optional[str] = None

class DataTransformer:
    """Data transformation utilities"""
    
    @staticmethod
    def clean_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and standardize data records"""
        cleaned_data = []
        
        for record in data:
            cleaned_record = {}
            for key, value in record.items():
                # Clean key names (remove spaces, special chars)
                clean_key = key.strip().replace(' ', '_').replace('-', '_')
                clean_key = ''.join(c for c in clean_key if c.isalnum() or c == '_')
                
                # Clean values
                if isinstance(value, str):
                    cleaned_record[clean_key] = value.strip()
                elif isinstance(value, dict):
                    cleaned_record[clean_key] = DataTransformer.clean_nested_dict(value)
                elif isinstance(value, list):
                    cleaned_record[clean_key] = DataTransformer.clean_list(value)
                else:
                    cleaned_record[clean_key] = value
            
            cleaned_data.append(cleaned_record)
        
        return cleaned_data
    
    @staticmethod
    def clean_nested_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean nested dictionary"""
        cleaned = {}
        for key, value in data.items():
            clean_key = key.strip().replace(' ', '_').replace('-', '_')
            clean_key = ''.join(c for c in clean_key if c.isalnum() or c == '_')
            
            if isinstance(value, str):
                cleaned[clean_key] = value.strip()
            elif isinstance(value, dict):
                cleaned[clean_key] = DataTransformer.clean_nested_dict(value)
            elif isinstance(value, list):
                cleaned[clean_key] = DataTransformer.clean_list(value)
            else:
                cleaned[clean_key] = value
        
        return cleaned
    
    @staticmethod
    def clean_list(data: List[Any]) -> List[Any]:
        """Clean list data"""
        cleaned = []
        for item in data:
            if isinstance(item, str):
                cleaned.append(item.strip())
            elif isinstance(item, dict):
                cleaned.append(DataTransformer.clean_nested_dict(item))
            elif isinstance(item, list):
                cleaned.append(DataTransformer.clean_list(item))
            else:
                cleaned.append(item)
        
        return cleaned
    
    @staticmethod
    def apply_transformations(data: List[Dict[str, Any]], rules: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply custom transformation rules"""
        if not rules:
            return data
        
        transformed_data = []
        
        for record in data:
            transformed_record = record.copy()
            
            # Apply field mappings
            if 'field_mappings' in rules:
                for old_field, new_field in rules['field_mappings'].items():
                    if old_field in transformed_record:
                        transformed_record[new_field] = transformed_record.pop(old_field)
            
            # Apply data type conversions
            if 'type_conversions' in rules:
                for field, target_type in rules['type_conversions'].items():
                    if field in transformed_record:
                        try:
                            if target_type == 'int':
                                transformed_record[field] = int(float(transformed_record[field]))
                            elif target_type == 'float':
                                transformed_record[field] = float(transformed_record[field])
                            elif target_type == 'str':
                                transformed_record[field] = str(transformed_record[field])
                            elif target_type == 'bool':
                                transformed_record[field] = bool(transformed_record[field])
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Type conversion failed for {field}: {e}")
            
            # Apply filters
            if 'filters' in rules:
                include_record = True
                for filter_rule in rules['filters']:
                    field = filter_rule.get('field')
                    operator = filter_rule.get('operator', '==')
                    value = filter_rule.get('value')
                    
                    if field in transformed_record:
                        record_value = transformed_record[field]
                        
                        if operator == '==' and record_value != value:
                            include_record = False
                            break
                        elif operator == '!=' and record_value == value:
                            include_record = False
                            break
                        elif operator == '>' and not (isinstance(record_value, (int, float)) and record_value > value):
                            include_record = False
                            break
                        elif operator == '<' and not (isinstance(record_value, (int, float)) and record_value < value):
                            include_record = False
                            break
                
                if include_record:
                    transformed_data.append(transformed_record)
            else:
                transformed_data.append(transformed_record)
        
        return transformed_data

class DataLoader:
    """Data loading utilities for different formats"""
    
    @staticmethod
    def to_json(data: List[Dict[str, Any]]) -> str:
        """Convert data to JSON format"""
        return json.dumps(data, indent=2, default=str)
    
    @staticmethod
    def to_csv(data: List[Dict[str, Any]]) -> str:
        """Convert data to CSV format"""
        if not data:
            return ""
        
        # Flatten nested data for CSV
        flattened_data = []
        for record in data:
            flattened_record = DataLoader._flatten_dict(record)
            flattened_data.append(flattened_record)
        
        df = pd.DataFrame(flattened_data)
        return df.to_csv(index=False)
    
    @staticmethod
    def to_parquet(data: List[Dict[str, Any]]) -> bytes:
        """Convert data to Parquet format"""
        if not data:
            return b""
        
        # Flatten nested data for Parquet
        flattened_data = []
        for record in data:
            flattened_record = DataLoader._flatten_dict(record)
            flattened_data.append(flattened_record)
        
        df = pd.DataFrame(flattened_data)
        return df.to_parquet(index=False)
    
    @staticmethod
    def _flatten_dict(data: Dict[str, Any], parent_key: str = '', separator: str = '_') -> Dict[str, Any]:
        """Flatten nested dictionary"""
        items = []
        
        for key, value in data.items():
            new_key = f"{parent_key}{separator}{key}" if parent_key else key
            
            if isinstance(value, dict):
                items.extend(DataLoader._flatten_dict(value, new_key, separator).items())
            elif isinstance(value, list):
                # Convert list to string representation for CSV/Parquet
                items.append((new_key, json.dumps(value) if value else "[]"))
            else:
                items.append((new_key, value))
        
        return dict(items)

class ETLPipeline:
    """Main ETL Pipeline class"""
    
    def __init__(self, schema_engine: SchemaInferenceEngine):
        self.schema_engine = schema_engine
        self.jobs: Dict[str, ETLJob] = {}
        self.statistics = {
            'total_jobs': 0,
            'successful_jobs': 0,
            'failed_jobs': 0,
            'total_records_processed': 0
        }
    
    async def process_async(self, 
                           data: List[Dict[str, Any]], 
                           source_id: str = "default",
                           target_format: str = "json",
                           transformation_rules: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Process data through the ETL pipeline asynchronously"""
        
        job_id = str(uuid.uuid4())
        logger.info(f"Starting ETL job {job_id} for source {source_id}")
        
        # Create job record
        job = ETLJob(
            job_id=job_id,
            source_id=source_id,
            status="processing",
            created_at=datetime.utcnow(),
            completed_at=None,
            records_count=len(data),
            schema_version="",
            target_format=target_format
        )
        self.jobs[job_id] = job
        
        try:
            # Step 1: Extract and clean data
            logger.info(f"Job {job_id}: Extracting and cleaning {len(data)} records")
            cleaned_data = DataTransformer.clean_data(data)
            
            # Step 2: Infer schema
            logger.info(f"Job {job_id}: Inferring schema")
            schema_version = self.schema_engine.infer_schema(cleaned_data, source_id)
            job.schema_version = schema_version.version
            
            # Step 3: Transform data
            logger.info(f"Job {job_id}: Applying transformations")
            if transformation_rules:
                transformed_data = DataTransformer.apply_transformations(cleaned_data, transformation_rules)
            else:
                transformed_data = cleaned_data
            
            # Step 4: Load data
            logger.info(f"Job {job_id}: Loading data to {target_format} format")
            if target_format == "json":
                output_data = DataLoader.to_json(transformed_data)
            elif target_format == "csv":
                output_data = DataLoader.to_csv(transformed_data)
            elif target_format == "parquet":
                output_data = DataLoader.to_parquet(transformed_data)
            else:
                raise ValueError(f"Unsupported target format: {target_format}")
            
            # Update job status
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            
            # Update statistics
            self.statistics['total_jobs'] += 1
            self.statistics['successful_jobs'] += 1
            self.statistics['total_records_processed'] += len(transformed_data)
            
            logger.info(f"Job {job_id}: Completed successfully. Processed {len(transformed_data)} records")
            
            return {
                "job_id": job_id,
                "status": "completed",
                "message": f"Successfully processed {len(transformed_data)} records",
                "schema_version": schema_version.version,
                "processed_records": len(transformed_data),
                "output_data": output_data,
                "schema_info": self.schema_engine.export_schema(schema_version.version)
            }
            
        except Exception as e:
            # Update job status with error
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            
            # Update statistics
            self.statistics['total_jobs'] += 1
            self.statistics['failed_jobs'] += 1
            
            logger.error(f"Job {job_id}: Failed with error: {str(e)}")
            
            return {
                "job_id": job_id,
                "status": "failed",
                "message": f"ETL job failed: {str(e)}",
                "schema_version": "",
                "processed_records": 0,
                "error": str(e)
            }
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job"""
        if job_id not in self.jobs:
            return None
        
        job = self.jobs[job_id]
        return {
            "job_id": job.job_id,
            "source_id": job.source_id,
            "status": job.status,
            "created_at": job.created_at.isoformat(),
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "records_count": job.records_count,
            "schema_version": job.schema_version,
            "target_format": job.target_format,
            "error_message": job.error_message
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get pipeline statistics"""
        return {
            **self.statistics,
            "active_jobs": len([job for job in self.jobs.values() if job.status == "processing"]),
            "total_jobs_tracked": len(self.jobs),
            "schema_versions": len(self.schema_engine.schema_versions),
            "current_schema_version": self.schema_engine.current_version
        }
    
    def list_jobs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent jobs"""
        jobs_list = []
        sorted_jobs = sorted(self.jobs.values(), key=lambda x: x.created_at, reverse=True)
        
        for job in sorted_jobs[:limit]:
            jobs_list.append({
                "job_id": job.job_id,
                "source_id": job.source_id,
                "status": job.status,
                "created_at": job.created_at.isoformat(),
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                "records_count": job.records_count,
                "schema_version": job.schema_version
            })
        
        return jobs_list
