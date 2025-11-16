"""
Test Setup for AirETL Pipeline
Basic tests to validate the setup and functionality
"""

import sys
import os
import logging
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_imports():
    """Test that all required modules can be imported"""
    try:
        # Test schema inference engine
        from src.schema_inference.engine import SchemaInferenceEngine, DataType, FieldSchema
        logger.info("✓ Schema inference engine imported successfully")
        
        # Test ETL pipeline
        from src.etl.pipeline import ETLPipeline, DataTransformer, DataLoader
        logger.info("✓ ETL pipeline imported successfully")
        
        return True
    except Exception as e:
        logger.error(f"✗ Import failed: {e}")
        return False

def test_schema_inference():
    """Test schema inference functionality"""
    try:
        from src.schema_inference.engine import SchemaInferenceEngine
        
        # Create sample data
        sample_data = [
            {"id": 1, "name": "John", "age": 30, "email": "john@example.com"},
            {"id": 2, "name": "Jane", "age": 25, "email": "jane@example.com"},
            {"id": 3, "name": "Bob", "age": 35, "email": "bob@example.com"}
        ]
        
        # Initialize schema engine
        engine = SchemaInferenceEngine()
        
        # Infer schema
        schema_version = engine.infer_schema(sample_data, "test")
        
        logger.info(f"✓ Schema inference successful: version={schema_version.version}, fields={len(schema_version.schema)}")
        
        # Export schema
        exported = engine.export_schema(schema_version.version)
        logger.info(f"✓ Schema export successful: {len(exported['fields'])} fields exported")
        
        return True
    except Exception as e:
        logger.error(f"✗ Schema inference test failed: {e}")
        return False

def test_etl_pipeline():
    """Test ETL pipeline functionality"""
    try:
        from src.schema_inference.engine import SchemaInferenceEngine
        from src.etl.pipeline import ETLPipeline, DataTransformer
        
        # Sample data with some messiness to clean
        sample_data = [
            {"user id": 1, "full name": "  John Doe  ", "email_address": "john@example.com", "user_age": "30"},
            {"user id": 2, "full name": "Jane Smith", "email_address": "jane@example.com", "user_age": "25"},
            {"user id": 3, "full name": " Bob Johnson ", "email_address": "bob@example.com", "user_age": "35"}
        ]
        
        # Test data cleaning
        cleaned_data = DataTransformer.clean_data(sample_data)
        logger.info(f"✓ Data cleaning successful: {len(cleaned_data)} records cleaned")
        
        # Test ETL pipeline (without async for now)
        schema_engine = SchemaInferenceEngine()
        pipeline = ETLPipeline(schema_engine)
        
        # Simulate processing
        logger.info(f"✓ ETL pipeline initialization successful")
        
        return True
    except Exception as e:
        logger.error(f"✗ ETL pipeline test failed: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality without external dependencies"""
    try:
        # Test basic Python functionality
        from src.schema_inference.engine import DataType
        
        # Check enum values
        assert DataType.STRING.value == "string"
        assert DataType.INTEGER.value == "integer"
        logger.info("✓ Basic enum functionality working")
        
        # Test datetime
        now = datetime.utcnow()
        logger.info(f"✓ Datetime functionality working: {now.isoformat()}")
        
        return True
    except Exception as e:
        logger.error(f"✗ Basic functionality test failed: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("Starting AirETL Setup Tests")
    logger.info("=" * 50)
    
    tests = [
        ("Basic Functionality", test_basic_functionality),
        ("Module Imports", test_imports),
        ("Schema Inference", test_schema_inference),
        ("ETL Pipeline", test_etl_pipeline)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        logger.info(f"\nRunning {test_name} Test...")
        try:
            if test_func():
                passed += 1
                logger.info(f"✓ {test_name} test passed")
            else:
                failed += 1
                logger.error(f"✗ {test_name} test failed")
        except Exception as e:
            failed += 1
            logger.error(f"✗ {test_name} test failed with exception: {e}")
    
    logger.info("\n" + "=" * 50)
    logger.info(f"Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        logger.info("🎉 All tests passed! AirETL setup is working correctly.")
        return True
    else:
        logger.error(f"❌ {failed} tests failed. Please check the setup.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
