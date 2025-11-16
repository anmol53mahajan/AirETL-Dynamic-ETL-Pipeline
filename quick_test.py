#!/usr/bin/env python3
import sys
import os
sys.path.append('.')

print("Testing imports...")

try:
    from src.schema_inference.engine import SchemaInferenceEngine
    print("✓ SchemaInferenceEngine import successful")
except Exception as e:
    print(f"✗ SchemaInferenceEngine import failed: {e}")

try:
    from src.etl.pipeline import ETLPipeline
    print("✓ ETLPipeline import successful")
except Exception as e:
    print(f"✗ ETLPipeline import failed: {e}")

print("Done.")
