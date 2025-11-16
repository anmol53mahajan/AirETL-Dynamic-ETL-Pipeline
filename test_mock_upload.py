#!/usr/bin/env python3
"""
Test script to upload mock_official.txt to the AirETL API
"""

import requests
import json

def test_mock_file_upload():
    """Test uploading the mock_official.txt file"""
    
    url = "http://localhost:8000/etl/upload"
    
    # Read the mock file
    with open("mock_official.txt", "r") as f:
        file_content = f.read()
    
    # Prepare the file for upload
    files = {
        'file': ('mock_official.txt', file_content, 'text/plain')
    }
    
    # Upload the file
    response = requests.post(url, files=files)
    
    if response.status_code == 200:
        result = response.json()
        print("✅ Upload successful!")
        print(f"📁 Filename: {result['filename']}")
        print(f"📊 Records processed: {result['records_processed']}")
        
        job_result = result['job_result']
        print(f"🔄 Job Status: {job_result['status']}")
        print(f"🆔 Job ID: {job_result['job_id']}")
        
        if 'schema_info' in job_result:
            schema_info = job_result['schema_info']
            print(f"📋 Schema Version: {schema_info['version']}")
            print(f"⏰ Timestamp: {schema_info['timestamp']}")
            print(f"🔧 Fields detected: {len(schema_info['fields'])}")
            
            print("\n📈 Field Analysis:")
            for field_name, field_info in schema_info['fields'].items():
                print(f"  • {field_name}: {field_info['type']} ({field_info['confidence']*100:.1f}% confidence)")
        
        # Show some sample structured data
        if 'output_data' in job_result:
            try:
                output_data = json.loads(job_result['output_data'])
                print(f"\n📦 Sample structured records (showing first 3 of {len(output_data)}):")
                for i, record in enumerate(output_data[:3]):
                    print(f"\n  Record {i+1} ({record.get('data_type', 'unknown')}):")
                    for key, value in list(record.items())[:5]:  # Show first 5 fields
                        if key not in ['source_file', 'processed_at']:
                            print(f"    {key}: {str(value)[:100]}")
                    if len(record) > 5:
                        print(f"    ... and {len(record) - 5} more fields")
            except json.JSONDecodeError:
                print("📋 Output data available but not JSON formatted")
    
    else:
        print(f"❌ Upload failed: {response.status_code}")
        print(f"Error: {response.text}")

if __name__ == "__main__":
    test_mock_file_upload()
