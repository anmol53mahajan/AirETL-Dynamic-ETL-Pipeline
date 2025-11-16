"""
AirETL - Dynamic ETL Pipeline for Unstructured Data
Main FastAPI application entry point
"""

import logging
import uvicorn
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import json
import pandas as pd
from datetime import datetime
import asyncio
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from src.schema_inference.engine import SchemaInferenceEngine
from src.etl.pipeline import ETLPipeline
from src.parsers.unstructured_parser import UnstructuredDataParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="AirETL - Dynamic ETL Pipeline",
    description="AI-powered ETL pipeline for unstructured data with automatic schema inference",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
schema_engine = SchemaInferenceEngine()
etl_pipeline = ETLPipeline(schema_engine)
unstructured_parser = UnstructuredDataParser()

# Pydantic models
class DataRecord(BaseModel):
    """Single data record model"""
    data: Dict[str, Any]
    source_id: str = "default"

class DataBatch(BaseModel):
    """Batch of data records"""
    records: List[Dict[str, Any]]
    source_id: str = "default"

class ETLJobRequest(BaseModel):
    """ETL job request model"""
    data: List[Dict[str, Any]]
    source_id: str = "default"
    target_format: str = "json"  # json, csv, parquet
    transformation_rules: Optional[Dict[str, Any]] = None

class ETLJobResponse(BaseModel):
    """ETL job response model"""
    job_id: str
    status: str
    message: str
    schema_version: str
    processed_records: int
    created_at: datetime

# Health check endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "AirETL - Dynamic ETL Pipeline",
        "version": "1.0.0",
        "description": "AI-powered ETL pipeline for unstructured data",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "documentation": "http://localhost:8001/docs",
        "endpoints": {
            "health": "/health",
            "demo": "/demo/run",
            "schema_inference": "/schema/infer",
            "schema_versions": "/schema/versions",
            "etl_process": "/etl/process",
            "file_upload": "/etl/upload"
        }
    }

@app.get("/home", response_class=HTMLResponse)
async def homepage():
    """HTML homepage for the API"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>AirETL - Dynamic ETL Pipeline</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f8f9fa; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .feature { background: #ffffff; padding: 15px; margin: 10px 0; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .endpoint { background: #3498db; color: white; padding: 10px; margin: 5px 0; border-radius: 5px; }
            .endpoint a { color: white; text-decoration: none; }
            .demo-btn { background: #e74c3c; color: white; padding: 15px 30px; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }
            .full-width-section { width: 100%; background: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin: 20px 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 AirETL - Dynamic ETL Pipeline</h1>
                <p>AI-powered ETL pipeline for unstructured data with automatic schema inference</p>
            </div>
        
        <div class="feature">
            <h3>✨ Features</h3>
            <ul>
                <li>🤖 AI-powered automatic schema detection</li>
                <li>📊 Schema drift detection and version control</li>
                <li>🔄 Real-time data processing</li>
                <li>📁 File upload support (JSON, CSV, TXT)</li>
                <li>📈 Statistical analysis and data profiling</li>
            </ul>
        </div>



        <div class="feature">
            <h3>📁 File Upload Test</h3>
            <input type="file" id="file-input" accept=".json,.csv,.txt" style="margin: 10px 0;">
            <button class="demo-btn" onclick="uploadFile()" style="background: #9b59b6;">Upload & Process File</button>
            <div id="upload-results" class="full-width-section" style="display: none; background: linear-gradient(135deg, #27ae60, #2ecc71); color: white;">
                <div id="upload-content"></div>
            </div>
        </div>
        
        <div id="upload-data-results" class="full-width-section" style="display: none; background: #ffffff; color: #2c3e50; max-height: none; overflow-y: visible;">
            <h3 style="color: #2c3e50; margin: 0 0 20px 0; font-size: 1.4em; border-bottom: 3px solid #3498db; padding-bottom: 10px;">📋 Processed File Data</h3>
            <div id="upload-data-content"></div>
        </div>
        </div>

        <script>
        
        function toggleTableRows(dataType) {
            const rows = document.querySelectorAll('.table-row-' + dataType);
            const btn = document.getElementById('toggle-btn-' + dataType);
            const maxInitialRows = 5;
            let hiddenRows = 0;
            
            rows.forEach((row, index) => {
                if (index >= maxInitialRows) {
                    if (row.style.display === 'none') {
                        row.style.display = '';
                    } else {
                        row.style.display = 'none';
                        hiddenRows++;
                    }
                }
            });
            
            if (hiddenRows > 0) {
                btn.textContent = `Show All ${rows.length} Records`;
                btn.style.background = '#3498db';
            } else {
                btn.textContent = `Show Less (${maxInitialRows} Records)`;
                btn.style.background = '#e74c3c';
            }
        }
        
        function showFullContent(title, content) {
            const modal = document.createElement('div');
            modal.style.cssText = `
                position: fixed; top: 0; left: 0; width: 100%; height: 100%;
                background: rgba(0,0,0,0.7); z-index: 10000; display: flex;
                align-items: center; justify-content: center; padding: 20px;
            `;
            
            const modalContent = document.createElement('div');
            modalContent.style.cssText = `
                background: white; border-radius: 8px; padding: 20px;
                max-width: 80%; max-height: 80%; overflow: auto;
                box-shadow: 0 8px 32px rgba(0,0,0,0.3);
            `;
            
            modalContent.innerHTML = `
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h3 style="margin: 0; color: #2c3e50;">${title}</h3>
                    <button onclick="this.closest('.modal-overlay').remove()" 
                            style="background: #e74c3c; color: white; border: none; border-radius: 50%; width: 30px; height: 30px; cursor: pointer; font-size: 16px;">×</button>
                </div>
                <pre style="background: #f8f9fa; padding: 15px; border-radius: 5px; overflow: auto; white-space: pre-wrap; word-wrap: break-word; max-height: 400px; font-family: 'Courier New', monospace; font-size: 0.9em;">${content}</pre>
                <div style="text-align: right; margin-top: 15px;">
                    <button onclick="navigator.clipboard.writeText(\`${content.replace(/\`/g, '\\\\`')}\`); this.textContent='Copied!'; setTimeout(() => this.textContent='Copy to Clipboard', 2000);"
                            style="background: #27ae60; color: white; border: none; padding: 8px 16px; border-radius: 5px; cursor: pointer; margin-right: 10px;">Copy to Clipboard</button>
                    <button onclick="this.closest('.modal-overlay').remove()"
                            style="background: #95a5a6; color: white; border: none; padding: 8px 16px; border-radius: 5px; cursor: pointer;">Close</button>
                </div>
            `;
            
            modal.className = 'modal-overlay';
            modal.appendChild(modalContent);
            document.body.appendChild(modal);
            
            // Close on background click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    modal.remove();
                }
            });
            
            // Close on Escape key
            const escapeHandler = (e) => {
                if (e.key === 'Escape') {
                    modal.remove();
                    document.removeEventListener('keydown', escapeHandler);
                }
            };
            document.addEventListener('keydown', escapeHandler);
        }
        
        function flattenObject(obj, prefix = '') {
            const flattened = {};
            
            for (const key in obj) {
                if (obj.hasOwnProperty(key)) {
                    const newKey = prefix ? `${prefix}.${key}` : key;
                    
                    if (obj[key] === null || obj[key] === undefined) {
                        flattened[newKey] = '';
                    } else if (Array.isArray(obj[key])) {
                        flattened[newKey] = `[${obj[key].length} items]`;
                        // Also show first few items if they're simple values
                        if (obj[key].length > 0 && typeof obj[key][0] !== 'object') {
                            flattened[newKey] = JSON.stringify(obj[key]);
                        }
                    } else if (typeof obj[key] === 'object') {
                        Object.assign(flattened, flattenObject(obj[key], newKey));
                    } else {
                        flattened[newKey] = obj[key];
                    }
                }
            }
            
            return flattened;
        }
        
        function showStructuringExplanation(sampleRecord, schemaInfo) {
            const structureDiv = document.getElementById('structure-explanation');
            const structureContent = document.getElementById('structure-content');
            
            let explanationHtml = `
                <div style="font-size: 0.9em; line-height: 1.6;">
                    <h5>🔄 ETL Processing Steps:</h5>
                    <ol style="margin: 10px 0; padding-left: 20px;">
                        <li><strong>Extraction:</strong> Raw unstructured data was ingested</li>
                        <li><strong>Type Detection:</strong> AI analyzed each field and detected data types with confidence scores</li>
                        <li><strong>Schema Creation:</strong> Generated schema version ${schemaInfo.version} with ${Object.keys(schemaInfo.fields).length} fields</li>
                        <li><strong>Data Validation:</strong> Verified data against detected schema</li>
                        <li><strong>Transformation:</strong> Applied data cleaning and standardization</li>
                        <li><strong>Loading:</strong> Structured data ready for consumption</li>
                    </ol>
                    
                    <h5>📊 Data Structure Analysis:</h5>
                    <div style="background: #d35400; padding: 10px; border-radius: 5px; margin: 10px 0;">
            `;
            
            // Analyze structure
            const analysis = analyzeDataStructure(sampleRecord);
            explanationHtml += analysis;
            
            explanationHtml += `
                    </div>
                    
                    <h5>🎯 Key Insights:</h5>
                    <ul style="margin: 10px 0; padding-left: 20px;">
                        <li>Nested objects were flattened for tabular display</li>
                        <li>Arrays were summarized or expanded based on content</li>
                        <li>Data types were normalized (dates, numbers, booleans)</li>
                        <li>Null values were handled gracefully</li>
                    </ul>
                </div>
            `;
            
            structureContent.innerHTML = explanationHtml;
            structureDiv.style.display = 'block';
        }
        
        function analyzeDataStructure(record) {
            let analysis = '<ul style="margin: 0; padding-left: 15px;">';
            
            const stats = {
                simpleFields: 0,
                nestedObjects: 0,
                arrays: 0,
                totalFields: 0
            };
            
            function analyzeObject(obj, prefix = '') {
                for (const key in obj) {
                    if (obj.hasOwnProperty(key)) {
                        stats.totalFields++;
                        const fullKey = prefix ? `${prefix}.${key}` : key;
                        
                        if (Array.isArray(obj[key])) {
                            stats.arrays++;
                            analysis += `<li><strong>${fullKey}</strong>: Array with ${obj[key].length} items</li>`;
                        } else if (typeof obj[key] === 'object' && obj[key] !== null) {
                            stats.nestedObjects++;
                            analysis += `<li><strong>${fullKey}</strong>: Nested object with ${Object.keys(obj[key]).length} properties</li>`;
                            analyzeObject(obj[key], fullKey);
                        } else {
                            stats.simpleFields++;
                            analysis += `<li><strong>${fullKey}</strong>: ${typeof obj[key]} value</li>`;
                        }
                    }
                }
            }
            
            analyzeObject(record);
            
            analysis += '</ul>';
            analysis = `
                <p><strong>Structure Overview:</strong></p>
                <ul style="margin: 5px 0; padding-left: 15px;">
                    <li>Total Fields: ${stats.totalFields}</li>
                    <li>Simple Fields: ${stats.simpleFields}</li>
                    <li>Nested Objects: ${stats.nestedObjects}</li>
                    <li>Arrays: ${stats.arrays}</li>
                </ul>
                <p><strong>Field Details:</strong></p>
                ${analysis}
            `;
            
            return analysis;
        }

        function uploadFile() {
            const fileInput = document.getElementById('file-input');
            const resultsDiv = document.getElementById('upload-results');
            const contentDiv = document.getElementById('upload-content');
            const dataDiv = document.getElementById('upload-data-results');
            const dataContent = document.getElementById('upload-data-content');
            
            if (!fileInput.files[0]) {
                alert('Please select a file first!');
                return;
            }
            
            const formData = new FormData();
            formData.append('file', fileInput.files[0]);
            
            resultsDiv.style.display = 'block';
            contentDiv.innerHTML = '🔄 Processing uploaded file...';
            dataDiv.style.display = 'none';
            
            fetch('/etl/upload', {
                method: 'POST',
                body: formData
            })
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                console.log('Upload response:', data); // Debug log
                
                contentDiv.innerHTML = `
                    <h4>✅ File Upload Successful!</h4>
                    <p><strong>File:</strong> ${data.filename || 'Unknown'}</p>
                    <p><strong>Records Processed:</strong> ${data.records_processed || 0}</p>
                    <p><strong>Job Status:</strong> ${data.job_result?.status || 'Unknown'}</p>
                    <p><strong>Schema Version:</strong> ${data.job_result?.schema_version || 'N/A'}</p>
                `;
                
                // Show processed file data if available
                if (data.job_result.output_data) {
                    try {
                        const processedData = JSON.parse(data.job_result.output_data);
                        
                        // Group data by data_type
                        const groupedData = {};
                        processedData.forEach(record => {
                            const dataType = record.data_type || 'unknown';
                            if (!groupedData[dataType]) {
                                groupedData[dataType] = [];
                            }
                            groupedData[dataType].push(record);
                        });
                        
                        let dataHtml = '<div style="margin-top: 20px;">';
                        
                        // Create navigation menu for data types
                        const dataTypes = Object.keys(groupedData).sort();
                        if (dataTypes.length > 1) {
                            dataHtml += '<h4>📊 Data Types Extracted:</h4>';
                            dataHtml += '<div style="margin: 15px 0; display: flex; flex-wrap: wrap; gap: 10px;">';
                            
                            dataTypes.forEach(dataType => {
                                const count = groupedData[dataType].length;
                                const displayName = dataType.replace(/_/g, ' ').replace(/\\b\\w/g, l => l.toUpperCase());
                                dataHtml += `<button onclick="scrollToDataType('${dataType}')" 
                                    style="background: linear-gradient(135deg, #3498db, #2980b9); color: white; border: none; padding: 10px 16px; border-radius: 8px; cursor: pointer; font-size: 0.9em; font-weight: 600; box-shadow: 0 2px 4px rgba(0,0,0,0.1); transition: all 0.2s ease;"
                                    onmouseover="this.style.transform='translateY(-2px)'; this.style.boxShadow='0 4px 8px rgba(0,0,0,0.2)'"
                                    onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='0 2px 4px rgba(0,0,0,0.1)'">
                                    ${displayName} (${count})
                                </button>`;
                            });
                            
                            dataHtml += '</div><hr style="margin: 20px 0;">';
                        }
                        
                        // Create separate table for each data type
                        dataTypes.forEach(dataType => {
                            const records = groupedData[dataType];
                            const displayName = dataType.replace(/_/g, ' ').replace(/\\b\\w/g, l => l.toUpperCase());
                            
                            dataHtml += `<div id="data-type-${dataType}" style="margin: 40px 0; padding: 20px; background: #f8f9fa; border-radius: 12px; border-left: 5px solid #3498db;">`;
                            dataHtml += `<h3 style="color: #2c3e50; margin: 0 0 20px 0; font-size: 1.3em; font-weight: 700;">
                                📋 ${displayName} <span style="color: #7f8c8d; font-weight: 400; font-size: 0.8em;">(${records.length} records)</span>
                            </h3>`;
                            
                            // Get unique keys for this data type
                            const typeKeys = new Set();
                            records.forEach(record => {
                                Object.keys(flattenObject(record)).forEach(key => {
                                    if (key !== 'data_type') { // Exclude data_type since it's the same for all
                                        typeKeys.add(key);
                                    }
                                });
                            });
                            
                            const keys = Array.from(typeKeys).sort();
                            
                            if (keys.length > 0) {
                                dataHtml += `<div style="overflow-x: auto; margin: 15px 0; background: white; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1);">`;
                                dataHtml += `<table style="width: 100%; border-collapse: collapse; font-size: 0.9em; background: white; border-radius: 8px; overflow: hidden;">`;
                                
                                // Table header
                                dataHtml += `<thead><tr style="background: linear-gradient(135deg, #2c3e50, #34495e);">`;
                                keys.forEach(key => {
                                    dataHtml += `<th style="border: none; padding: 15px 12px; text-align: left; color: white; font-weight: 600; white-space: nowrap; font-size: 0.95em;">${key}</th>`;
                                });
                                dataHtml += '</tr></thead><tbody>';
                                
                                // Add show more/less controls if there are many records
                                const maxInitialRows = 5;
                                const showExpandControl = records.length > maxInitialRows;
                                
                                // Table rows
                                records.forEach((record, index) => {
                                    const flatRecord = flattenObject(record);
                                    const bgColor = index % 2 === 0 ? '#ffffff' : '#f8f9fa';
                                    const isHidden = showExpandControl && index >= maxInitialRows;
                                    const rowClass = `table-row-${dataType}`;
                                    const hiddenStyle = isHidden ? ' display: none;' : '';
                                    
                                    dataHtml += `<tr class="${rowClass}" style="background: ${bgColor}; transition: all 0.2s ease; border-bottom: 1px solid #e9ecef;${hiddenStyle}" 
                                        onmouseover="this.style.background='#e8f4fd'; this.style.transform='scale(1.01)'" 
                                        onmouseout="this.style.background='${bgColor}'; this.style.transform='scale(1)'>">`;
                                    
                                    keys.forEach(key => {
                                        let value = flatRecord[key];
                                        let displayValue = '';
                                        let cellStyle = 'border: none; padding: 12px; color: #2c3e50; line-height: 1.4; position: relative;';
                                        
                                        if (value === null || value === undefined) {
                                            displayValue = '<em style="color: #999;">null</em>';
                                        } else if (typeof value === 'boolean') {
                                            displayValue = value ? '✅ true' : '❌ false';
                                        } else if (typeof value === 'object') {
                                            if (Array.isArray(value)) {
                                                const fullArrayText = JSON.stringify(value, null, 2);
                                                displayValue = `<div style="cursor: pointer; padding: 5px; border-radius: 3px; border: 1px solid #e0e0e0; background: #f9f9f9;" onclick="showFullContent('Array with ${value.length} items', \`${fullArrayText.replace(/`/g, '\\\\`').replace(/\\$/g, '\\\\$')}\`)">
                                                    <strong>📋 [${value.length} items]</strong>
                                                    <br><small style="color: #666; font-family: monospace;">${JSON.stringify(value).substring(0, 60)}...</small>
                                                    <br><small style="color: #3498db; font-weight: bold;">👆 Click to expand</small>
                                                </div>`;
                                            } else {
                                                const fullObjectText = JSON.stringify(value, null, 2);
                                                displayValue = `<div style="cursor: pointer; padding: 5px; border-radius: 3px; border: 1px solid #e0e0e0; background: #f9f9f9;" onclick="showFullContent('Object with ${Object.keys(value).length} properties', \`${fullObjectText.replace(/`/g, '\\\\`').replace(/\\$/g, '\\\\$')}\`)">
                                                    <strong>🏷️ {${Object.keys(value).length} props}</strong>
                                                    <br><small style="color: #666; font-family: monospace;">${JSON.stringify(value).substring(0, 60)}...</small>
                                                    <br><small style="color: #3498db; font-weight: bold;">👆 Click to expand</small>
                                                </div>`;
                                            }
                                        } else if (typeof value === 'string' && value.startsWith('http')) {
                                            displayValue = `<a href="${value}" target="_blank" style="color: #8e44ad; text-decoration: none;">${value.length > 50 ? value.substring(0, 47) + '...' : value}</a>`;
                                        } else {
                                            const valueStr = String(value);
                                            if (valueStr.length > 80) {
                                                displayValue = `<div style="cursor: pointer; padding: 5px; border-radius: 3px; border: 1px solid #e0e0e0; background: #f9f9f9;" onclick="showFullContent('Full text (${valueStr.length} characters)', \`${valueStr.replace(/`/g, '\\\\`').replace(/\\$/g, '\\\\$')}\`)">
                                                    <span>${valueStr.substring(0, 77)}...</span>
                                                    <br><small style="color: #3498db; font-weight: bold;">📄 Click to view full content (${valueStr.length} chars)</small>
                                                </div>`;
                                            } else {
                                                displayValue = valueStr;
                                            }
                                        }
                                        
                                        dataHtml += `<td style="${cellStyle}" title="${String(value || '')}">${displayValue}</td>`;
                                    });
                                    
                                    dataHtml += '</tr>';
                                });
                                
                                dataHtml += '</tbody></table>';
                                
                                // Add show more button after table if needed
                                if (showExpandControl) {
                                    dataHtml += `<div style="text-align: center; margin: 10px 0; padding: 15px; background: #f8f9fa; border-radius: 5px;">
                                        <button onclick="toggleTableRows('${dataType}')" 
                                                id="toggle-btn-${dataType}"
                                                style="background: #3498db; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-size: 0.9em; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                            📊 Show All ${records.length} Records (Currently showing 5)
                                        </button>
                                    </div>`;
                                }
                                
                                dataHtml += '</div>';
                            } else {
                                dataHtml += '<p><em>No structured fields found for this data type.</em></p>';
                            }
                            
                            dataHtml += '</div>';
                        });
                        
                        dataHtml += '</div>';
                        dataContent.innerHTML = dataHtml;
                        dataDiv.style.display = 'block';
                    } catch (e) {
                        console.error('Could not parse processed data:', e);
                    }
                }
                
                resultsDiv.scrollIntoView({ behavior: 'smooth' });
            })
            .catch(error => {
                contentDiv.innerHTML = `❌ Upload failed: ${error.message}`;
                console.error('Upload error:', error);
            });
        }
        
        function scrollToDataType(dataType) {
            const element = document.getElementById(`data-type-${dataType}`);
            if (element) {
                element.scrollIntoView({ behavior: 'smooth', block: 'start' });
                // Add highlight effect
                element.style.border = '2px solid #8e44ad';
                element.style.borderRadius = '5px';
                element.style.padding = '10px';
                setTimeout(() => {
                    element.style.border = '';
                    element.style.borderRadius = '';
                    element.style.padding = '';
                }, 2000);
            }
        }
        </script>
        </div>
    </body>
    </html>
    """
    return html_content

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "schema_engine": "active",
            "etl_pipeline": "active"
        }
    }

# Schema inference endpoints
@app.post("/schema/infer")
async def infer_schema(batch: DataBatch):
    """Infer schema from a batch of data records"""
    try:
        logger.info(f"Inferring schema for {len(batch.records)} records from source: {batch.source_id}")
        
        schema_version = schema_engine.infer_schema(batch.records, batch.source_id)
        
        return {
            "schema_version": schema_version.version,
            "timestamp": schema_version.timestamp.isoformat(),
            "fields_count": len(schema_version.schema),
            "hash": schema_version.hash,
            "changes": schema_version.changes,
            "schema": schema_engine.export_schema(schema_version.version)
        }
    except Exception as e:
        logger.error(f"Schema inference failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Schema inference failed: {str(e)}")

@app.get("/schema/versions")
async def get_schema_versions():
    """Get all schema versions"""
    try:
        versions = []
        for version_id, schema_version in schema_engine.schema_versions.items():
            versions.append({
                "version": version_id,
                "timestamp": schema_version.timestamp.isoformat(),
                "hash": schema_version.hash,
                "fields_count": len(schema_version.schema),
                "changes": schema_version.changes
            })
        
        return {
            "versions": versions,
            "total_count": len(versions),
            "current_version": schema_engine.current_version
        }
    except Exception as e:
        logger.error(f"Failed to get schema versions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get schema versions: {str(e)}")

@app.get("/schema/export/{version_id}")
async def export_schema(version_id: str):
    """Export a specific schema version"""
    try:
        if version_id not in schema_engine.schema_versions:
            raise HTTPException(status_code=404, detail=f"Schema version {version_id} not found")
        
        schema_export = schema_engine.export_schema(version_id)
        return schema_export
    except Exception as e:
        logger.error(f"Failed to export schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to export schema: {str(e)}")

# ETL pipeline endpoints
@app.post("/etl/process", response_model=ETLJobResponse)
async def process_etl_job(job_request: ETLJobRequest):
    """Process an ETL job with the provided data"""
    try:
        logger.info(f"Processing ETL job for {len(job_request.data)} records")
        
        # Run ETL pipeline
        result = await etl_pipeline.process_async(
            data=job_request.data,
            source_id=job_request.source_id,
            target_format=job_request.target_format,
            transformation_rules=job_request.transformation_rules
        )
        
        return ETLJobResponse(
            job_id=result["job_id"],
            status=result["status"],
            message=result["message"],
            schema_version=result["schema_version"],
            processed_records=result["processed_records"],
            created_at=datetime.utcnow()
        )
    except Exception as e:
        logger.error(f"ETL processing failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"ETL processing failed: {str(e)}")

@app.post("/etl/upload")
async def upload_file(file: UploadFile = File(...), source_id: str = "upload"):
    """Upload and process a file through the ETL pipeline"""
    try:
        logger.info(f"Processing uploaded file: {file.filename}")
        
        # Read file content
        content = await file.read()
        
        # Parse based on file type
        if file.filename.endswith('.json'):
            data = json.loads(content.decode('utf-8'))
            if isinstance(data, dict):
                data = [data]  # Convert single object to list
        elif file.filename.endswith('.csv'):
            df = pd.read_csv(pd.io.common.StringIO(content.decode('utf-8')))
            data = df.to_dict('records')
        elif file.filename.endswith('.txt'):
            # Use unstructured parser for complex text files
            text_content = content.decode('utf-8')
            data = unstructured_parser.parse_file(text_content, file.filename)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Use JSON, CSV, or TXT.")
        
        # Process through ETL pipeline
        result = await etl_pipeline.process_async(
            data=data,
            source_id=source_id,
            target_format="json"
        )
        
        return {
            "filename": file.filename,
            "records_processed": len(data),
            "job_result": result
        }
    except Exception as e:
        logger.error(f"File upload processing failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"File upload processing failed: {str(e)}")

@app.get("/etl/stats")
async def get_etl_stats():
    """Get ETL pipeline statistics"""
    try:
        stats = etl_pipeline.get_statistics()
        return stats
    except Exception as e:
        logger.error(f"Failed to get ETL stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get ETL stats: {str(e)}")

@app.get("/config")
async def get_configuration():
    """Get current configuration from environment variables"""
    try:
        config = {
            "api": {
                "host": os.getenv("API_HOST", "0.0.0.0"),
                "port": int(os.getenv("API_PORT", 8001)),
                "workers": int(os.getenv("API_WORKERS", 4)),
                "log_level": os.getenv("LOG_LEVEL", "INFO")
            },
            "database": {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "database": os.getenv("POSTGRES_DB", "etl_database"),
                "user": os.getenv("POSTGRES_USER", "etl_user")
            },
            "kafka": {
                "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                "input_topic": os.getenv("KAFKA_TOPIC_INPUT", "etl_input"),
                "output_topic": os.getenv("KAFKA_TOPIC_OUTPUT", "etl_output"),
                "consumer_group": os.getenv("KAFKA_CONSUMER_GROUP", "etl_consumer_group")
            },
            "redis": {
                "host": os.getenv("REDIS_HOST", "localhost"),
                "port": int(os.getenv("REDIS_PORT", 6379)),
                "url": os.getenv("REDIS_URL", "redis://localhost:6379")
            },
            "features": {
                "schema_evolution": os.getenv("ENABLE_SCHEMA_EVOLUTION", "true").lower() == "true",
                "auto_healing": os.getenv("ENABLE_AUTO_HEALING", "true").lower() == "true",
                "metrics": os.getenv("ENABLE_METRICS", "true").lower() == "true",
                "alerting": os.getenv("ENABLE_ALERTING", "true").lower() == "true"
            },
            "monitoring": {
                "prometheus_port": int(os.getenv("PROMETHEUS_PORT", 9090)),
                "grafana_port": int(os.getenv("GRAFANA_PORT", 3000))
            }
        }
        return config
    except Exception as e:
        logger.error(f"Failed to get configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get configuration: {str(e)}")

# Demo endpoint with sample data
@app.post("/demo/run")
async def run_demo():
    """Run a demonstration of the ETL pipeline with sample data"""
    try:
        # Sample unstructured data
        sample_data = [
            {
                "user_id": 1,
                "name": "John Doe", 
                "email": "john.doe@example.com",
                "age": 30,
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "zipcode": "10001"
                },
                "preferences": ["music", "sports", "technology"],
                "signup_date": "2023-01-15T10:30:00"
            },
            {
                "user_id": 2,
                "name": "Jane Smith",
                "email": "jane.smith@example.com", 
                "age": 25,
                "address": {
                    "street": "456 Oak Ave",
                    "city": "Los Angeles",
                    "zipcode": "90210"
                },
                "preferences": ["books", "travel"],
                "signup_date": "2023-02-20T14:15:00"
            },
            {
                "user_id": 3,
                "name": "Bob Johnson",
                "email": "bob.johnson@example.com",
                "age": 35,
                "address": {
                    "street": "789 Pine Rd",
                    "city": "Chicago", 
                    "zipcode": "60601"
                },
                "preferences": ["cooking", "photography", "music"],
                "signup_date": "2023-03-10T09:45:00"
            }
        ]
        
        logger.info("Running demo with sample data")
        
        # Process through ETL pipeline
        result = await etl_pipeline.process_async(
            data=sample_data,
            source_id="demo",
            target_format="json"
        )
        
        return {
            "message": "Demo completed successfully",
            "sample_data_records": len(sample_data),
            "etl_result": result
        }
    except Exception as e:
        logger.error(f"Demo failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Demo failed: {str(e)}")

if __name__ == "__main__":
    logger.info("Starting AirETL server...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
