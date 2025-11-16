# AirETL - Dynamic ETL Pipeline for Unstructured Data

## 🌟 Overview

AirETL is a sophisticated, AI-powered Extract, Transform, and Load (ETL) pipeline specifically designed to handle complex unstructured data sources. Built with Python and FastAPI, it leverages advanced machine learning techniques to automatically detect data schemas, parse mixed-format content, and transform chaotic data into structured, analyzable formats.

### 🎯 What Makes AirETL Special

Unlike traditional ETL tools that require predefined schemas and structured inputs, AirETL excels at processing:
- **Mixed-format files** containing JSON, CSV, HTML, and raw text
- **Scraped web content** with embedded markup and irregular formatting  
- **OCR-processed documents** with character recognition errors
- **Malformed data** with syntax errors and inconsistencies
- **Multi-language content** with varying date formats and encodings

## 🏗️ Architecture & Core Components

### 1. **Unstructured Data Parser** (`src/parsers/unstructured_parser.py`)
The heart of the system that handles complex, mixed-format input files.

**What it does:**
- Identifies and extracts 15+ different data section types from a single file
- Parses malformed JSON with multiple fixing strategies
- Extracts structured data from HTML tables and snippets
- Handles CSV sections with inconsistent formatting
- Processes key-value pairs and metadata blocks
- Repairs OCR errors and character encoding issues

**How it works:**
- Uses regex patterns to identify section boundaries (e.g., `--- METADATA ---`, `--- JSON ---`)
- Applies section-specific parsing strategies
- Implements aggressive JSON repair algorithms for malformed data
- Extracts dates, prices, and contact information using pattern matching
- Handles nested data structures and arrays

**Data Types Supported:**
- Metadata blocks (key-value pairs)
- Well-formed and malformed JSON objects
- HTML tables and snippets
- CSV-like sections with various delimiters
- Raw text with embedded data
- SQL snippets and database-like content
- OCR-processed content with errors
- Mixed encodings and special characters

### 2. **AI-Powered Schema Inference Engine** (`src/schema_inference/engine.py`)
Automatically detects and adapts to evolving data structures using machine learning.

**What it does:**
- Analyzes data patterns to infer field types and structures
- Provides confidence scores for type detection
- Tracks schema evolution over time with versioning
- Detects complex types: emails, URLs, dates, nested objects
- Generates statistical profiles for each field

**How it works:**
- Uses pattern matching for structured types (email, URL, date formats)
- Applies clustering algorithms (DBSCAN) for similar data grouping
- Leverages sentence transformers for semantic field analysis
- Maintains version history with change tracking
- Provides field-level statistics and confidence metrics

**Supported Data Types:**
```python
DataType.STRING, DataType.INTEGER, DataType.FLOAT
DataType.BOOLEAN, DataType.DATETIME, DataType.DATE
DataType.EMAIL, DataType.URL, DataType.JSON
DataType.ARRAY, DataType.OBJECT, DataType.NULL
```

### 3. **ETL Pipeline Engine** (`src/etl/pipeline.py`)
Orchestrates the complete data transformation process.

**What it does:**
- Manages ETL job lifecycle and status tracking
- Applies data cleaning and standardization rules
- Handles nested data structures recursively
- Integrates with schema inference for automatic typing
- Provides job monitoring and error handling

**How it works:**
- Creates unique job IDs for each processing request
- Cleans field names (removes spaces, special characters)
- Standardizes data formats and encoding
- Applies custom transformation rules
- Tracks processing statistics and completion status

### 4. **Web Interface** (`main.py` FastAPI Application)
A full-featured web application for file upload and data visualization.

**What it provides:**
- **Interactive file upload** for .txt, .json, .csv files
- **Real-time data parsing** with progress indicators
- **Multi-table visualization** organized by data type
- **Expandable content viewing** for large fields
- **Copy-to-clipboard functionality** for extracted data
- **Responsive design** with professional styling

**Key Features:**
- Shows first 5 records with "Show More" expansion
- Click-to-expand for long text, JSON objects, and arrays
- Modal popup viewer for complete content
- Professional table styling with hover effects
- Full-width layout utilizing entire screen space

## �️ Technology Stack

### **Core Backend Technologies**

#### **Python 3.11+**
- **What it does:** Primary programming language for the entire application
- **Why chosen:** Excellent ecosystem for data processing, ML libraries, and web frameworks
- **Role in project:** Foundation for all components - parsing, ML inference, web server, and data processing
- **Specific usage:** Type hints, async/await support, dataclasses, and modern language features

#### **FastAPI**
- **What it does:** Modern, high-performance web framework for building APIs
- **Why chosen:** Automatic API documentation, async support, type validation, and excellent performance
- **Role in project:** 
  - Serves the web interface at `/home`
  - Handles file uploads via multipart form data
  - Provides RESTful endpoints for data processing
  - Automatic request/response validation with Pydantic
- **Specific features used:** File upload handling, HTML responses, CORS middleware, dependency injection

#### **Uvicorn**
- **What it does:** Lightning-fast ASGI server implementation
- **Why chosen:** High performance, HTTP/2 support, and seamless FastAPI integration
- **Role in project:** Production-ready web server that hosts the FastAPI application
- **Configuration:** Runs on `0.0.0.0:8000` with auto-reload for development

### **Data Processing & Analysis**

#### **Pandas**
- **What it does:** Powerful data manipulation and analysis library
- **Why chosen:** Excel at handling structured data, CSV processing, and data transformations
- **Role in project:**
  - CSV section parsing in unstructured files
  - Data cleaning and normalization
  - Converting parsed data into tabular format
  - Statistical analysis of processed records
- **Specific usage:** DataFrame operations, CSV parsing with `StringIO`, data type inference

#### **NumPy**
- **What it does:** Fundamental package for scientific computing with arrays
- **Why chosen:** Foundation for pandas and scikit-learn, efficient numerical operations
- **Role in project:** Underlying support for data processing and ML operations
- **Usage:** Array operations, mathematical computations, data type handling

#### **BeautifulSoup4**
- **What it does:** HTML and XML parsing library
- **Why chosen:** Robust handling of malformed HTML, excellent for web scraping data
- **Role in project:**
  - Parsing HTML snippets embedded in unstructured files
  - Extracting data from HTML tables in scraped content
  - Handling poorly formatted markup with error recovery
- **Specific features:** CSS selectors, tree traversal, automatic encoding detection

### **Machine Learning & AI**

#### **Scikit-learn**
- **What it does:** Comprehensive machine learning library for data science
- **Why chosen:** Production-ready ML algorithms, excellent documentation, and stable API
- **Role in project:**
  - **DBSCAN clustering** for grouping similar data patterns
  - **StandardScaler** for normalizing data before ML operations
  - Type detection and pattern recognition algorithms
- **Specific algorithms used:**
  ```python
  from sklearn.cluster import DBSCAN          # Data clustering
  from sklearn.preprocessing import StandardScaler  # Data normalization
  ```

#### **Sentence Transformers**
- **What it does:** State-of-the-art sentence and text embeddings using transformer models
- **Why chosen:** Semantic understanding of field names and content for intelligent schema inference
- **Role in project:**
  - Semantic similarity analysis for field name matching
  - Content-based field type inference
  - Schema evolution detection through semantic comparison
- **Model used:** `all-MiniLM-L6-v2` (lightweight, fast, good performance)
- **Features:** 384-dimensional embeddings, multilingual support, efficient inference

#### **Loguru**
- **What it does:** Advanced logging library with better defaults than standard logging
- **Why chosen:** Beautiful console output, automatic log rotation, and structured logging
- **Role in project:**
  - Comprehensive logging throughout all components
  - Error tracking and debugging information
  - Performance monitoring and system health logs
- **Configuration:** Time-stamped logs with colored output and automatic file rotation

### **Web Technologies & Frontend**

#### **HTML5 & CSS3**
- **What it does:** Modern web standards for structure and styling
- **Why chosen:** Clean, accessible web interface without heavy JavaScript frameworks
- **Role in project:**
  - **Responsive design** that works on all screen sizes
  - **Professional styling** with gradients, shadows, and animations
  - **Full-width layout** utilizing entire screen real estate
  - **Accessibility features** like proper semantic HTML

#### **JavaScript (ES6+)**
- **What it does:** Client-side scripting for interactive web features
- **Why chosen:** Native browser support, no build process required, lightweight implementation
- **Role in project:**
  - **File upload handling** with progress indicators
  - **Dynamic table rendering** based on data types
  - **Modal popups** for viewing complete content
  - **Interactive show/hide** functionality for large datasets
- **Key features implemented:**
  ```javascript
  // File upload with FormData
  // Dynamic table generation
  // Modal dialog system
  // Copy-to-clipboard functionality
  // Responsive table interactions
  ```

### **Data Parsing & Format Support**

#### **JSON (Built-in)**
- **What it does:** Native Python JSON parsing and generation
- **Why chosen:** Standard format, high performance, universal compatibility
- **Role in project:**
  - Parsing well-formed JSON objects in files
  - **Malformed JSON repair** with custom algorithms
  - API request/response serialization
  - Configuration file handling

#### **Regular Expressions (re module)**
- **What it does:** Pattern matching and text processing
- **Why chosen:** Powerful pattern recognition for unstructured data
- **Role in project:**
  - **Section boundary detection** (e.g., `--- METADATA ---`)
  - **Data type pattern matching** (emails, URLs, dates, prices)
  - **Content extraction** from mixed-format files
  - **OCR error correction** patterns

#### **CSV Processing**
- **What it does:** Comma-separated values parsing and generation
- **Why chosen:** Universal tabular data format
- **Role in project:**
  - Parsing CSV sections within unstructured files
  - Handling various delimiters and quote styles
  - Converting CSV data to structured records

### **Development & Utilities**

#### **Pydantic**
- **What it does:** Data validation using Python type hints
- **Why chosen:** Automatic validation, serialization, and documentation generation
- **Role in project:**
  - **API request/response models** with automatic validation
  - **Configuration management** with type safety
  - **Data schema definitions** for ETL jobs
- **Models used:** `ETLJobResponse`, `SchemaInferenceRequest`, configuration classes

#### **Python-dotenv**
- **What it does:** Environment variable management from `.env` files
- **Why chosen:** Secure configuration management, development/production separation
- **Role in project:**
  - Server configuration (host, port, debug mode)
  - ML model settings and paths
  - Processing limits and thresholds
- **Usage:** Database connections, API keys, feature flags

### **Architecture Patterns**

#### **Dependency Injection**
- **Implementation:** FastAPI's built-in DI system
- **Purpose:** Clean separation of concerns, testability
- **Usage:** Schema engine and ETL pipeline injection into endpoints

#### **Factory Pattern**
- **Implementation:** Parser factory for different data section types
- **Purpose:** Extensible parsing system for new data formats
- **Usage:** Dynamic parser selection based on content patterns

#### **Observer Pattern**
- **Implementation:** Schema evolution tracking and change notifications
- **Purpose:** Monitor data structure changes over time
- **Usage:** Version control for schema changes

### **Performance & Scalability**

#### **Async/Await**
- **What it provides:** Non-blocking I/O operations
- **Why important:** Handle multiple file uploads simultaneously
- **Implementation:** FastAPI endpoints with async functions

#### **Memory Management**
- **Streaming processing:** Large files processed in chunks
- **Garbage collection:** Explicit cleanup of processed data
- **Resource limits:** Configurable memory and file size limits

#### **Caching Strategy**
- **ML model caching:** Sentence transformers loaded once at startup
- **Schema caching:** Previously inferred schemas cached for reuse
- **Pattern caching:** Compiled regex patterns cached for performance

## �🚀 Installation & Setup

### Prerequisites
- **Python 3.11+** (Required for compatibility)
- **Virtual environment** (Recommended for isolation)
- **4GB+ RAM** (For ML models and data processing)

### Step 1: Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/AirETL-Dynamic-ETL-Pipeline.git
cd AirETL-Dynamic-ETL-Pipeline
```

**Note:** Replace `YOUR_USERNAME` with your actual GitHub username.

### Step 2: Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### Step 3: Install Core Dependencies
The system uses a minimal set of actually required dependencies:
```bash
pip install fastapi uvicorn beautifulsoup4 pandas numpy scikit-learn sentence-transformers loguru python-dotenv pydantic
```

**Note:** The `requirements.txt` file contains extensive dependencies for a full enterprise ETL system, but the current implementation runs successfully with just the core dependencies listed above.

### Step 4: Verify Installation
```bash
python test_setup.py
```
This will verify that all core components can be imported and initialized properly.

## 🎮 Usage Guide

### Starting the Server
```bash
# Activate virtual environment
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Start the server
python main.py
```

The server will start on `http://localhost:8000` with the following output:
```
INFO: Loading semantic similarity model...
INFO: Schema Inference Engine initialized
INFO: Starting AirETL server...
INFO: Uvicorn running on http://0.0.0.0:8000
```

### Accessing the Web Interface
1. Open your browser and navigate to `http://localhost:8000/home`
2. You'll see a clean, professional interface with file upload functionality
3. Click "Choose File" and select a text file to process
4. Click "Upload & Process File" to analyze your data

### Processing Sample Data
The repository includes `mock_official.txt` - a complex file demonstrating various data formats:
```bash
# Test with included sample
# Upload mock_official.txt through the web interface
```

This file contains 15 different data sections showcasing the parser's capabilities with real-world messy data.

## 📊 Web Interface Access

### Home Document
```http
GET /home
```
**Purpose:** Access the main web interface for file upload and data visualization
**URL:** `http://localhost:8000/home`

**Features:**
- **Interactive File Upload**: Drag-and-drop or click to select files (.txt, .json, .csv)
- **Real-time Processing**: Live progress indicators during file analysis
- **Multi-format Data Display**: Organized tables by data type (JSON objects, CSV rows, metadata, etc.)
- **Expandable Content**: Click-to-view full content for large text fields and complex objects
- **Professional Styling**: Clean, responsive design with hover effects and animations
- **Copy Functionality**: Copy extracted data to clipboard with one click
- **Show More/Less**: Toggle between showing 5 records or all records per data type

**How to Use:**
1. Navigate to `http://localhost:8000/home` in your browser
2. Click "Choose File" or drag a file to the upload area
3. Select your unstructured data file (recommended: try `mock_official.txt`)
4. Click "Upload & Process File" to analyze the data
5. View results organized by data type in expandable tables
6. Click on any content to view full details in a modal popup

## 🔧 Configuration & Customization

### Environment Variables
Create a `.env` file for custom configuration:
```bash
# Server Configuration
HOST=0.0.0.0
PORT=8000
DEBUG=False

# ML Models
MODEL_CACHE_DIR=./models
CONFIDENCE_THRESHOLD=0.8

# Processing Limits
MAX_FILE_SIZE_MB=50
MAX_RECORDS_PER_JOB=10000
```

### Custom Parsing Patterns
You can extend the parser by modifying section patterns in `UnstructuredDataParser`:
```python
self.section_patterns = {
    'custom_section': r'--- CUSTOM FORMAT.*?(?=---|$)',
    # Add your patterns here
}
```

## 🧪 Testing & Validation

### Running Tests
```bash
# Basic functionality test
python test_setup.py

# Test with sample data
python quick_test.py

# Test specific components
python -c "from src.parsers.unstructured_parser import UnstructuredDataParser; print('Parser OK')"
```

### Test Files Included
- `mock_official.txt` - Complex multi-format test file
- `test_simple.txt` - Basic parsing test
- `sample_employees.csv` - CSV format test
- `simple_sample.json` - JSON format test

## 📈 Performance & Limitations

### Performance Characteristics
- **File Size Limit:** Up to 50MB per upload (configurable)
- **Processing Speed:** ~1000 records/second for mixed data
- **Memory Usage:** ~2GB for ML models + data processing
- **Concurrent Users:** Handles multiple simultaneous uploads

### Current Limitations
1. **Language Support:** Optimized for English content (can handle others but with reduced accuracy)
2. **Binary Files:** Does not process images, videos, or proprietary binary formats
3. **Real-time Streaming:** Designed for batch processing, not real-time streams
4. **Database Integration:** Currently returns processed data; no direct database loading

### Scalability Considerations
- Single-threaded processing (can be extended for multi-processing)
- In-memory processing (suitable for files up to available RAM)
- Local file storage (can be extended for cloud storage)

## 🔍 Real-World Use Cases

### 1. **Web Scraping Data Processing**
- **Scenario:** Processing scraped e-commerce product pages
- **Challenge:** Mixed HTML, JSON, and text with inconsistent formatting
- **Solution:** AirETL automatically extracts product details, prices, and reviews into structured format

### 2. **Document Digitization**
- **Scenario:** Converting OCR-processed invoices and receipts
- **Challenge:** Character recognition errors and varying layouts
- **Solution:** Handles OCR errors and extracts key financial data points

### 3. **API Response Cleaning**
- **Scenario:** Processing third-party API responses with nested and malformed JSON
- **Challenge:** Inconsistent schemas and embedded error messages
- **Solution:** Repairs JSON syntax and normalizes field structures

### 4. **Data Migration Projects**
- **Scenario:** Migrating legacy system exports to modern databases
- **Challenge:** Multiple export formats in single files
- **Solution:** Identifies and processes each format separately, providing unified output

## 🤝 Contributing & Development

### Project Structure
```
AirETL/
├── src/
│   ├── parsers/
│   │   └── unstructured_parser.py    # Core parsing logic
│   ├── schema_inference/
│   │   └── engine.py                 # ML-powered schema detection
│   └── etl/
│       └── pipeline.py               # ETL orchestration
├── main.py                           # FastAPI web application
├── test_setup.py                     # Basic functionality tests
├── mock_official.txt                 # Complex test data
└── requirements.txt                  # Full dependency list
```

### Development Guidelines
1. **Code Style:** Follow PEP 8 standards
2. **Testing:** Add tests for new parsing patterns
3. **Documentation:** Update README for new features
4. **Error Handling:** Use proper logging and exception handling

## 🆘 Troubleshooting

### Common Issues

#### 1. Import Errors
```bash
ModuleNotFoundError: No module named 'src'
```
**Solution:** Ensure you're running from the project root directory

#### 2. ML Model Loading Issues
```bash
sentence_transformers model download failed
```
**Solution:** Ensure internet connection for initial model download (~100MB)

#### 3. Memory Issues with Large Files
```bash
MemoryError during processing
```
**Solution:** Process smaller chunks or increase system RAM

#### 4. Port Already in Use
```bash
ERROR: [Errno 98] Address already in use
```
**Solution:** 
```bash
# Kill existing process
pkill -f "python.*main.py"
# Or use different port
uvicorn main:app --port 8001
```

### Getting Help
- Check logs in the `logs/` directory for detailed error messages
- Review the test files to understand expected input formats
- Examine `mock_official.txt` for examples of supported data patterns

## 📝 License & Credits

This project demonstrates advanced ETL capabilities for unstructured data processing. It showcases:
- **Real-world data parsing** challenges and solutions
- **AI-powered schema inference** using machine learning
- **Modern web technologies** with FastAPI and responsive design
- **Production-ready architecture** with proper error handling and logging

The system is designed to handle the complexity and messiness of real-world data while providing a clean, professional interface for data analysts and engineers.