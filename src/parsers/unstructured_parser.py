"""
Unstructured Data Parser
Handles complex multi-format data parsing for scraped and mixed content
"""

import re
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

# Use standard logging instead of loguru for now
logger = logging.getLogger(__name__)

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


class UnstructuredDataParser:
    """Parser for complex unstructured data files with mixed formats"""
    
    def __init__(self):
        """Initialize the parser with various pattern matchers"""
        self.section_patterns = {
            'metadata': r'--- METADATA.*?(?=---|$)',
            'raw_paragraph': r'--- RAW PARAGRAPH.*?(?=---|$)',
            'inline_json': r'--- INLINE JSON.*?(?=---|$)',
            'malformed_json': r'--- MALFORMED JSON.*?(?=---|$)',
            'html_snippet': r'--- HTML SNIPPET.*?(?=---|$)',
            'csv_section': r'--- CSV-LIKE SECTION.*?(?=---|$)',
            'key_value': r'--- KEY-VALUE KVP BLOCK.*?(?=---|$)',
            'json_ld': r'--- JSON-LD.*?(?=---|$)',
            'inline_csv': r'--- INLINE CSV TABLE.*?(?=---|$)',
            'free_text': r'--- FREE TEXT.*?(?=---|$)',
            'ocr_footer': r'--- OCR-LIKE PAGE FOOTER.*?(?=---|$)',
            'sql_snippet': r'--- SQL-LIKE SNIPPET.*?(?=---|$)',
            'repeated_fields': r'--- REPEATED FIELDS.*?(?=---|$)',
            'ambiguous_types': r'--- AMBIGUOUS TYPES.*?(?=---|$)',
            'variant_v2': r'=== VARIANT v2 START ===.*?=== VARIANT v2 END ===',
        }
        
        self.json_patterns = {
            'json_object': r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',
            'json_array': r'\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]',
        }
        
        self.price_patterns = [
            r'\$?\d+\.?\d*\s*USD',
            r'\$\d+\.?\d*',
            r'\d+[.,]\d+',
            r'price[:\s]+[\$]?\d+\.?\d*'
        ]
        
        self.date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-\d{2}-\d{4}',
            r'\w+\s+\d{1,2},\s+\d{4}',
            r'\d{4}/\d{2}/\d{2}'
        ]

    def parse_file(self, content: str, filename: str = "unknown") -> List[Dict[str, Any]]:
        """
        Parse a complex unstructured file and extract structured data
        
        Args:
            content: File content as string
            filename: Original filename for context
            
        Returns:
            List of structured data records
        """
        logger.info(f"Parsing unstructured file: {filename}")
        
        # Detect and extract different sections
        sections = self._extract_sections(content)
        
        # Parse each section
        parsed_data = []
        
        for section_type, section_content in sections.items():
            try:
                section_data = self._parse_section(section_type, section_content)
                if section_data:
                    parsed_data.extend(section_data)
            except Exception as e:
                logger.warning(f"Failed to parse section {section_type}: {e}")
                # Add raw section as fallback
                parsed_data.append({
                    'section_type': section_type,
                    'raw_content': section_content[:500],  # Truncate long content
                    'parse_error': str(e),
                    'source_file': filename
                })
        
        # Add global metadata
        if parsed_data:
            metadata = self._extract_global_metadata(content, filename)
            for record in parsed_data:
                record.update(metadata)
        
        logger.info(f"Extracted {len(parsed_data)} structured records from {len(sections)} sections")
        return parsed_data

    def _extract_sections(self, content: str) -> Dict[str, str]:
        """Extract different sections from the content"""
        sections = {}
        
        for section_name, pattern in self.section_patterns.items():
            matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
            if matches:
                sections[section_name] = matches[0]
        
        return sections

    def _parse_section(self, section_type: str, content: str) -> List[Dict[str, Any]]:
        """Parse individual section based on its type"""
        
        if section_type == 'metadata':
            return self._parse_metadata(content)
        elif section_type == 'inline_json':
            return self._parse_json_section(content)
        elif section_type == 'malformed_json':
            return self._parse_malformed_json(content)
        elif section_type == 'html_snippet':
            return self._parse_html_section(content)
        elif section_type == 'csv_section' or section_type == 'inline_csv':
            return self._parse_csv_section(content)
        elif section_type == 'key_value':
            return self._parse_key_value(content)
        elif section_type == 'json_ld':
            return self._parse_json_ld(content)
        elif section_type == 'repeated_fields':
            return self._parse_repeated_fields(content)
        elif section_type == 'ambiguous_types':
            return self._parse_ambiguous_types(content)
        elif section_type == 'variant_v2':
            return self._parse_variant_section(content)
        elif section_type == 'raw_paragraph':
            return self._parse_raw_text(content)
        elif section_type == 'free_text':
            return self._parse_free_text(content)
        elif section_type == 'ocr_footer':
            return self._parse_ocr_footer(content)
        else:
            # Generic text parsing
            return self._parse_generic_text(content, section_type)

    def _parse_metadata(self, content: str) -> List[Dict[str, Any]]:
        """Parse metadata section with key: value pairs"""
        metadata = {'data_type': 'metadata'}
        
        lines = content.split('\n')
        for line in lines:
            if ':' in line and not line.strip().startswith('#') and not line.strip().startswith('---'):
                try:
                    key, value = line.split(':', 1)
                    metadata[key.strip()] = value.strip()
                except ValueError:
                    continue
        
        return [metadata] if len(metadata) > 1 else []

    def _parse_json_section(self, content: str) -> List[Dict[str, Any]]:
        """Parse JSON sections with fallback strategies"""
        results = []
        
        # Find JSON objects in content
        json_objects = re.findall(self.json_patterns['json_object'], content, re.DOTALL)
        
        for json_str in json_objects:
            try:
                parsed = json.loads(json_str)
                if isinstance(parsed, dict):
                    parsed['data_type'] = 'json_object'
                    parsed['was_malformed'] = False
                    results.append(parsed)
                elif isinstance(parsed, list):
                    for item in parsed:
                        if isinstance(item, dict):
                            item['data_type'] = 'json_object'
                            item['was_malformed'] = False
                            item['from_array'] = True
                            results.append(item)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON, attempting fixes: {e}")
                # Try to fix and parse as json_object
                parsed_obj = self._attempt_json_fixes(json_str)
                if parsed_obj:
                    parsed_obj['data_type'] = 'json_object'
                    parsed_obj['was_malformed'] = True
                    results.append(parsed_obj)
        
        return results

    def _parse_malformed_json(self, content: str) -> List[Dict[str, Any]]:
        """Parse malformed JSON by attempting multiple fix strategies"""
        results = []
        
        # Find potential JSON objects
        json_candidates = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
        
        for json_str in json_candidates:
            parsed_obj = self._attempt_json_fixes(json_str)
            if parsed_obj:
                parsed_obj['data_type'] = 'json_object'
                parsed_obj['was_malformed'] = True
                results.append(parsed_obj)
        
        return results

    def _attempt_json_fixes(self, json_str: str) -> Dict[str, Any]:
        """Attempt multiple strategies to fix malformed JSON"""
        
        # Strategy 1: Basic fixes
        try:
            fixed_json = self._fix_malformed_json(json_str)
            return json.loads(fixed_json)
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: More aggressive fixes
        try:
            aggressive_fix = self._aggressive_json_fix(json_str)
            return json.loads(aggressive_fix)
        except json.JSONDecodeError:
            pass
        
        # Strategy 3: Extract key-value pairs manually
        try:
            return self._extract_json_fields_manually(json_str)
        except:
            pass
        
        # If all strategies fail, create a structured representation
        return {
            'raw_content': json_str.strip(),
            'parsing_error': 'Could not parse as valid JSON',
            'extracted_text': self._extract_readable_content(json_str)
        }

    def _fix_malformed_json(self, json_str: str) -> str:
        """Attempt to fix common JSON formatting issues"""
        # Remove trailing commas
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        
        # Add missing commas between properties
        json_str = re.sub(r'"\s*\n\s*"', '",\n"', json_str)
        
        # Fix missing quotes around keys
        json_str = re.sub(r'(\w+):', r'"\1":', json_str)
        
        return json_str

    def _aggressive_json_fix(self, json_str: str) -> str:
        """More aggressive JSON fixing strategies"""
        # Remove comments
        json_str = re.sub(r'//.*?\n', '', json_str)
        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
        
        # Fix missing quotes around string values
        json_str = re.sub(r':\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*([,}])', r': "\1"\2', json_str)
        
        # Fix single quotes to double quotes
        json_str = json_str.replace("'", '"')
        
        # Remove trailing commas more aggressively
        json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
        
        # Fix missing commas after values
        json_str = re.sub(r'"\s*\n\s*"', '",\n"', json_str)
        json_str = re.sub(r'(\d)\s*\n\s*"', r'\1,\n"', json_str)
        json_str = re.sub(r'}\s*\n\s*"', r'},\n"', json_str)
        
        return json_str

    def _extract_json_fields_manually(self, json_str: str) -> Dict[str, Any]:
        """Manually extract fields from malformed JSON using regex"""
        result = {}
        
        # Extract string fields: "key": "value"
        string_matches = re.findall(r'["\']?(\w+)["\']?\s*:\s*["\']([^"\']*)["\']', json_str)
        for key, value in string_matches:
            result[key] = value
        
        # Extract numeric fields: "key": 123
        numeric_matches = re.findall(r'["\']?(\w+)["\']?\s*:\s*(\d+(?:\.\d+)?)', json_str)
        for key, value in numeric_matches:
            try:
                result[key] = int(value) if '.' not in value else float(value)
            except ValueError:
                result[key] = value
        
        # Extract boolean fields: "key": true/false
        bool_matches = re.findall(r'["\']?(\w+)["\']?\s*:\s*(true|false)', json_str, re.IGNORECASE)
        for key, value in bool_matches:
            result[key] = value.lower() == 'true'
        
        # Extract array fields: "key": [...]
        array_matches = re.findall(r'["\']?(\w+)["\']?\s*:\s*\[([^\]]*)\]', json_str)
        for key, array_content in array_matches:
            # Simple array parsing
            items = [item.strip().strip('"\'') for item in array_content.split(',')]
            result[key] = [item for item in items if item]
        
        return result if result else None

    def _extract_readable_content(self, json_str: str) -> str:
        """Extract readable content from unparseable JSON"""
        # Remove brackets and clean up
        content = json_str.strip('{}')
        # Remove excessive whitespace
        content = re.sub(r'\s+', ' ', content)
        # Extract quoted strings and values
        readable_parts = re.findall(r'["\']?(\w+)["\']?\s*:\s*["\']?([^,"\']*)["\']?', content)
        
        if readable_parts:
            return '; '.join([f"{key}: {value.strip()}" for key, value in readable_parts])
        return content[:100] + "..." if len(content) > 100 else content

    def _parse_html_section(self, content: str) -> List[Dict[str, Any]]:
        """Parse HTML sections and extract structured data"""
        results = []
        
        if not HAS_BS4:
            # Fallback: simple table extraction without BeautifulSoup
            return self._parse_html_simple(content)
        
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract tables
            tables = soup.find_all('table')
            for table in tables:
                table_data = self._extract_table_data(table)
                if table_data:
                    results.extend(table_data)
            
            # Extract other structured elements
            for script in soup.find_all('script'):
                if script.get('type') == 'application/ld+json':
                    try:
                        json_data = json.loads(script.string)
                        json_data['data_type'] = 'json_ld_from_html'
                        results.append(json_data)
                    except:
                        pass
            
        except Exception as e:
            logger.warning(f"HTML parsing failed: {e}")
            results.append({
                'data_type': 'raw_html',
                'content': content,
                'parse_error': str(e)
            })
        
        return results

    def _parse_html_simple(self, content: str) -> List[Dict[str, Any]]:
        """Simple HTML table parsing without BeautifulSoup"""
        results = []
        
        # Extract table rows using regex
        table_pattern = r'<tr[^>]*>(.*?)</tr>'
        rows = re.findall(table_pattern, content, re.DOTALL | re.IGNORECASE)
        
        if len(rows) >= 2:
            # Extract header
            header_cells = re.findall(r'<th[^>]*>(.*?)</th>', rows[0], re.IGNORECASE)
            if not header_cells:
                header_cells = re.findall(r'<td[^>]*>(.*?)</td>', rows[0], re.IGNORECASE)
            
            # Extract data rows
            for row in rows[1:]:
                data_cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.IGNORECASE)
                if len(data_cells) == len(header_cells):
                    row_data = {}
                    for i, cell in enumerate(data_cells):
                        # Clean HTML tags
                        clean_cell = re.sub(r'<[^>]+>', '', cell).strip()
                        row_data[header_cells[i].strip()] = clean_cell
                    row_data['data_type'] = 'html_table_row'
                    results.append(row_data)
        
        return results

    def _extract_table_data(self, table) -> List[Dict[str, Any]]:
        """Extract data from HTML table (requires BeautifulSoup)"""
        if not HAS_BS4:
            return []
        
        results = []
        
        try:
            # Get headers
            headers = []
            header_row = table.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
            
            # Get data rows
            rows = table.find_all('tr')[1:]  # Skip header row
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) == len(headers):
                    row_data = {}
                    for i, cell in enumerate(cells):
                        row_data[headers[i]] = cell.get_text(strip=True)
                    row_data['data_type'] = 'html_table_row'
                    results.append(row_data)
        
        except Exception as e:
            logger.warning(f"Table extraction failed: {e}")
        
        return results

    def _parse_csv_section(self, content: str) -> List[Dict[str, Any]]:
        """Parse CSV-like sections"""
        results = []
        
        lines = content.strip().split('\n')
        csv_lines = [line for line in lines if ',' in line and not line.strip().startswith('#') and not line.strip().startswith('---')]
        
        if len(csv_lines) >= 2:
            try:
                if HAS_PANDAS:
                    # Use pandas if available
                    csv_content = '\n'.join(csv_lines)
                    df = pd.read_csv(pd.io.common.StringIO(csv_content))
                    
                    for _, row in df.iterrows():
                        row_dict = row.to_dict()
                        row_dict['data_type'] = 'csv_row'
                        results.append(row_dict)
                else:
                    # Manual CSV parsing
                    headers = csv_lines[0].split(',')
                    headers = [h.strip() for h in headers]
                    
                    for line in csv_lines[1:]:
                        values = line.split(',')
                        values = [v.strip() for v in values]
                        
                        if len(values) == len(headers):
                            row_dict = {}
                            for i, value in enumerate(values):
                                row_dict[headers[i]] = value
                            row_dict['data_type'] = 'csv_row'
                            results.append(row_dict)
                    
            except Exception as e:
                logger.warning(f"CSV parsing failed: {e}")
                results.append({
                    'data_type': 'malformed_csv',
                    'raw_content': '\n'.join(csv_lines),
                    'error': str(e)
                })
        
        return results

    def _parse_key_value(self, content: str) -> List[Dict[str, Any]]:
        """Parse key-value pair sections"""
        result = {'data_type': 'key_value_pairs'}
        
        lines = content.split('\n')
        for line in lines:
            if ':' in line and not line.strip().startswith('#') and not line.strip().startswith('---'):
                try:
                    key, value = line.split(':', 1)
                    result[key.strip()] = value.strip()
                except ValueError:
                    continue
            elif ';' in line and not line.strip().startswith('#'):
                # Handle semicolon-separated values
                parts = line.split(';')
                for i, part in enumerate(parts):
                    result[f'tag_{i}'] = part.strip()
        
        return [result] if len(result) > 1 else []

    def _parse_json_ld(self, content: str) -> List[Dict[str, Any]]:
        """Parse JSON-LD sections"""
        results = []
        
        # Extract JSON from script tags
        json_ld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(json_ld_pattern, content, re.DOTALL)
        
        for match in matches:
            try:
                parsed = json.loads(match.strip())
                parsed['data_type'] = 'json_ld'
                results.append(parsed)
            except json.JSONDecodeError as e:
                results.append({
                    'data_type': 'malformed_json_ld',
                    'raw_content': match.strip(),
                    'error': str(e)
                })
        
        return results

    def _parse_repeated_fields(self, content: str) -> List[Dict[str, Any]]:
        """Parse sections with repeated field names"""
        fields = {}
        
        lines = content.split('\n')
        for line in lines:
            if ':' in line and not line.strip().startswith('#') and not line.strip().startswith('---'):
                try:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key in fields:
                        # Handle repeated fields
                        if not isinstance(fields[key], list):
                            fields[key] = [fields[key]]
                        fields[key].append(value)
                    else:
                        fields[key] = value
                except ValueError:
                    continue
        
        if fields:
            fields['data_type'] = 'repeated_fields'
            return [fields]
        return []

    def _parse_ambiguous_types(self, content: str) -> List[Dict[str, Any]]:
        """Parse sections with ambiguous data types"""
        result = {'data_type': 'ambiguous_types'}
        
        lines = content.split('\n')
        for line in lines:
            if ':' in line and not line.strip().startswith('#') and not line.strip().startswith('---'):
                try:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip().strip('"')
                    
                    # Try to infer actual type
                    if value.lower() in ['true', 'false']:
                        result[key] = value.lower() == 'true'
                        result[f'{key}_original'] = value
                    elif value == 'N/A':
                        result[key] = None
                        result[f'{key}_original'] = value
                    elif value.isdigit():
                        result[key] = int(value)
                        result[f'{key}_original'] = value
                    elif self._is_float(value):
                        result[key] = float(value)
                        result[f'{key}_original'] = value
                    else:
                        result[key] = value
                        
                except ValueError:
                    continue
        
        return [result] if len(result) > 1 else []

    def _parse_variant_section(self, content: str) -> List[Dict[str, Any]]:
        """Parse variant/evolution sections showing schema changes"""
        results = []
        
        # Extract JSON objects from variant section
        json_objects = re.findall(self.json_patterns['json_object'], content, re.DOTALL)
        for json_str in json_objects:
            try:
                parsed = json.loads(json_str)
                parsed['data_type'] = 'schema_variant'
                parsed['version'] = 'v2'
                results.append(parsed)
            except json.JSONDecodeError:
                pass
        
        # Extract YAML frontmatter if yaml is available
        if HAS_YAML:
            yaml_pattern = r'---\n(.*?)\n---'
            yaml_matches = re.findall(yaml_pattern, content, re.DOTALL)
            for yaml_str in yaml_matches:
                try:
                    parsed = yaml.safe_load(yaml_str)
                    if isinstance(parsed, dict):
                        parsed['data_type'] = 'yaml_frontmatter'
                        results.append(parsed)
                except yaml.YAMLError:
                    pass
        
        # Extract CSV from variant section
        csv_lines = []
        for line in content.split('\n'):
            if ',' in line and not line.strip().startswith('#'):
                csv_lines.append(line)
        
        if len(csv_lines) >= 2:
            try:
                if HAS_PANDAS:
                    csv_content = '\n'.join(csv_lines)
                    df = pd.read_csv(pd.io.common.StringIO(csv_content))
                    for _, row in df.iterrows():
                        row_dict = row.to_dict()
                        row_dict['data_type'] = 'variant_csv_row'
                        results.append(row_dict)
                else:
                    # Manual parsing fallback
                    headers = csv_lines[0].split(',')
                    for line in csv_lines[1:]:
                        values = line.split(',')
                        if len(values) == len(headers):
                            row_dict = {}
                            for i, value in enumerate(values):
                                row_dict[headers[i].strip()] = value.strip()
                            row_dict['data_type'] = 'variant_csv_row'
                            results.append(row_dict)
            except:
                pass
        
        return results

    def _parse_raw_text(self, content: str) -> List[Dict[str, Any]]:
        """Parse raw text and extract structured information"""
        result = {'data_type': 'raw_text_analysis'}
        
        # Extract prices
        prices = []
        for pattern in self.price_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            prices.extend(matches)
        if prices:
            result['extracted_prices'] = prices
        
        # Extract dates
        dates = []
        for pattern in self.date_patterns:
            matches = re.findall(pattern, content)
            dates.extend(matches)
        if dates:
            result['extracted_dates'] = dates
        
        # Extract quoted strings
        quoted_strings = re.findall(r'"([^"]*)"', content)
        if quoted_strings:
            result['quoted_strings'] = quoted_strings
        
        # Basic text statistics
        result['word_count'] = len(content.split())
        result['char_count'] = len(content)
        result['line_count'] = len(content.split('\n'))
        
        return [result]

    def _parse_free_text(self, content: str) -> List[Dict[str, Any]]:
        """Parse free text with embedded scripts and mixed content"""
        results = []
        
        # Extract JavaScript objects
        js_pattern = r'var\s+\w+\s*=\s*(\{[^}]+\})'
        js_matches = re.findall(js_pattern, content)
        for js_obj in js_matches:
            try:
                # Convert JS object to JSON (simple cases)
                json_str = js_obj.replace("'", '"')
                parsed = json.loads(json_str)
                parsed['data_type'] = 'javascript_object'
                results.append(parsed)
            except:
                results.append({
                    'data_type': 'malformed_js_object',
                    'raw_content': js_obj
                })
        
        # Extract contact information
        phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        phones = re.findall(phone_pattern, content)
        if phones:
            results.append({
                'data_type': 'contact_info',
                'phone_numbers': phones
            })
        
        # Extract promo codes
        promo_pattern = r'promo code:\s*([A-Z0-9]+)'
        promos = re.findall(promo_pattern, content, re.IGNORECASE)
        if promos:
            results.append({
                'data_type': 'promotional_info',
                'promo_codes': promos
            })
        
        return results

    def _parse_ocr_footer(self, content: str) -> List[Dict[str, Any]]:
        """Parse OCR-like footer content"""
        result = {'data_type': 'ocr_footer'}
        
        lines = content.strip().split('\n')
        for line in lines:
            line = line.strip()
            if 'page' in line.lower():
                result['page_info'] = line
            elif 'document title' in line.lower():
                result['document_title'] = line.split(':', 1)[1].strip() if ':' in line else line
            elif 'l0cation' in line.lower() or 'location' in line.lower():
                # Handle OCR errors
                result['location'] = line.split(':', 1)[1].strip() if ':' in line else line
                result['ocr_errors_detected'] = True
            elif 'total' in line.lower():
                result['total_info'] = line
        
        return [result] if len(result) > 1 else []

    def _parse_generic_text(self, content: str, section_type: str) -> List[Dict[str, Any]]:
        """Generic text parsing for unknown sections"""
        result = {
            'data_type': f'generic_{section_type}',
            'content': content.strip(),
            'word_count': len(content.split()),
            'char_count': len(content)
        }
        
        return [result]

    def _extract_global_metadata(self, content: str, filename: str) -> Dict[str, Any]:
        """Extract global metadata from the entire file"""
        metadata = {
            'source_file': filename,
            'processed_at': datetime.now().isoformat(),
            'file_size_chars': len(content),
            'total_lines': len(content.split('\n'))
        }
        
        # Extract scraped_at timestamp if present
        scraped_pattern = r'scraped_at:\s*([^\n]+)'
        scraped_match = re.search(scraped_pattern, content)
        if scraped_match:
            metadata['scraped_at'] = scraped_match.group(1).strip()
        
        # Extract source URL if present
        source_pattern = r'source:\s*(https?://[^\n\s]+)'
        source_match = re.search(source_pattern, content)
        if source_match:
            metadata['source_url'] = source_match.group(1).strip()
        
        return metadata

    def _is_float(self, value: str) -> bool:
        """Check if string represents a float"""
        try:
            float(value)
            return '.' in value
        except (ValueError, TypeError):
            return False
