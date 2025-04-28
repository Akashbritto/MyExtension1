import csv
import re
import os
from datetime import datetime

# ======= CONFIGURATION - MODIFY THESE VALUES =======
INPUT_FILE = "input.csv"  # Path to your input CSV file
OUTPUT_FILE = "output_bq_ready.csv"  # Path to save the cleaned CSV file
DELIMITER = ','  # CSV delimiter character
QUOTECHAR = '"'  # CSV quote character
ENCODING = 'utf-8'  # File encoding

# List of column names from the original CSV to include in the output
# Leave empty [] to include all columns
# Example: COLUMNS_TO_INCLUDE = ["Customer ID", "First Name", "Last Name", "Purchase Date"]
COLUMNS_TO_INCLUDE = []  # Fill this with the column names you want to keep
# ==================================================

def clean_column_name(name):
    """Clean a single column name to be BigQuery-compatible"""
    # Preserve initial underscore if present
    starts_with_underscore = name.startswith('#')
    
    # Convert to lowercase
    new_name = name.lower()
    
    # If the column starts with '#', replace it with nothing to preserve 'change' instead of '_change'
    if starts_with_underscore:
        new_name = new_name.replace('#', '', 1)
    
    # Replace other special characters with underscores
    new_name = re.sub(r'[^\w\s]', '_', new_name)
    
    # Replace multiple underscores with a single one
    new_name = re.sub(r'_+', '_', new_name)
    
    # Replace spaces with underscores
    new_name = new_name.replace(' ', '_')
    
    # Ensure names start with a letter or underscore
    if not re.match(r'^[a-zA-Z_]', new_name):
        new_name = f"col_{new_name}"
    
    # Remove trailing underscores
    new_name = new_name.rstrip('_')
    
    return new_name

def is_potential_date(value):
    """Check if a value might be a date"""
    date_patterns = [
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # MM/DD/YYYY or DD/MM/YYYY
        r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',     # YYYY/MM/DD
        r'\d{1,2}-[A-Za-z]{3}-\d{2,4}',     # DD-Mon-YYYY
    ]
    
    if not value or value.strip() == '':
        return False
        
    for pattern in date_patterns:
        if re.match(pattern, value):
            return True
    return False

def try_convert_date(value):
    """Try to convert a string to YYYY-MM-DD format"""
    if not value or value.strip() == '':
        return ''
        
    date_formats = [
        '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d',
        '%m-%d-%Y', '%d-%m-%Y', '%Y-%m-%d',
        '%d-%b-%Y', '%d-%B-%Y'
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    return value

def is_potential_boolean(value):
    """Check if a value might be boolean"""
    bool_values = {'true', 'false', 'yes', 'no', 't', 'f', 'y', 'n', '1', '0'}
    return value.lower() in bool_values if value else False

def convert_boolean(value):
    """Convert string to boolean representation"""
    if not value:
        return ''
    
    true_values = {'true', 'yes', 't', 'y', '1'}
    false_values = {'false', 'no', 'f', 'n', '0'}
    
    if value.lower() in true_values:
        return 'true'
    elif value.lower() in false_values:
        return 'false'
    return value

def clean_value(value):
    """Clean a value by removing control characters"""
    if not value:
        return ''
    # Remove control characters
    return re.sub(r'[\x00-\x1F\x7F]', '', value)

def detect_column_types(reader, header, sample_size=100):
    """Detect column types from a sample of rows"""
    column_types = {col: {'date': 0, 'boolean': 0, 'total': 0} for col in header}
    
    # Reset file position
    for i, row in enumerate(reader):
        if i >= sample_size:
            break
            
        for col_idx, col_name in enumerate(header):
            if col_idx < len(row) and row[col_idx]:
                column_types[col_name]['total'] += 1
                if is_potential_date(row[col_idx]):
                    column_types[col_name]['date'] += 1
                elif is_potential_boolean(row[col_idx]):
                    column_types[col_name]['boolean'] += 1
    
    # Determine likely types
    column_conversions = {}
    for col, counts in column_types.items():
        if counts['total'] == 0:
            continue
            
        date_ratio = counts['date'] / counts['total']
        bool_ratio = counts['boolean'] / counts['total']
        
        if date_ratio > 0.7:  # If more than 70% look like dates
            column_conversions[col] = 'date'
        elif bool_ratio > 0.7:  # If more than 70% look like booleans
            column_conversions[col] = 'boolean'
        else:
            column_conversions[col] = 'string'
            
    return column_conversions

def clean_csv_for_bigquery():
    """Clean a CSV file for BigQuery upload using predefined constants"""
    print(f"Reading CSV file: {INPUT_FILE}")
    
    # First pass: Read headers and detect column types
    try:
        with open(INPUT_FILE, 'r', encoding=ENCODING, newline='') as f:
            reader = csv.reader(f, delimiter=DELIMITER, quotechar=QUOTECHAR)
            all_headers = next(reader)
            
            # Filter columns if COLUMNS_TO_INCLUDE is specified
            if COLUMNS_TO_INCLUDE:
                # Check if all specified columns exist in the file
                missing_columns = [col for col in COLUMNS_TO_INCLUDE if col not in all_headers]
                if missing_columns:
                    print(f"Warning: The following specified columns were not found in the CSV: {missing_columns}")
                
                # Get indices of columns to include
                column_indices = [i for i, col in enumerate(all_headers) if col in COLUMNS_TO_INCLUDE]
                header = [all_headers[i] for i in column_indices]
            else:
                # Include all columns
                header = all_headers
                column_indices = list(range(len(header)))
            
            # Clean column names
            clean_header = [clean_column_name(col) for col in header]
            
            # Print info about selected columns
            print(f"Total columns in file: {len(all_headers)}")
            print(f"Columns selected for output: {len(header)}")
            print("Original headers and cleaned headers:")
            for i, (orig, cleaned) in enumerate(zip(header, clean_header)):
                print(f"  {i+1}. '{orig}' -> '{cleaned}'")
            
            # Detect column types from sample
            column_conversions = detect_column_types(reader, header)
    except UnicodeDecodeError:
        # If encoding fails, try with latin1
        print(f"Encoding {ENCODING} failed. Trying with 'latin1' encoding...")
        with open(INPUT_FILE, 'r', encoding='latin1', newline='') as f:
            reader = csv.reader(f, delimiter=DELIMITER, quotechar=QUOTECHAR)
            all_headers = next(reader)
            
            # Filter columns if COLUMNS_TO_INCLUDE is specified
            if COLUMNS_TO_INCLUDE:
                # Get indices of columns to include
                column_indices = [i for i, col in enumerate(all_headers) if col in COLUMNS_TO_INCLUDE]
                header = [all_headers[i] for i in column_indices]
            else:
                # Include all columns
                header = all_headers
                column_indices = list(range(len(header)))
            
            # Clean column names
            clean_header = [clean_column_name(col) for col in header]
            
            # Detect column types from sample
            column_conversions = detect_column_types(reader, header)
    
    # Second pass: Process the file
    rows_read = 0
    rows_written = 0
    seen_rows = set()  # For duplicate detection
    
    try:
        with open(INPUT_FILE, 'r', encoding=ENCODING, newline='') as infile, \
             open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.reader(infile, delimiter=DELIMITER, quotechar=QUOTECHAR)
            writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
            
            # Write clean header
            writer.writerow(clean_header)
            
            # Skip header in input file
            next(reader)
            
            # Process rows
            for row in reader:
                rows_read += 1
                
                # Skip rows that are too short
                if len(row) < max(column_indices, default=0) + 1:
                    continue
                
                # Extract and process only the columns we want
                filtered_row = [row[i] if i < len(row) else '' for i in column_indices]
                
                # Clean values
                clean_row = []
                for i, value in enumerate(filtered_row):
                    col_name = header[i]
                    clean_value_str = clean_value(value)
                    
                    # Skip completely empty values
                    if not clean_value_str or clean_value_str.strip() == '':
                        clean_row.append('')
                        continue
                        
                    # Apply type-specific conversions
                    col_type = column_conversions.get(col_name, 'string')
                    if col_type == 'date':
                        clean_row.append(try_convert_date(clean_value_str))
                    elif col_type == 'boolean':
                        clean_row.append(convert_boolean(clean_value_str))
                    else:
                        clean_row.append(clean_value_str)
                
                # Check for duplicates
                row_tuple = tuple(clean_row)
                if row_tuple in seen_rows:
                    continue
                
                seen_rows.add(row_tuple)
                writer.writerow(clean_row)
                rows_written += 1
                
                # Print progress periodically
                if rows_read % 10000 == 0:
                    print(f"Processed {rows_read} rows...")
                    
    except UnicodeDecodeError:
        print(f"Encoding issues detected. Trying with 'latin1' encoding...")
        with open(INPUT_FILE, 'r', encoding='latin1', newline='') as infile, \
             open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.reader(infile, delimiter=DELIMITER, quotechar=QUOTECHAR)
            writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
            
            # Write clean header
            writer.writerow(clean_header)
            
            # Skip header in input file
            next(reader)
            
            # Process rows (duplicate code from above - in production would refactor)
            for row in reader:
                rows_read += 1
                
                # Skip rows that are too short
                if len(row) < max(column_indices, default=0) + 1:
                    continue
                
                # Extract and process only the columns we want
                filtered_row = [row[i] if i < len(row) else '' for i in column_indices]
                
                # Clean values
                clean_row = []
                for i, value in enumerate(filtered_row):
                    col_name = header[i]
                    clean_value_str = clean_value(value)
                    
                    # Skip completely empty values
                    if not clean_value_str or clean_value_str.strip() == '':
                        clean_row.append('')
                        continue
                        
                    # Apply type-specific conversions
                    col_type = column_conversions.get(col_name, 'string')
                    if col_type == 'date':
                        clean_row.append(try_convert_date(clean_value_str))
                    elif col_type == 'boolean':
                        clean_row.append(convert_boolean(clean_value_str))
                    else:
                        clean_row.append(clean_value_str)
                
                # Check for duplicates
                row_tuple = tuple(clean_row)
                if row_tuple in seen_rows:
                    continue
                
                seen_rows.add(row_tuple)
                writer.writerow(clean_row)
                rows_written += 1
    
    print(f"Processing complete.")
    print(f"Total rows read: {rows_read}")
    print(f"Total rows written: {rows_written}")
    print(f"Duplicates removed: {rows_read - rows_written}")
    print(f"Cleaned CSV saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    clean_csv_for_bigquery()
