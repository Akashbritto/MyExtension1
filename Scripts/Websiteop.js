import csv
import re
import io
from datetime import datetime

def clean_csv_for_bigquery(input_data, 
                           columns_to_include=None, 
                           delimiter=',', 
                           quotechar='"', 
                           encoding='utf-8'):
    """
    Clean CSV data to make it compatible with BigQuery.
    
    Args:
        input_data: Can be a file path string or a file-like object (StringIO)
        columns_to_include: List of column names to include (None for all columns)
        delimiter: CSV delimiter character
        quotechar: CSV quote character
        encoding: File encoding
        
    Returns:
        StringIO object containing the cleaned CSV data
    """
    
    def clean_column_name(name):
        """Clean a single column name to be BigQuery-compatible"""
        # Preserve initial underscore if present
        starts_with_underscore = name.startswith('#')
        
        # Convert to lowercase
        new_name = name.lower()
        
        # If the column starts with '#', replace it with nothing
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
        
        # Sample rows
        rows = []
        for i, row in enumerate(reader):
            rows.append(row)
            if i >= sample_size:
                break
                
        for row in rows:
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
                
        return column_conversions, rows

    # Output buffer for the cleaned data
    output_buffer = io.StringIO()
    
    # Handle different input types
    if isinstance(input_data, str):
        # It's a file path
        try:
            file_obj = open(input_data, 'r', encoding=encoding, newline='')
        except UnicodeDecodeError:
            # If encoding fails, try with latin1
            file_obj = open(input_data, 'r', encoding='latin1', newline='')
    else:
        # Assume it's already a file-like object
        file_obj = input_data
    
    try:
        reader = csv.reader(file_obj, delimiter=delimiter, quotechar=quotechar)
        writer = csv.writer(output_buffer, quoting=csv.QUOTE_MINIMAL)
        
        # Read headers
        all_headers = next(reader)
        
        # Filter columns if columns_to_include is specified
        if columns_to_include:
            # Get indices of columns to include
            column_indices = [i for i, col in enumerate(all_headers) if col in columns_to_include]
            header = [all_headers[i] for i in column_indices]
        else:
            # Include all columns
            header = all_headers
            column_indices = list(range(len(header)))
        
        # Clean column names
        clean_header = [clean_column_name(col) for col in header]
        
        # Detect column types and retrieve sample rows
        column_conversions, sample_rows = detect_column_types(reader, header)
        
        # Write clean header
        writer.writerow(clean_header)
        
        # Process the sample rows
        seen_rows = set()  # For duplicate detection
        for row in sample_rows:
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
        
        # Process remaining rows
        for row in reader:
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
            
    finally:
        # Close the file if we opened it
        if isinstance(input_data, str):
            file_obj.close()
    
    # Reset buffer position to the beginning
    output_buffer.seek(0)
    return output_buffer


# Example usage:
# Method 1: From a file path
# cleaned_data = clean_csv_for_bigquery("input.csv")

# Method 2: From a string buffer
# from io import StringIO
# csv_string = "Name,Age,Date\nJohn,30,2021-01-01\nJane,25,2021-02-15"
# input_buffer = StringIO(csv_string)
# cleaned_data = clean_csv_for_bigquery(input_buffer)

# Method 3: With specific columns
# cleaned_data = clean_csv_for_bigquery("input.csv", columns_to_include=["Name", "Age"])
