import re

import core.constants as constants
import core.utils as utils


def debug_schema_structure(parquet_path: str) -> None:
    """
    Debug function to examine the schema structure of a Parquet file
    """
    conn, local_db_file = utils.create_duckdb_connection()
    
    try:
        with conn:
            # Get schema of Parquet file
            schema = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}') LIMIT 0").fetchdf()
            
            utils.logger.info("=== SCHEMA DEBUG INFO ===")
            for _, row in schema.iterrows():
                col_name = row['column_name']
                col_type = row['column_type']
                utils.logger.info(f"Column: {col_name}")
                utils.logger.info(f"Type: {col_type}")
                
                # Extract fields and log them
                try:
                    fields = extract_struct_fields(col_type, [col_name])
                    utils.logger.info(f"Extracted {len(fields)} fields:")
                    for field_type, field_path in fields[:10]:  # Limit to first 10 for readability
                        utils.logger.info(f"  - {field_path} -> {field_type}")
                    if len(fields) > 10:
                        utils.logger.info(f"  ... and {len(fields) - 10} more fields")
                except Exception as e:
                    utils.logger.error(f"Error extracting fields for {col_name}: {e}")
                
                utils.logger.info("---")
                
    except Exception as e:
        utils.logger.error(f"Unable to debug schema: {e}")
    finally:
        utils.close_duckdb_connection(conn, local_db_file)


def flatten_table_file(destination_bucket: str, table_name: str) -> None:
    # Generate a SQL statement that "flattens" a Parquet file and then execute it to create a new file
    source_parquet_path = utils.get_raw_parquet_file_location(destination_bucket, table_name)
    flattened_file_path = utils.get_flattened_parquet_file_location(destination_bucket, table_name)

    # Add debug logging for module3_v1
    if table_name == "module3_v1":
        utils.logger.info("Debugging module3_v1 schema structure...")
        debug_schema_structure(source_parquet_path)

    # Build the SELECT statement
    select_statement = create_flattening_select_statement(source_parquet_path)

    if select_statement:
        # Log the generated SQL for debugging
        if table_name == "module3_v1":
            utils.logger.info("Generated SQL for module3_v1:")
            utils.logger.info(select_statement[:2000] + "..." if len(select_statement) > 2000 else select_statement)

        final_query = f"""
        COPY (
            {select_statement}
        ) TO '{flattened_file_path}' {constants.DUCKDB_FORMAT_STRING};
        """

        conn, local_db_file = utils.create_duckdb_connection()

        try:
            with conn:
                conn.execute(final_query)
        except Exception as e:
            utils.logger.error(f"Unable to execute flattening SQL: {e}")
            # For module3_v1, also log the problematic SQL
            if table_name == "module3_v1":
                utils.logger.error("Full SQL that failed:")
                utils.logger.error(final_query)
            raise Exception(f"Unable to execute flattening SQL: {e}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)

def extract_struct_fields(schema_str, parent_path=[]):
    """
    Extract all fields from a struct schema string with their proper hierarchical paths
    
    Args:
        schema_str: The schema string to parse
        parent_path: Current path in the hierarchy
        
    Returns:
        List of tuples (field_type, complete_path) for all fields
    """
    result = []
    
    # Match STRUCT pattern
    struct_match = re.match(r'^STRUCT\((.*)\)', schema_str.strip())
    if not struct_match:
        # Not a struct, return the field type and path
        return [(schema_str.strip(), parent_path)]
    
    # Get the content inside STRUCT()
    struct_content = struct_match.group(1)
    
    # Function to split fields at the top level of a struct
    def split_top_level_fields(content):
        fields = []
        current = ""
        paren_level = 0
        
        for char in content:
            if char == '(':
                paren_level += 1
                current += char
            elif char == ')':
                paren_level -= 1
                current += char
            elif char == ',' and paren_level == 0:
                # Only split at commas at the top level
                fields.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            fields.append(current.strip())
        
        return fields
    
    # Split the struct content into individual fields
    fields = split_top_level_fields(struct_content)
    
    # Process each field
    for field in fields:
        # Match field name and type pattern
        field_match = re.match(r'([a-zA-Z0-9_"]+)\s+(.*)', field)
        if not field_match:
            continue
            
        field_name = field_match.group(1).strip('"')
        field_type = field_match.group(2).strip()
        
        # Skip ignored fields
        if any(ignore in field_name for ignore in constants.IGNORE_FIELDS):
            continue
        
        # Build current path
        current_path = parent_path + [field_name]
        
        if field_type.startswith('STRUCT'):
            # This is a nested struct, process recursively
            nested_fields = extract_struct_fields(field_type, current_path)
            result.extend(nested_fields)
        else:
            # This is a field
            result.append((field_type, current_path))
    
    return result

def escape_sql_value(val):
    # Safely escape values for SQL
    if val is None:
        return "NULL"
    return str(val).replace("\\", "\\\\").replace("'", "''").replace('"', '\\"')

def build_sql_path(field_path):
    """
    Build the SQL path for accessing struct fields, handling special cases properly.
    
    Args:
        field_path: List of path components
        
    Returns:
        Properly formatted SQL path string
    """
    if not field_path:
        return ""
    
    sql_parts = []
    
    for i, part in enumerate(field_path):
        # Handle special case for D_470862706 array access
        if part.startswith(constants.SPECIAL_LOGIC_FIELDS.D_470862706.value + "["):
            # This is the array access part, use as-is
            sql_parts.append(part)
        elif i == 0:
            # First part - quote if it doesn't contain array access
            if '[' in part and ']' in part:
                sql_parts.append(part)  # Already has array notation
            else:
                sql_parts.append(f'"{part}"')
        else:
            # Subsequent parts should always be quoted field names
            sql_parts.append(f'"{part}"')
    
    return '.'.join(sql_parts)

def create_flattening_select_statement(parquet_path: str) -> str:
    # Create a SQL SELECT statement that, when executed, "expands" a nested Parquet file
    # All fields must be of type STRING per analyst requirements

    conn, local_db_file = utils.create_duckdb_connection()

    try:
        with conn:
            # Get schema of Parquet file
            # pandas must be installed, but doesn't need to be imported, for fetchdf() to work
            schema = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}') LIMIT 0").fetchdf()

            # Declare empty list to hold SELECT expressions
            select_exprs = []

            # Process each column in the schema
            for _, row in schema.iterrows():
                col_name = row['column_name']
                col_type = row['column_type']
                
                # Skip ignored columns
                if col_name in constants.IGNORE_FIELDS:
                    continue
                                
                # Extract all fields with their correct hierarchical paths
                fields = extract_struct_fields(col_type, [col_name])
                
                for field_type, field_path in fields:

                    # Handle special case for D_470862706
                    # The field is an array, and the second item in the array is a struct
                    # if field_path[0] == constants.SPECIAL_LOGIC_FIELDS.D_470862706.value:
                    #     # Create a new path with array access notation
                    #     modified_path = field_path.copy()
                    #     modified_path[0] = f"{constants.SPECIAL_LOGIC_FIELDS.D_470862706.value}[1]"
                    #     field_path = modified_path

                    # Skip if any part of the path should be ignored
                    if any(ignore in part for part in field_path for ignore in constants.IGNORE_FIELDS):
                        continue
                    
                    # Build SQL path with proper handling for different cases
                    sql_path = build_sql_path(field_path)

                    # Build alias by joining path parts with underscores
                    alias = '_'.join(field_path)

                    # Remove [] from alias
                    alias = alias.replace('[','').replace(']','')

                    # Remove entity string from column alias
                    if '_entity_' in alias:
                        utils.logger.warning(f"entity field identified in {sql_path} within file {parquet_path}")
                        alias = alias.replace('_entity', '')
                    
                    # Debug logging for module3_v1
                    if "module3_v1" in parquet_path:
                        utils.logger.debug(f"Processing field: {field_path} -> {sql_path} (type: {field_type})")
                    
                    # Handle different field types
                    if field_type == 'VARCHAR[]' and constants.SPECIAL_LOGIC_FIELDS.d_110349197.value not in field_path and constants.SPECIAL_LOGIC_FIELDS.d_543608829.value not in field_path:
                        # d_110349197 and d_543608829 must be represented as list of values per analyst requirements

                        # Query to get distinct values in the array used to build new columns
                        # Include the check for integer values so free-text survey responses don't get created as a column
                        distinct_vals_query = f"""
                            WITH vals AS (
                            SELECT DISTINCT UNNEST({sql_path}) AS val
                            FROM read_parquet('{parquet_path}')
                            WHERE {sql_path} IS NOT NULL
                            )
                            SELECT * FROM vals WHERE TRY_CAST(val AS BIGINT) IS NOT NULL
                        """
            
                        try:
                            # Execute the query to get distinct values
                            distinct_vals = conn.execute(distinct_vals_query).fetchdf()['val'].tolist()
                            
                            # For each distinct value, create a binary indicator column
                            for val in distinct_vals:
                                # Create a safe column name
                                safe_val = re.sub(r'\W+', '_', str(val))
                                new_col_name = f"{alias}_D_{safe_val}"
                                
                                # Escape the value for SQL
                                escaped_val = escape_sql_value(val)
                                
                                # Create expression for binary indicator (1 if array contains value, 0 otherwise)
                                expr = f"CAST(IFNULL(CAST(array_contains({sql_path}, '{escaped_val}') AS INTEGER), 0) AS STRING) AS \"{new_col_name}\""
                                select_exprs.append(expr)
                        except Exception as e:
                            # Fallback to including the array as-is
                            utils.logger.warning(f"Could not expand array field {sql_path}: {e}")
                            select_expr = f"CAST({sql_path} AS STRING) AS \"{alias}\""
                            select_exprs.append(select_expr)
                    
                    else:
                        # For non-array fields, include them as-is
                        select_expr = f"CAST({sql_path} AS STRING) AS \"{alias}\""
                        select_exprs.append(select_expr)
                
            # Generate final SQL query
            if select_exprs:
                final_query = f"""
                    SELECT
                    {', '.join(select_exprs)}
                    FROM read_parquet('{parquet_path}')
                """

                return final_query
            
    except Exception as e:
        utils.logger.error(f"Unable to process incoming Parquet file: {e}")
        raise Exception(f"Unable to process incoming Parquet file: {e}") from e
    finally:
        utils.close_duckdb_connection(conn, local_db_file)