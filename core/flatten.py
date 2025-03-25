import re

import core.constants as constants
import core.utils as utils


def flatten_table_file(destination_bucket: str, table_name: str) -> None:
    # Generate a SQL statement that "flattens" a Parquet file and then execute it to create a new file
    source_parquet_path = utils.get_raw_parquet_file_location(destination_bucket, table_name)
    flattened_file_path = utils.get_flattened_parquet_file_location(destination_bucket, table_name)

    # Build the SELECT statement
    select_statement = create_flattening_select_statement(source_parquet_path)

    if select_statement:
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
    #struct_match = re.match(r'^STRUCT\((.*)\)$', schema_str.strip())
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

def create_flattening_select_statement(parquet_path: str) -> str:
    # Create a SQL SELECT statement that, when exected, "expands" a nested Parquet file
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


                    # DuckDB struggles to parse D_470862706
                    # The field is an array, and the second item in the array is a struct
                    # Without specifing the struct object in the array directly, DuckDB can't read the struct
                    D_470862706_field_path = f"{constants.SPECIAL_LOGIC_FIELDS.D_470862706.value}[1]"
                    if field_path[0] == constants.SPECIAL_LOGIC_FIELDS.D_470862706.value:
                        field_path[0] = D_470862706_field_path
                    #     sql_path = "CAST(" + '.'.join([f'{part}' for part in field_path]) + " AS STRING)"
                    # else:
                    #     # Build SQL path with proper quoting
                    #     sql_path = "CAST(" + '.'.join([f'"{part}"' for part in field_path]) + " AS STRING)"

                    # Skip if any part of the path should be ignored
                    if any(ignore in part for part in field_path for ignore in constants.IGNORE_FIELDS):
                        continue
                    
                    # Build SQL path with proper quoting
                    if field_path[0] == D_470862706_field_path:
                        sql_path = '.'.join([f'{part}' for part in field_path])
                    else:
                        sql_path = '.'.join([f'"{part}"' for part in field_path])

                    # Build alias by joining path parts with underscores
                    alias = '_'.join(field_path)

                    # Remove [] from alias
                    alias = alias.replace('[','',).replace(']','')

                    # Remove entity string from column alias
                    if '_entity_' in alias:
                        utils.logger.warning(f"entity field identifed in {sql_path} within file {parquet_path}")
                        alias = alias.replace('_entity', '')
                    
                    # Handle different field types
                    if field_type == 'VARCHAR[]' and \
                        constants.SPECIAL_LOGIC_FIELDS.d_110349197.value not in field_path and \
                        constants.SPECIAL_LOGIC_FIELDS.d_543608829.value not in field_path:
                        # d_110349197 and d_543608829 must be represented as list of values per analyst requirements
                        
                        # Query to get distinct values in the array used to build new columns
                        # Include the check for interger values so free-text survery responses don't get created as a column
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
                            select_expr = f"{sql_path} AS {alias}"
                            select_exprs.append(select_expr)
                    else:
                        # For non-array fields, include them as-is
                        select_expr = f"{sql_path} AS {alias}"
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

