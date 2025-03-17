
import core.constants as constants
import core.utils as utils
import re

def build_source_parquet_file_location(destination_bucket: str, table_name: str) -> str:
    parquet_path = f"gs://{destination_bucket}/{table_name}/{table_name}_part*.parquet"
    return parquet_path


def find_matching_closing_paren(text, start_pos):
    """Find the matching closing parenthesis"""
    level = 1
    for i in range(start_pos + 1, len(text)):
        if text[i] == '(':
            level += 1
        elif text[i] == ')':
            level -= 1
            if level == 0:
                return i
    return -1


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
    struct_match = re.match(r'^STRUCT\((.*)\)$', schema_str.strip())
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

# Safely escape values for SQL
def escape_sql_value(val):
    if val is None:
        return "NULL"
    return str(val).replace("\\", "\\\\").replace("'", "''").replace('"', '\\"')


def create_flattening_select_statement(parque_path: str) -> str:
    # Create a SQL SELECT statement that, when exected, "expands" a nested Parquet file

    conn, local_db_file = utils.create_duckdb_connection()

    try:
        with conn:
            # Get schema of Parquet file
            # schema = conn.execute(f"""
            # DESCRIBE SELECT * FROM read_parquet('{parque_path}')
            # """).fetchdf()

            # Get schema of Parquet file
            # pandas must be installed, but doesn't need to be imported, for fetchdf() to work
            schema = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{parque_path}') LIMIT 0").fetchdf()#.fetchall()
            #utils.logger.warning(f"the schema is: {schema}")

            # Declare empty list to hold SELECT expressions
            select_exprs = []

            # Process each column in the schema
            for _, row in schema.iterrows():
                col_name = row['column_name']
                col_type = row['column_type']
                
                # Skip ignored columns
                if col_name in constants.IGNORE_FIELDS:
                    continue
                
                #utils.logger.warning(f"------------------------------------------------------------------------")
                #utils.logger.warning(f"Processing column: {col_name}")
                #utils.logger.warning(f"Type: {col_type}")
                
                # Extract all fields with their correct hierarchical paths
                fields = extract_struct_fields(col_type, [col_name])
                
                for field_type, field_path in fields:
                    # Skip if any part of the path should be ignored
                    if any(ignore in part for part in field_path for ignore in constants.IGNORE_FIELDS):
                        continue
                    
                    # Build SQL path with proper quoting
                    sql_path = '.'.join([f'"{part}"' for part in field_path])
                    
                    # Build alias by joining path parts with underscores
                    alias = '_'.join(field_path)
                    
                    # Handle different field types
                    if field_type == 'VARCHAR[]':
                        #utils.logger.warning(f"Processing VARCHAR[] field: {sql_path}")
                        
                        # Query to get distinct values in the array used to build new columns
                        distinct_vals_query = f"""
                            WITH vals AS (
                            SELECT DISTINCT UNNEST({sql_path}) AS val
                            FROM read_parquet('{parque_path}')
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
                                #utils.logger.warning(f"Adding array indicator: {new_col_name}")
                                select_exprs.append(expr)
                        except Exception as e:
                            #utils.logger.warning(f"Error processing array field {sql_path}: {e}")
                            # Fallback to including the array as-is
                            select_expr = f"{sql_path} AS {alias}"
                            #utils.logger.warning(f"Adding field as-is: {select_expr}")
                            select_exprs.append(select_expr)
                    else:
                        # For non-array fields, include them as-is
                        select_expr = f"{sql_path} AS {alias}"
                        #utils.logger.warning(f"Adding field: {select_expr}")
                        select_exprs.append(select_expr)
                
                utils.logger.warning(f"------------------------------------------------------------------------")

            # Generate final SQL query
            if select_exprs:
                final_query = f"""
    `                SELECT
                    {', '.join(select_exprs)}
                    FROM read_parquet('{parque_path}')`
                """

                utils.logger.warning(f"\n\nFinal query generated with {len(select_exprs)} fields\n\n")
                return final_query
    except Exception as e:
        utils.logger.error(f"Unable to process incoming Parquet file: {e}")
        raise Exception(f"Unable to process incoming Parquet file: {e}") from e
    finally:
        utils.close_duckdb_connection(conn, local_db_file)


def flatten_table(destination_bucket: str, project_id: str, dataset_id: str, table_name: str) -> None:
    utils.logger.warning(f"flattening parquets in table {destination_bucket}")
    source_parquet_path = build_source_parquet_file_location(destination_bucket, table_name)
    utils.logger.warning(f"looking for files ins {source_parquet_path}")
    select_statement = create_flattening_select_statement(source_parquet_path)
    utils.logger.warning(f"did run create_flattening_select_statement!!!")
    select_no_return = select_statement.replace('\n', ' ')
    utils.logger.warning(f"\n**** Final select statement is **** \n {select_no_return}")
