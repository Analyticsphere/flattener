# flattener
A Python utility for flattening complex nested data structures generated by the Connect for Cancer Prevention study questionnaires.

## Overview
The flattener tool converts Connect study data tables to Parquet files within Google Cloud Storage (GCS), builds and executes a DuckDB SQL script to process these files, and generates flattened Parquet files that are then loaded into BigQuery tables.

This tool is deployed as a Cloud Run function which accepts API requests to initiate the flattening process. The use of this package is orchastrated as an Airflow DAG, available in the flattener-orchestrator repository. 

## Features


1. Correctly traversing complex nested structures (STRUCTs within STRUCTs)
2. Expanding array fields into separate indicator columns
3. Maintaining proper hierarchical relationships between fields
4. Generating a flattened Parquet file with intuitive column naming


## Usage

_TODO_

## How It Works

### Struct Traversal

The tool obtains the schema of a Parquet file and then recursively explores it to identify all fields and their hierarchical relationships. For each field, it:

1. Builds the correct hierarchical path (e.g., `"parent"."child"."grandchild"`)
2. Creates an intuitive alias using underscores (e.g., `parent_child_grandchild`)
3. Adds the field to the SELECT expression with proper SQL syntax

### Array Field Expansion

For fields of type `VARCHAR[]`, the tool:

1. Identifies all unique values in the array across the entire dataset
2. For each unique value, creates a string binary indicator column (1 if present, 0 if not)
3. Names these columns using a pattern of `original_field_D_value`

For example, an array field containing values `["foo", "bar"]` would expand to:
- `original_field_D_foo = 1`
- `original_field_D_bar = 1`

### SQL Generation

The tool generates a SQL query that:
1. Selects all identified fields with proper paths and aliases
2. Reads from the input Parquet file
3. Outputs to a new Parquet file

## Column Naming Conventions

The tool follows these naming conventions:

- Regular nested fields: `parent_child_grandchild`
- Expanded array values: `parent_array_field_D_value`

## Example

### Input Schema (Simplified)

```
struct_column STRUCT(
  nested_struct STRUCT(
    string_field VARCHAR, 
    number_field BIGINT, 
    array_field VARCHAR[]
  )
)
```

### Output Columns

```
struct_column_nested_struct_string_field
struct_column_nested_struct_number_field
struct_column_nested_struct_array_field_D_value1
struct_column_nested_struct_array_field_D_value2
...
```
