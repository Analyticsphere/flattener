import logging
import os
import sys
import tempfile
import uuid
from typing import Any

import duckdb
from fsspec import filesystem  # type: ignore
from google.cloud import storage  # type: ignore

import core.constants as constants

"""
Set up a logging instance that will write to stdout (and therefor show up in Google Cloud logs)
"""
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
# Create the logger at module level so its settings are applied throughout code base
logger = logging.getLogger(__name__)

def create_duckdb_connection() -> tuple[duckdb.DuckDBPyConnection, str]:
    # Creates a DuckDB instance with a local database
    # Returns tuple of DuckDB object, name of db file, and path to db file
    try:
        random_string = str(uuid.uuid4())
        
        # Cloud Run mounts /mnt/data, but local/dev environments generally do not.
        tmp_dir = "/mnt/data" if os.path.isdir("/mnt/data") else tempfile.gettempdir()
        local_db_file = os.path.join(tmp_dir, f"{random_string}.db")

        conn = duckdb.connect(local_db_file)
        conn.execute(f"SET temp_directory='{tmp_dir}'")
        conn.execute(f"SET memory_limit='{constants.DUCKDB_MEMORY_LIMIT}'")
        conn.execute(f"SET max_memory='{constants.DUCKDB_MEMORY_LIMIT}'")

        # Improves performance for large queries
        conn.execute("SET preserve_insertion_order='false'")

        # Set to number of CPU cores
        # https://duckdb.org/docs/configuration/overview.html#global-configuration-options
        conn.execute(f"SET threads={constants.DUCKDB_THREADS}")

        # Set max disk space to allow on GCS
        conn.execute(f"SET max_temp_directory_size='{constants.DUCKDB_MAX_SIZE}'")

        # Register GCS filesystem to read/write to GCS buckets when available.
        try:
            conn.register_filesystem(filesystem('gcs'))
        except ImportError:
            logger.warning("gcsfs is not available; skipping GCS filesystem registration")

        return conn, local_db_file
    except Exception as e:
        logger.error(f"Unable to create DuckDB instance: {e}")
        raise Exception(f"Unable to create DuckDB instance: {e}") from e

def close_duckdb_connection(conn: duckdb.DuckDBPyConnection, local_db_file: str) -> None:
    # Destory DuckDB object to free memory, and remove temporary files
    try:
        # Close the DuckDB connection
        conn.close()

        # Remove the local database file if it exists
        if os.path.exists(local_db_file):
            os.remove(local_db_file)

    except Exception as e:
        logger.error(f"Unable to close DuckDB connection: {e}")

def get_raw_parquet_file_location(destination_bucket: str, table_name: str) -> str:
    parquet_path = f"gs://{destination_bucket}/{table_name}/{table_name}_part*.parquet"
    return parquet_path

def get_converted_parquet_directory(destination_bucket: str, table_name: str) -> str:
    """Directory that holds pre-flatten converted Parquet files for tables that need them."""
    return f"gs://{destination_bucket}/{table_name}/{constants.CONVERTED_PARQUET_DIRECTORY_NAME}"

def get_converted_parquet_file_location(destination_bucket: str, table_name: str) -> str:
    """Wildcard path for converted Parquet part files."""
    parquet_path = (
        f"{get_converted_parquet_directory(destination_bucket, table_name)}/{table_name}_part*.parquet"
    )
    return parquet_path

def get_flattening_source_parquet_file_location(destination_bucket: str, table_name: str) -> str:
    """Return the Parquet location the flatten step should read from for a given table."""
    if table_name.lower() == constants.SPECIAL_LOGIC_TABLES.BOXES.value:
        return get_converted_parquet_file_location(destination_bucket, table_name)

    return get_raw_parquet_file_location(destination_bucket, table_name)

def get_flattened_parquet_file_location(destination_bucket: str, table_name: str) -> str:
    parquet_path = f"gs://{destination_bucket}/{table_name}/flattened/{table_name}.parquet"
    return parquet_path

def get_parquet_column_names(parquet_path: str) -> set[str]:
    return set(get_parquet_schema_map(parquet_path))

def get_parquet_schema_map(parquet_path: str) -> dict[str, str]:
    """Describe a Parquet file and return ``column_name -> DuckDB column_type``."""
    conn, local_db_file = create_duckdb_connection()

    try:
        with conn:
            schema = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}') LIMIT 0"
            ).fetchdf()
            return dict(zip(schema["column_name"], schema["column_type"]))
    except Exception as e:
        logger.error(f"Unable to describe incoming Parquet file: {e}")
        raise Exception(f"Unable to describe incoming Parquet file: {e}") from e
    finally:
        close_duckdb_connection(conn, local_db_file)

def escape_sql_value(val: Any) -> str:
    """Escape a dynamic value for safe interpolation into generated DuckDB SQL."""
    if val is None:
        return "NULL"

    return str(val).replace("\\", "\\\\").replace("'", "''").replace('"', '\\"')

def valid_parquet_file(gcs_file_path: str) -> bool:
    # Retuns bool indicating whether Parquet file is valid/can be read by DuckDB
    conn, local_db_file = create_duckdb_connection()

    try:
        with conn:
            # If the file is not a valid Parquet file, this will throw an exception
            conn.execute(f"DESCRIBE SELECT * FROM read_parquet('gs://{gcs_file_path}')")

            # If we get to this point, we were able to describe the Parquet file and will assume it's valid
            return True
    except Exception as e:
        logger.error(f"Unable to validate Parquet file: {e}")
        return False
    finally:
        close_duckdb_connection(conn, local_db_file)

def parquet_file_exists(file_path: str) -> bool:
    """
    Check if a Parquet file exists in Google Cloud Storage.
    """
    # Strip gs:// prefix if it exists
    gcs_path = file_path.replace('gs://', '')
    
    # Parse bucket and blob name
    path_parts = gcs_path.split('/')
    bucket_name = path_parts[0]
    blob_name = '/'.join(path_parts[1:])
    
    try:
        # Initialize storage client with default credentials
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        return blob.exists()
    except Exception as e:
        logger.error(f"Error checking Parquet file existence: {e}")
        return False
