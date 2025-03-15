from google.cloud import bigquery
import core.constants as constants
import core.utils as utils
from google.cloud import storage


def parse_gcs_path(gcs_path: str) -> tuple[str, str]:
    """
    Parse a GCS path in the format 'bucket/path/to/files' into bucket name and prefix.
    
    Args:
        gcs_path (str): GCS path in the format 'bucket/path/to/files'
        
    Returns:
        tuple: (bucket_name, path_prefix)
    """
    # Remove preceding gs://
    gcs_path = gcs_path.replace('gs://', '')

    parts = gcs_path.strip().split('/', 1)
    bucket_name = parts[0]
    path_prefix = parts[1] if len(parts) > 1 else ""
    
    return bucket_name, path_prefix

def delete_from_gcs_path(gcs_path: str) -> None:
    try:
        # Parse the GCS path
        bucket_name, path_prefix = parse_gcs_path(gcs_path)
        
        # Initialize the GCS client
        storage_client = storage.Client()
        
        # Get the bucket
        bucket = storage_client.bucket(bucket_name)
        
        # List all blobs with the specified prefix
        blobs = bucket.list_blobs(prefix=path_prefix)
        
        # Delete each blob
        deleted_count = 0
        for blob in blobs:
            print(f"Deleting: {blob.name}")
            blob.delete()
            deleted_count += 1
        
        print(f"Successfully deleted {deleted_count} files from gs://{bucket_name}/{path_prefix}")
    
    except Exception as e:
        print(f"Error deleting files: {str(e)}")

def export_table_to_parquet(project_id: str, dataset_id: str, table_id: str, destination_bucket: str):
    """
    Export a BigQuery table to Parquet file(s) in a GCS bucket.
    
    Args:
        project_id (str): Google Cloud project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID to be flattened
        destination_uri (str): GCS destination bucket (e.g., 'bucket-name/path/')
    """
    # Clear any exists files
    table_directory = f"{destination_bucket}/{table_id}"
    delete_from_gcs_path(table_directory)
    
    # Build path to save Parquet file(s)
    destination_uri = f'gs://{table_directory}/{table_id}_part*.parquet'

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Create a reference to the source table
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Configure the extract job
    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.PARQUET
    job_config.compression = constants.EXPORT_PARQUET_COMPRESSION
    
    # Start the export job
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config
    )
    
    # Wait for the job to complete
    extract_job.result()
    
    print(f"Export job completed: {extract_job.job_id}")
    print(f"Exported data to: {destination_uri}")