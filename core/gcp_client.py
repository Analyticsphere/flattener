import json
from typing import Optional

from google.cloud import bigquery, pubsub_v1, storage

import core.constants as constants
import core.utils as utils


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

def table_to_parquet(project_id: str, dataset_id: str, table_id: str, destination_bucket: str) -> None:
    """
    Export a BigQuery table to Parquet file(s) in a GCS bucket.
    
    Args:
        project_id (str): Google Cloud project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID to be flattened
        destination_uri (str): GCS destination bucket (e.g., 'bucket-name/path/')
    """
    # Clear any existing files
    table_directory = f"{destination_bucket}/{table_id}"
    delete_from_gcs_path(table_directory)
    
    # Build path to save Parquet file(s)
    destination_uri = utils.get_raw_parquet_file_location(destination_bucket, table_id)#f'gs://{table_directory}/{table_id}_part*.parquet'

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

def parquet_to_table(project_id: str, dataset_id: str, table_id: str, destination_bucket: str) -> None:
    parquet_file_path = utils.get_flattened_parquet_file_location(destination_bucket, table_id)

    if utils.parquet_file_exists(parquet_file_path):
        if utils.valid_parquet_file(parquet_file_path):
            # Initialize BigQuery client
            client = bigquery.Client(project=project_id)
            
            # Create a reference to the destination table
            table_ref = client.dataset(dataset_id).table(table_id)
            
            # Configure the load job
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.PARQUET
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            
            # Start the load job
            load_job = client.load_table_from_uri(
                parquet_file_path,
                table_ref,
                job_config=job_config
            )
            
            # Wait for the job to complete
            load_job.result()
            
            # Log success message
            utils.logger.info(f"Successfully loaded {parquet_file_path} to {project_id}.{dataset_id}.{table_id}")
        else:
            utils.logger.warning(f"Parquet file {parquet_file_path} exists but is not valid, did not load to BigQuery")
    else:
        utils.logger.warning(f"Parquet file {parquet_file_path} not found, did not load to BigQuery")
    
def publish_pubsub_message(project_id: str, topic: str, data: Optional[dict]) -> None:
    try:
        # Create a publisher client
        publisher = pubsub_v1.PublisherClient()

        # The topic path follows this format: projects/{project_id}/topics/{topic_id}
        topic_path = publisher.topic_path(project_id, topic)

        # Data must be a bytestring
        if data is None:
            data = {}
        
        # Convert the dictionary to a JSON string, then encode to bytes
        data_bytes = json.dumps(data).encode("utf-8")

        # Publish the message
        future = publisher.publish(topic_path, data_bytes)

        # Wait for Google to acknowledge receipt of the pubsub message 
        _ = future.result()

        utils.logger.info(f"Published PubSub message on topic {topic} to {project_id}")
    except Exception as e:
        raise Exception(f"Unable to publish PubSub message: {str(e)}")