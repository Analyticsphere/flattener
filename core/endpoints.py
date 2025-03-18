import os
import core.utils as utils
import core.constants as constants
import core.flatten as flatten
from datetime import datetime
from typing import Any, Optional
import core.gcp_client as gcp_client

from flask import Flask, jsonify, request # type: ignore

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat() -> tuple[Any, int]:
    utils.logger.info("API status check called")
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': constants.SERVICE_NAME
    }), 200

@app.route('/refresh_firestore', methods=['POST'])
def refresh_firestore_data() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    project_id: Optional[str] = data.get('project_id')
    topic: Optional[str] = data.get('topic')

    if not project_id or not topic:
        return "Missing required parameters: project_id, dataset_id", 400

    try:
        utils.logger.info(f"Backing up and refreshing Firestore data")
        gcp_client.publish_pubsub_message(project_id, topic, None)
        return f"Backed up and refreshed Firestore", 200
    except Exception as e:
        utils.logger.error(f"Unable to backup Firestore: {str(e)}")
        return f"Unable to backup Firestore: {str(e)}", 500

@app.route('/table_to_parquet', methods=['POST'])
def bq_to_parquet() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')
    table_id: Optional[str] = data.get('table_id')
    destination_bucket: Optional[str] = data.get('destination_bucket')

    if not project_id or not dataset_id or not table_id or not destination_bucket:
        return "Missing required parameters: project_id, dataset_id, table_id, destination_bucket", 400

    try:
        utils.logger.info(f"Extracting {table_id} to {destination_bucket}")
        gcp_client.table_to_parquet(project_id, dataset_id, table_id, destination_bucket)
        return f"Extracted {table_id} to Parquet", 200
    except Exception as e:
        utils.logger.error(f"Unable to extract BigQuery table {table_id} to Parquet: {str(e)}")
        return f"Unable to extract BigQuery table {table_id} to Parquet: {str(e)}", 500

@app.route('/flatten_parquet', methods=['POST'])
def flatten_parquet() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    destination_bucket: Optional[str] = data.get('destination_bucket')
    table_id: Optional[str] = data.get('table_id')
    
    if not table_id or not destination_bucket:
        return "Missing required parameters: table_id, destination_bucket", 400

    try:
        utils.logger.info(f"Flattening {table_id} Parquet files")

        flatten.flatten_table_file(destination_bucket, table_id)
        return f"Flattened {table_id} Parquet files", 200
    except Exception as e:
        utils.logger.error(f"Unable to flatten {table_id} Parquet files: {str(e)}")
        return f"Unable to flatten {table_id} Parquet files: {str(e)}", 500

@app.route('/parquet_to_table', methods=['POST'])
def parquet_to_bq() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')
    table_id: Optional[str] = data.get('table_id')
    destination_bucket: Optional[str] = data.get('destination_bucket')

    if not project_id or not dataset_id or not table_id or not destination_bucket:
        return "Missing required parameters: project_id, dataset_id, table_id, destination_bucket", 400

    try:
        utils.logger.info(f"Moving {table_id} Parquet file into BigQuery")
        gcp_client.parquet_to_table(project_id, dataset_id, table_id, destination_bucket)
        return f"Moved {table_id} Parquet file to BigQuery", 200
    except Exception as e:
        utils.logger.error(f"Unable to move {table_id} Parquet to BigQuery: {str(e)}")
        return f"Unable to move {table_id} Parquet to BigQuery: {str(e)}", 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)