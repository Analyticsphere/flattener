import os
import utils
import constants
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
        gcp_client.export_table_to_parquet(project_id, dataset_id, table_id, destination_bucket)
        return f"Extracted {table_id} to Parquet ", 200
    except Exception as e:
        utils.logger.error(f"Unable extract BigQuery table to Parquet: {str(e)}")
        return f"Unable extract BigQuery table to Parquet: {str(e)}", 500
    

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)