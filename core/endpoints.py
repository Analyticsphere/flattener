import os
import utils
import constants
from datetime import datetime
from typing import Any

from flask import Flask, jsonify # type: ignore

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat() -> tuple[Any, int]:
    utils.logger.info("API status check called")
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': constants.SERVICE_NAME
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)