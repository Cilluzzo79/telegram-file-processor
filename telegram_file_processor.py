#!/usr/bin/env python3
import os
import io
import logging
import requests
from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest
import PyPDF2
import pdfplumber
from openpyxl import load_workbook
import traceback
from datetime import datetime

# Configurazione logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configurazione
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '8369912973:AAFEQ2y-EXVPSM4PJL2iJkHnsF821GLGISk')
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL', 'https://mauro79.app.n8n.cloud/webhook/processed-data')
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB limit

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    try:
        data = request.get_json()
        if not data:
            raise BadRequest("No JSON data received")
        
        return jsonify({'status': 'received', 'message': 'Processing not yet implemented'})
        
    except Exception as e:
        logger.error(f"Errore webhook handler: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
