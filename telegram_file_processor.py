#!/usr/bin/env python3
"""
Telegram File Processor - App esterna per processing PDF e Excel
Riceve webhook da Telegram, processa file complessi, inoltra dati a N8N
"""

import os
import io
import logging
import requests
import pandas as pd
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

class FileProcessor:
    """Classe per gestire il processing di diversi tipi di file"""
    
    @staticmethod
    def download_telegram_file(file_id):
        """Scarica file da Telegram usando l'API"""
        try:
            # Ottieni info del file
            file_info_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile"
            response = requests.post(file_info_url, data={'file_id': file_id})
            response.raise_for_status()
            
            file_info = response.json()
            if not file_info['ok']:
                raise Exception(f"Errore Telegram API: {file_info.get('description', 'Unknown error')}")
            
            file_path = file_info['result']['file_path']
            file_size = file_info['result']['file_size']
            
            if file_size > MAX_FILE_SIZE:
                raise Exception(f"File troppo grande: {file_size} bytes (max {MAX_FILE_SIZE})")
            
            # Scarica il file
            download_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
            file_response = requests.get(download_url)
            file_response.raise_for_status()
            
            return file_response.content, file_info['result']
            
        except Exception as e:
            logger.error(f"Errore download file Telegram: {e}")
            raise
    
    @staticmethod
    def process_excel(file_content, filename):
        """Processa file Excel e estrae dati strutturati"""
        try:
            # Prova con pandas prima (più veloce)
            try:
                df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
                data = df.to_dict('records')
                headers = df.columns.tolist()
                return {
                    'success': True,
                    'data': data,
                    'headers': headers,
                    'record_count': len(data),
                    'sheets_processed': 1
                }
            except Exception as pd_error:
                logger.warning(f"Pandas fallito, provo openpyxl: {pd_error}")
                
                # Fallback con openpyxl per file complessi
                workbook = load_workbook(io.BytesIO(file_content), read_only=True)
                all_data = []
                all_headers = []
                
                for sheet_name in workbook.sheetnames:
                    sheet = workbook[sheet_name]
                    sheet_data = []
                    sheet_headers = []
                    
                    # Leggi headers dalla prima riga
                    first_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
                    if first_row:
                        sheet_headers = [str(cell) if cell is not None else f"Col_{i}" 
                                       for i, cell in enumerate(first_row)]
                    
                    # Leggi dati dalle righe successive
                    for row in sheet.iter_rows(min_row=2, values_only=True):
                        if any(cell is not None for cell in row):
                            row_dict = {}
                            for i, cell in enumerate(row):
                                if i < len(sheet_headers):
                                    row_dict[sheet_headers[i]] = str(cell) if cell is not None else ""
                            sheet_data.append(row_dict)
                    
                    all_data.extend(sheet_data)
                    if not all_headers:  # Usa headers del primo foglio
                        all_headers = sheet_headers
                
                return {
                    'success': True,
                    'data': all_data,
                    'headers': all_headers,
                    'record_count': len(all_data),
                    'sheets_processed': len(workbook.sheetnames)
                }
                
        except Exception as e:
            logger.error(f"Errore processing Excel: {e}")
            return {
                'success': False,
                'error': f"Errore processing Excel: {str(e)}",
                'file_name': filename
            }
    
    @staticmethod
    def process_pdf(file_content, filename):
        """Processa file PDF ed estrae testo/tabelle"""
        try:
            extracted_data = []
            
            # Prova con pdfplumber per tabelle
            try:
                with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                    all_text = ""
                    tables_found = 0
                    
                    for page_num, page in enumerate(pdf.pages):
                        # Estrai testo
                        page_text = page.extract_text()
                        if page_text:
                            all_text += f"\n--- Pagina {page_num + 1} ---\n{page_text}"
                        
                        # Cerca tabelle
                        tables = page.extract_tables()
                        for table_num, table in enumerate(tables):
                            if table and len(table) > 1:  # Almeno header + 1 riga dati
                                headers = [str(cell) if cell else f"Col_{i}" for i, cell in enumerate(table[0])]
                                
                                for row in table[1:]:
                                    if any(cell for cell in row):
                                        row_dict = {}
                                        for i, cell in enumerate(row):
                                            if i < len(headers):
                                                row_dict[headers[i]] = str(cell) if cell else ""
                                        extracted_data.append(row_dict)
                                tables_found += 1
                    
                    if extracted_data:
                        return {
                            'success': True,
                            'data': extracted_data,
                            'headers': list(extracted_data[0].keys()) if extracted_data else [],
                            'record_count': len(extracted_data),
                            'tables_found': tables_found,
                            'full_text': all_text[:5000]  # Primi 5000 caratteri
                        }
                    else:
                        # Solo testo, nessuna tabella
                        return {
                            'success': True,
                            'data': [{'content': all_text, 'source': 'pdf_text'}],
                            'headers': ['content', 'source'],
                            'record_count': 1,
                            'tables_found': 0,
                            'full_text': all_text[:5000]
                        }
                        
            except Exception as pdfplumber_error:
                logger.warning(f"PDFPlumber fallito, provo PyPDF2: {pdfplumber_error}")
                
                # Fallback con PyPDF2 per solo testo
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
                text_content = ""
                
                for page in pdf_reader.pages:
                    text_content += page.extract_text() + "\n"
                
                return {
                    'success': True,
                    'data': [{'content': text_content, 'source': 'pdf_text'}],
                    'headers': ['content', 'source'],
                    'record_count': 1,
                    'tables_found': 0,
                    'full_text': text_content[:5000]
                }
                
        except Exception as e:
            logger.error(f"Errore processing PDF: {e}")
            return {
                'success': False,
                'error': f"Errore processing PDF: {str(e)}",
                'file_name': filename
            }

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """Endpoint principale per ricevere webhook da Telegram"""
    try:
        data = request.get_json()
        if not data:
            raise BadRequest("No JSON data received")
        
        logger.info(f"Ricevuto webhook: {data}")
        
        # Verifica presenza messaggio e documento
        message = data.get('message', {})
        document = message.get('document')
        
        if not document:
            return jsonify({
                'status': 'ignored',
                'reason': 'No document in message'
            })
        
        file_id = document.get('file_id')
        filename = document.get('file_name', 'unknown')
        file_size = document.get('file_size', 0)
        mime_type = document.get('mime_type', '')
        
        # Determina se dobbiamo processare questo file
        file_extension = filename.split('.')[-1].lower() if '.' in filename else ''
        
        if file_extension not in ['xlsx', 'xls', 'pdf']:
            return jsonify({
                'status': 'ignored',
                'reason': f'File type {file_extension} not handled by this service'
            })
        
        logger.info(f"Processing file: {filename} ({file_extension}, {file_size} bytes)")
        
        # Scarica file da Telegram
        file_content, file_info = FileProcessor.download_telegram_file(file_id)
        
        # Processa file in base al tipo
        if file_extension in ['xlsx', 'xls']:
            result = FileProcessor.process_excel(file_content, filename)
        elif file_extension == 'pdf':
            result = FileProcessor.process_pdf(file_content, filename)
        else:
            raise Exception(f"Unsupported file type: {file_extension}")
        
        # Aggiungi metadati
        result['metadata'] = {
            'original_filename': filename,
            'file_size': file_size,
            'mime_type': mime_type,
            'processed_at': datetime.now().isoformat(),
            'user_id': message.get('from', {}).get('id'),
            'username': message.get('from', {}).get('username'),
            'chat_id': message.get('chat', {}).get('id')
        }
        
        # Inoltra risultato a N8N
        try:
            n8n_response = requests.post(
                N8N_WEBHOOK_URL,
                json=result,
                timeout=30
            )
            n8n_response.raise_for_status()
            logger.info(f"Dati inoltrati a N8N con successo")
        except Exception as n8n_error:
            logger.error(f"Errore inoltrando a N8N: {n8n_error}")
            # Non fail l'intero processo se N8N non risponde
        
        return jsonify({
            'status': 'processed',
            'filename': filename,
            'record_count': result.get('record_count', 0),
            'success': result.get('success', False)
        })
        
    except Exception as e:
        logger.error(f"Errore webhook handler: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/process-file', methods=['POST'])
def process_file_direct():
    """Endpoint alternativo per processing diretto (senza Telegram)"""
    try:
        data = request.get_json()
        file_url = data.get('file_url')
        file_type = data.get('file_type')
        
        if not file_url or not file_type:
            raise BadRequest("file_url and file_type required")
        
        # Scarica file dall'URL
        response = requests.get(file_url)
        response.raise_for_status()
        file_content = response.content
        
        # Processa in base al tipo
        if file_type in ['xlsx', 'xls']:
            result = FileProcessor.process_excel(file_content, f"file.{file_type}")
        elif file_type == 'pdf':
            result = FileProcessor.process_pdf(file_content, "file.pdf")
        else:
            raise Exception(f"Unsupported file type: {file_type}")
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Errore process-file: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    # Configurazione per sviluppo locale
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('DEBUG', 'False').lower() == 'true'
    
    app.run(host='0.0.0.0', port=port, debug=debug)
