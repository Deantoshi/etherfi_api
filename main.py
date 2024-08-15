from flask import Flask, send_from_directory, send_file, make_response, jsonify, url_for, Response, stream_with_context
from flask_cors import CORS
import os
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.auth import default
from google.oauth2 import service_account
import pandas as pd
import io
from io import BytesIO
import logging
import time
import zipfile
import datetime
import csv

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')



KEYWORDS = ['aurelius', 'metis', 'ironclad', 'optimism', 'arbitrum', 'aggregate']

app = Flask(__name__)
# cors = CORS(app, origins='*')
CORS(app, resources={r"/api/*": {"origins": "https://frontend-dot-internal-website-427620.uc.r.appspot.com"}})

# Initialize GCP storage client
credentials, project = default()
storage_client = storage.Client(credentials=credentials, project=project)

@app.route('/api/data')
def get_data():
    return {"message": "Hello from Flask!"}

# Serve React App
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    if path != "" and os.path.exists(app.static_folder + '/' + path):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/files')
def get_files():
    bucket_name = 'cooldowns2'
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    
    csv_files = [blob.name for blob in blobs 
                 if blob.name.endswith('.zip')
                 and any(keyword.lower() in blob.name.lower() for keyword in KEYWORDS)]

    return jsonify(csv_files)

@app.route('/api/file/<filename>')
def get_file_content(filename):
    bucket_name = 'cooldowns2'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    
    content = blob.download_as_string()
    df = pd.read_csv(io.BytesIO(content))
    return jsonify(df.to_dict(orient='records'))

# Function to get credentials
def get_credentials():
    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        # Running on App Engine
        return None  # Default credentials will be used
    else:
        # Running locally
        key_path = 'fast-web-419215-35d284e06546.json'  # Update this path
        return service_account.Credentials.from_service_account_file(key_path)

@app.route('/api/download/<filename>')
def download_file(filename):
    bucket_name = 'cooldowns2'
    credentials = get_credentials()
    storage_client = storage.Client(credentials=credentials)
    
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)

        if not blob.exists():
            logging.warning(f"File not found in bucket: {filename}")
            return jsonify({"error": f"File {filename} not found in bucket {bucket_name}"}), 404

        logging.info(f"Generating signed URL for file: {filename}")
        
        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            method="GET",
            service_account_email=credentials.service_account_email if credentials else None,
            access_token=credentials.token if credentials else None,
        )
        
        logging.info(f"Signed URL generated for: {filename}")
        
        return jsonify({
            "signedUrl": url,
            "filename": filename
        }), 200

    except Exception as e:
        logging.exception(f"Error generating signed URL for {filename}: {str(e)}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route('/api/files/<filename>/url')
def get_signed_url(filename):
    # Code to generate a signed URL for the specified file
    storage_client = storage.Client()
    bucket = storage_client.bucket('cooldowns2')
    blob = bucket.blob(filename)
    url = blob.generate_signed_url(expiration=900)  # 15 minutes
    return jsonify({"filename": filename, "signedUrl": url})

def extract_data_from_zip(zip_file):
    data = []
    with zipfile.ZipFile(zip_file, 'r') as z:
        for filename in z.namelist():
            if filename.endswith('.csv'):
                with z.open(filename) as f:
                    csv_reader = csv.DictReader(f.read().decode('utf-8').splitlines())
                    for row in csv_reader:
                        data.append({
                            'day': row['day'],
                            'total_revenue': float(row['total_revenue'])
                        })
    return data

@app.route('/api/data/<filename>', methods=['GET'])
def get_zip_data(filename):
    bucket_name = 'cooldowns2'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)

    # List all blobs in the bucket
    blobs = bucket.list_blobs()

    # Filter blobs that contain 'lend_revenue' and match the given filename
    matching_blobs = [blob for blob in blobs if 'lend_revenue' in blob.name.lower() and filename in blob.name]

    if not matching_blobs:
        return jsonify({'error': 'File not found'}), 404

    # Use the first matching blob
    blob = matching_blobs[0]

    # Download the content of the zip file
    zip_content = blob.download_as_bytes()

    data = []
    with zipfile.ZipFile(BytesIO(zip_content)) as z:
        for zip_filename in z.namelist():
            if zip_filename.endswith('.csv'):
                with z.open(zip_filename) as f:
                    csv_reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8'))
                    for row in csv_reader:
                        data.append({
                            'day': row['day'],
                            'total_revenue': float(row['total_revenue'])
                        })

    return jsonify(data)

def format_currency(amount):
    return "${:,.2f}".format(float(amount))

@app.route('/api/all_revenue_data', methods=['GET'])
def get_all_revenue_data():
    bucket_name = 'cooldowns2'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)

    # List all blobs in the bucket
    blobs = bucket.list_blobs()

    # Filter blobs that contain 'lend_revenue'
    lend_revenue_blobs = [blob for blob in blobs if '_lend_revenue' in blob.name.lower()]
    
    if not lend_revenue_blobs:
        return jsonify({'error': 'File not found'}), 404

    all_data = {}

    for blob in lend_revenue_blobs:
        # Download the content of the file
        content = blob.download_as_bytes()

        data = []
        try:
            # Try to open as a zip file
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                for zip_filename in z.namelist():
                    if zip_filename.endswith('.csv'):
                        with z.open(zip_filename) as f:
                            csv_reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8'))
                            for row in csv_reader:
                                data.append({
                                    'day': row['day'],
                                    'total_revenue': format_currency(float(row['total_revenue'])),
                                    '7_days_ma_revenue': row['7_days_ma_revenue'],
                                    '30_days_ma_revenue': row['30_days_ma_revenue'],
                                    '90_days_ma_revenue': row['90_days_ma_revenue'],
                                    '180_days_ma_revenue': row['180_days_ma_revenue']
                                })
        except zipfile.BadZipFile:
            # If it's not a zip file, assume it's a CSV
            csv_reader = csv.DictReader(io.StringIO(content.decode('utf-8')))
            for row in csv_reader:
                data.append({
                    'day': row['day'],
                    'total_revenue': format_currency(float(row['total_revenue'])),
                    '7_days_ma_revenue': row['7_days_ma_revenue'],
                    '30_days_ma_revenue': row['30_days_ma_revenue'],
                    '90_days_ma_revenue': row['90_days_ma_revenue'],
                    '180_days_ma_revenue': row['180_days_ma_revenue']
                })
        
        all_data[blob.name] = data

    return jsonify(all_data)

# # gets the data for our token_revenue pie chart
@app.route('/api/token_revenue_data', methods=['GET'])
def get_token_revenue_data():
    bucket_name = 'cooldowns2'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)

    # List all blobs in the bucket
    blobs = bucket.list_blobs()

    # Filter blobs that contain 'token_revenue'
    token_revenue_blobs = [blob for blob in blobs if 'revenue_per_token' in blob.name.lower()]
    
    if not token_revenue_blobs:
        return jsonify({'error': 'File not found'}), 404

    all_data = {}

    for blob in token_revenue_blobs:
        # Download the content of the file
        content = blob.download_as_bytes()

        data = []
        try:
            # Try to open as a zip file
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                for zip_filename in z.namelist():
                    if zip_filename.endswith('.csv'):
                        with z.open(zip_filename) as f:
                            csv_reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8'))
                            for row in csv_reader:
                                data.append({
                                    'token_name': row['token_name'],
                                    'token_revenue': float(row['token_revenue'])
                                })
        except zipfile.BadZipFile:
            # If it's not a zip file, assume it's a CSV
            csv_reader = csv.DictReader(io.StringIO(content.decode('utf-8')))
            for row in csv_reader:
                data.append({
                    'token_name': row['token_name'],
                    'token_revenue': float(row['token_revenue'])
                })
        
        all_data[blob.name] = data

    return jsonify(all_data)


@app.route('/api/revenue_card_data', methods=['GET'])
def get_revenue_card_data():
    bucket_name = 'cooldowns2'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)

    # List all blobs in the bucket
    blobs = bucket.list_blobs()

    # Filter blobs that contain your specific file name
    revenue_blobs = [blob for blob in blobs if 'lend_revenue_data_card' in blob.name.lower()]
    
    if not revenue_blobs:
        return jsonify({'error': 'File not found'}), 404

    all_data = {}

    for blob in revenue_blobs:
        # Download the content of the file
        content = blob.download_as_bytes()

        try:
            # Try to open as a zip file
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                for zip_filename in z.namelist():
                    if zip_filename.endswith('.csv'):
                        with z.open(zip_filename) as f:
                            csv_reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8'))
                            all_data = list(csv_reader)
        except zipfile.BadZipFile:
            # If it's not a zip file, assume it's a CSV
            csv_reader = csv.DictReader(io.StringIO(content.decode('utf-8')))
            all_data = list(csv_reader)

    return jsonify(all_data)

@app.route('/api/deployment_revenue', methods=['GET'])
def get_deployment_revenue():
    bucket_name = 'cooldowns2'
    filename = 'combined_deployment_revenue.zip'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    content = blob.download_as_bytes()
    data = []

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for zip_filename in z.namelist():
            if zip_filename.endswith('.csv'):
                with z.open(zip_filename) as f:
                    df = pd.read_csv(f)
                    df['day'] = pd.to_datetime(df['day'])
                    data = df.to_dict(orient='records')

    # Group by day
    grouped_data = {}
    for entry in data:
        day = entry['day'].strftime('%Y-%m-%d')  # Convert to string
        if day not in grouped_data:
            grouped_data[day] = {'day': day, 'total_aggregate_revenue': entry['total_aggregate_revenue']}
        deployment = entry['deployment']
        grouped_data[day][deployment] = entry['total_deployment_revenue']

    # Convert to list of dictionaries
    result = list(grouped_data.values())
    
    return jsonify(result)

# # 7 Days MA
@app.route('/api/7_days_ma_revenue', methods=['GET'])
def get_7_days_ma_revenue():
    bucket_name = 'cooldowns2'
    filename = 'combined_deployment_revenue.zip'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    content = blob.download_as_bytes()
    data = []

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for zip_filename in z.namelist():
            if zip_filename.endswith('.csv'):
                with z.open(zip_filename) as f:
                    df = pd.read_csv(f)
                    df['day'] = pd.to_datetime(df['day'])
                    data = df.to_dict(orient='records')

    # Group by day
    grouped_data = {}
    for entry in data:
        day = entry['day'].strftime('%Y-%m-%d')  # Convert to string
        if day not in grouped_data:
            grouped_data[day] = {
                'day': day, 
                'total_7_days_ma_revenue': entry['total_7_days_ma_revenue']
            }
        deployment = entry['deployment']
        grouped_data[day][f"{deployment}_7_days_ma_revenue"] = entry['7_days_ma_revenue']

    # Convert to list of dictionaries
    result = list(grouped_data.values())
    
    return jsonify(result)

# # 30 days MA 
@app.route('/api/30_days_ma_revenue', methods=['GET'])
def get_30_days_ma_revenue():
    bucket_name = 'cooldowns2'
    filename = 'combined_deployment_revenue.zip'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    content = blob.download_as_bytes()
    data = []

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for zip_filename in z.namelist():
            if zip_filename.endswith('.csv'):
                with z.open(zip_filename) as f:
                    df = pd.read_csv(f)
                    df['day'] = pd.to_datetime(df['day'])
                    data = df.to_dict(orient='records')

    # Group by day
    grouped_data = {}
    for entry in data:
        day = entry['day'].strftime('%Y-%m-%d')  # Convert to string
        if day not in grouped_data:
            grouped_data[day] = {
                'day': day, 
                'total_30_days_ma_revenue': entry['total_30_days_ma_revenue']
            }
        deployment = entry['deployment']
        grouped_data[day][f"{deployment}_30_days_ma_revenue"] = entry['30_days_ma_revenue']

    # Convert to list of dictionaries
    result = list(grouped_data.values())
    
    return jsonify(result)

# # 90 days MA 
@app.route('/api/90_days_ma_revenue', methods=['GET'])
def get_90_days_ma_revenue():
    bucket_name = 'cooldowns2'
    filename = 'combined_deployment_revenue.zip'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    content = blob.download_as_bytes()
    data = []

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for zip_filename in z.namelist():
            if zip_filename.endswith('.csv'):
                with z.open(zip_filename) as f:
                    df = pd.read_csv(f)
                    df['day'] = pd.to_datetime(df['day'])
                    data = df.to_dict(orient='records')

    # Group by day
    grouped_data = {}
    for entry in data:
        day = entry['day'].strftime('%Y-%m-%d')  # Convert to string
        if day not in grouped_data:
            grouped_data[day] = {
                'day': day, 
                'total_90_days_ma_revenue': entry['total_90_days_ma_revenue']
            }
        deployment = entry['deployment']
        grouped_data[day][f"{deployment}_90_days_ma_revenue"] = entry['90_days_ma_revenue']

    # Convert to list of dictionaries
    result = list(grouped_data.values())
    
    return jsonify(result)

# # 180 days MA 
@app.route('/api/180_days_ma_revenue', methods=['GET'])
def get_180_days_ma_revenue():
    bucket_name = 'cooldowns2'
    filename = 'combined_deployment_revenue.zip'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    content = blob.download_as_bytes()
    data = []

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for zip_filename in z.namelist():
            if zip_filename.endswith('.csv'):
                with z.open(zip_filename) as f:
                    df = pd.read_csv(f)
                    df['day'] = pd.to_datetime(df['day'])
                    data = df.to_dict(orient='records')

    # Group by day
    grouped_data = {}
    for entry in data:
        day = entry['day'].strftime('%Y-%m-%d')  # Convert to string
        if day not in grouped_data:
            grouped_data[day] = {
                'day': day, 
                'total_180_days_ma_revenue': entry['total_180_days_ma_revenue']
            }
        deployment = entry['deployment']
        grouped_data[day][f"{deployment}_180_days_ma_revenue"] = entry['180_days_ma_revenue']

    # Convert to list of dictionaries
    result = list(grouped_data.values())
    
    return jsonify(result)

# # revenue_by_type
@app.route('/api/revenue_by_type', methods=['GET'])
def get_revenue_by_type():
    bucket_name = 'cooldowns2'
    filename = 'revenue_by_type.zip'
    storage_client = storage.Client(credentials=get_credentials())
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    content = blob.download_as_bytes()
    data = []

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for zip_filename in z.namelist():
            if zip_filename.endswith('.csv'):
                with z.open(zip_filename) as f:
                    df = pd.read_csv(f)
                    df['day'] = pd.to_datetime(df['day'])
                    
                    # Select only the required columns
                    df = df[['day', 'revenue_type', 'cumulative_revenue']]  # Assuming 'total_revenue' is your cumulative revenue column
                    
                    data = df.to_dict(orient='records')

    # Convert datetime to string for JSON serialization
    for entry in data:
        entry['day'] = entry['day'].strftime('%Y-%m-%d')

    return jsonify(data)

if __name__ == '__main__':
    app.run(use_reloader=True, port=8000, threaded=True, DEBUG=True)