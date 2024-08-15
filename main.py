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
from flask import Flask, jsonify, request

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
cors = CORS(app, origins='*')

# Initialize GCP storage client
credentials, project = default()
storage_client = storage.Client(credentials=credentials, project=project)

def get_balance_df():
    # Initialize the Google Cloud Storage client
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket('cooldowns2')

    # Get the zip file blob
    blob = bucket.blob('weeth_balances.zip')

    # Download the contents of the zip file to a bytes buffer
    zip_buffer = io.BytesIO()
    blob.download_to_file(zip_buffer)
    zip_buffer.seek(0)

    # Open the zip file
    with zipfile.ZipFile(zip_buffer) as zip_file:
        # Read the CSV file from within the zip
        with zip_file.open('weeth_balances.csv') as csv_file:
            # Read the CSV into a pandas DataFrame
            df = pd.read_csv(csv_file)

    df[['effective_balance']] = df[['effective_balance']].astype(float)
    df[['block_number']] = df[['block_number']].astype(int)

    return df

# # will return a dataframe with only the most recent row of data for each user that is less than or equal to the specified block_number
def get_most_recent_block_balances(df, block_number, address_list=None):

    if address_list is not None:
        df = df.loc[df['user'].isin(address_list)]

    # # will make out dataframe only contain blocks that are less than or equal to the block requested
    df = df.loc[df['block_number'] <= block_number]

    # Group by 'user' and find the index of the max 'block_number' for each user
    grouped_df = df.groupby('user')['block_number'].idxmax()

    # Use these indices to select the corresponding rows from the original DataFrame
    df = df.loc[grouped_df]

    # Reset the index if you want to remove the hierarchical index
    df = df.reset_index(drop=True)

    return df


@app.route('/user_balances', methods=['GET'])
def user_balances():
    try:
        # Get the block_number from query parameters
        block_number = request.args.get('block_number', type=int)
        
        # Get the list of addresses from query parameters
        addresses = request.args.get('addresses')
        if addresses:
            # Split the comma-separated string into a list
            address_list = [addr.strip() for addr in addresses.split(',')]
        else:
            address_list = None

        # Get the DataFrame
        df = get_balance_df()
        
        # Get the latest balance for each user
        if address_list is not None:
            max_block_df = get_most_recent_block_balances(df, block_number, address_list)
        
        else:
            max_block_df = get_most_recent_block_balances(df, block_number)
        
        # Prepare the result in the specified format
        result = [
            {
                "address": row['user'],
                "effective_balance": round(row['effective_balance'], 18)  # Adjust precision as needed
            }
            for _, row in max_block_df.iterrows()
        ]
        
        # Prepare the response
        response = {
            "Result": result
            # "block_number": block_number if block_number is not None else int(max_block_df['block_number'].max())
        }
        
        return jsonify(response)
    
    except Exception as e:
        # Handle any errors
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(use_reloader=True, port=8000, threaded=True, DEBUG=True)

# df = get_balance_df()
# df = get_most_recent_block_balances(df, 8402412)
# print(df)