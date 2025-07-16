# Importing necessary libraries
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# Data Loading
def run_loading():
    # Loading the dataset
    data = pd.read_csv(r'cleaned_MarketsData.csv')
    crypto_assets = pd.read_csv(r'crypto_assets.csv')
    crypto_market = pd.read_csv(r'crypto_market.csv')
    crypto_prices = pd.read_csv(r'crypto_prices.csv')
    crypto_datetime = pd.read_csv(r'crypto_datetime.csv')
    transaction = pd.read_csv(r'transaction.csv')

    # Load environment variables from .env file
    load_dotenv()

    connect_str = os.getenv('AZURE_CONNECTION_STRING')
    container_name = os.getenv('CONTAINER_NAME')

    # create a BlobServiceClient Object
    blob_service_client = BlobServiceClient(account_url=AZURE_BLOB_URL, credential=AccountKey)

    # Create a container client
    container_name = 'blockpulsecontainer'  # Replace with your container name
    container_client = blob_service_client.get_container_client(container_name)

    # Load data into Azure Blob Storage
    # List of tuples containing DataFrames and their corresponding blob names
    files = [
        (data, 'rawdata/cleaned_MarketsData.csv'),
        (crypto_assets, 'cleaneddata/crypto_assets.csv'),
        (crypto_market, 'cleaneddata/crypto_market.csv'),
        (crypto_prices, 'cleaneddata/crypto_prices.csv'),
        (crypto_datetime, 'cleaneddata/crypto_datetime.csv'),
        (transaction, 'cleaneddata/transaction.csv')
    ]

    for file, blob_name in files:
        bob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        bob_client.upload_blob(output, overwrite=True)
        print(f'file {blob_name} uploaded successfully to Azure Blob Storage.')