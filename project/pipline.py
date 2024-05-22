import pandas as pd
import requests
import zipfile
import io
from sqlalchemy import create_engine
import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

def download_and_extract_csv(url, local_filename, extracted_csv_filename):
    """Download ZIP file from a URL, extract CSV, and save it locally."""
    
    # Ensure kaggle.json is in the correct path or set environment variables KAGGLE_USERNAME and KAGGLE_KEY
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(url, path=extracted_csv_filename, unzip=True)
    #response = requests.get(url)
    #zip_data = io.BytesIO(response.content)
    #
    #with zipfile.ZipFile(zip_data, 'r') as zip_ref:
    #    zip_ref.extract(extracted_csv_filename, path='.')
    #
    #print(f"CSV file extracted and saved as {extracted_csv_filename}")

def read_csv_to_dataframe(filename):
    """Read a CSV file into a pandas DataFrame."""
    df = pd.read_csv(filename)
    print(f"CSV file {filename} read into DataFrame")
    return df

def manipulate_data(df):
    """Perform data manipulation on the DataFrame."""
    df.columns = [col.lower() for col in df.columns]  # Convert column names to lowercase
    df['processed'] = True  # Add a new column 'processed' with True value
    # Example: Filter rows where a column 'value' is greater than 10
    # df = df[df['value'] > 10]
    print("Data manipulation completed")
    return df

def save_dataframe_to_sqlite(df, database_filename, table_name):
    """Save the DataFrame to an SQLite database."""
    engine = create_engine(f'sqlite:///{database_filename}')
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data saved to SQLite database {database_filename} in table {table_name}")

def main():
    zip_url = 'bhavikjikadara/mental-health-dataset'  # Update with your actual URL
    local_csv_filename = 'data/downloaded_data.zip'
    #extracted_csv_filename = 'data/extracted_data.csv'  # Name of the CSV file inside the ZIP
    extracted_csv_filename = 'data/'
    database_filename = 'data/processed_data.sqlite'  # Use .sqlite extension
    table_name = 'table_name'

    download_and_extract_csv(zip_url, local_csv_filename, extracted_csv_filename)
    df = read_csv_to_dataframe(extracted_csv_filename)
    df = manipulate_data(df)
    save_dataframe_to_sqlite(df, database_filename, table_name)
    print("Process completed successfully")

if __name__ == "__main__":
    main()
