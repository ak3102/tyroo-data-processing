import pandas as pd
import gzip
import shutil
import requests
import sqlite3
import logging
import os

# Setup logging
logging.basicConfig(filename='data_processing.log', level=logging.INFO)

# Constants
DOWNLOAD_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
CSV_GZ_FILE = "Tyroo-dummy-data.csv.gz"
CSV_FILE = "Tyroo-dummy-data.csv"
DB_FILE = "tyroo_data.db"
CHUNK_SIZE = 10000

def download_csv():
    try:
        logging.info("Downloading the file...")
        response = requests.get(DOWNLOAD_URL, stream=True)
        with open(CSV_GZ_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        logging.info("Download complete.")
    except Exception as e:
        logging.error(f"Failed to download CSV: {e}")
        raise

def decompress_csv():
    try:
        logging.info("Decompressing the file...")
        with gzip.open(CSV_GZ_FILE, 'rb') as f_in:
            with open(CSV_FILE, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logging.info("Decompression complete.")
    except Exception as e:
        logging.error(f"Failed to decompress CSV: {e}")
        raise

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ad_id TEXT,
        campaign TEXT,
        impressions INTEGER,
        clicks INTEGER,
        revenue REAL
    );
    """)
    conn.commit()

def process_and_store_data():
    conn = sqlite3.connect(DB_FILE)
    create_table(conn)
    try:
        for chunk in pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE):
            # Clean & transform
            chunk = chunk.dropna()
            chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
            chunk.to_sql("data", conn, if_exists="append", index=False)
            logging.info(f"Processed a chunk of size: {len(chunk)}")
    except Exception as e:
        logging.error(f"Error processing data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        download_csv()
        decompress_csv()
        process_and_store_data()
        print("Data processing completed successfully.")
    except Exception as e:
        print(f"Script failed: {e}")
