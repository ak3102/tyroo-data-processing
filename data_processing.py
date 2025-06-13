import pandas as pd
import requests
import gzip
import shutil
import os
import sqlite3
import logging

logging.basicConfig(filename='data_processing.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

CSV_GZ_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
CSV_GZ_FILENAME = "Tyroo-dummy-data.csv.gz"
CSV_FILENAME = "Tyroo-dummy-data.csv"
DB_FILENAME = "tyroo_data.db"
CHUNK_SIZE = 10000

def download_csv():
    try:
        logging.info("Starting download...")
        with requests.get(CSV_GZ_URL, stream=True) as r:
            with open(CSV_GZ_FILENAME, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        logging.info("Download completed.")
    except Exception as e:
        logging.error(f"Error during download: {e}")
        raise

def decompress_csv():
    try:
        logging.info("Decompressing file...")
        with gzip.open(CSV_GZ_FILENAME, 'rb') as f_in:
            with open(CSV_FILENAME, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logging.info("Decompression completed.")
    except Exception as e:
        logging.error(f"Error during decompression: {e}")
        raise

def create_database():
    try:
        logging.info("Creating database schema...")
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ad_id TEXT,
            campaign TEXT,
            impressions INTEGER,
            clicks INTEGER,
            revenue REAL
        )
        """)
        conn.commit()
        conn.close()
        logging.info("Database schema created.")
    except Exception as e:
        logging.error(f"Error creating database schema: {e}")
        raise

def process_csv():
    try:
        logging.info("Starting CSV processing...")
        conn = sqlite3.connect(DB_FILENAME)
        for chunk in pd.read_csv(CSV_FILENAME, chunksize=CHUNK_SIZE):
            
            chunk = chunk.dropna()
            chunk.columns = [col.strip().lower().replace(" ", "_") for col in chunk.columns]
            chunk.to_sql("data", conn, if_exists="append", index=False)
            logging.info(f"Chunk of size {len(chunk)} processed.")
        conn.close()
        logging.info("CSV processing completed.")
    except Exception as e:
        logging.error(f"Error during CSV processing: {e}")
        raise

if __name__ == "__main__":
    try:
        download_csv()
        decompress_csv()
        create_database()
        process_csv()
        print(" Data processing completed successfully.")
    except Exception as err:
        print(" Process failed. Check log file for details.")