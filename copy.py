import duckdb
import os
import logging
import time
import boto3

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)
s3 = boto3.client('s3')

def load_parquet_files():

    con = None
    yellow_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    green_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"
    bucket = "uvasds-systems"

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # import GREEN taxi data from 2023 to 2024
        # loop through years and months
        for year in range(2024, 2025):
            for month in range(1, 13):
                date_str = f"{year}-{month:02d}"

                # copy files from URL to s3 bucket
                s3.upload_file(f"{green_base_url}{date_str}.parquet", bucket, f"data/taxi/green_tripdata_{date_str}.parquet")
                s3.upload_file(f"{yellow_base_url}{date_str}.parquet", bucket, f"data/taxi/yellow_tripdata_{date_str}.parquet")

                logger.info(f"Loaded {date_str} YELLOW/GREEN parquets into tripdata table")
                time.sleep(60)  # Sleep to avoid overwhelming the server
        logger.info("All GREEN parquet files loaded successfully")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
