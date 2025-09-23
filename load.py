import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():

    con = None
    yellow_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    green_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute(f"""
            -- SQL goes here
            CREATE OR REPLACE TABLE tripdata (
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                trip_distance DOUBLE,
                PULocationID INT,
                DOLocationID INT,
                fare_amount DOUBLE,
                taxi_type VARCHAR,
            );
        """)
        logger.info("Created-dropped table if exists")

        # import YELLOW taxi data from 2023 to 2024
        # loop through years and months
        for year in range(2024, 2025):
            for month in range(1, 13):
                date_str = f"{year}-{month:02d}"
                con.execute(f"""
                    INSERT INTO tripdata
                    SELECT tpep_pickup_datetime AS pickup_datetime, 
                        tpep_dropoff_datetime AS dropoff_datetime, 
                        trip_distance, 
                        PULocationID, 
                        DOLocationID, 
                        fare_amount,
                        'yellow' AS taxi_type
                    FROM read_parquet('{yellow_base_url}{date_str}.parquet');
                """)
                logger.info(f"Loaded {date_str} YELLOW parquet into tripdata table")
                time.sleep(20)  # Sleep to avoid overwhelming the server
        logger.info("All YELLOW parquet files loaded successfully")

        # import GREEN taxi data from 2023 to 2024
        # loop through years and months
        for year in range(2024, 2025):
            for month in range(1, 13):
                date_str = f"{year}-{month:02d}"
                con.execute(f"""
                    INSERT INTO tripdata
                    SELECT lpep_pickup_datetime AS pickup_datetime, 
                        lpep_dropoff_datetime AS dropoff_datetime, 
                        trip_distance, 
                        PULocationID, 
                        DOLocationID, 
                        fare_amount,
                        'green' AS taxi_type
                    FROM read_parquet('{green_base_url}{date_str}.parquet');
                """)
                logger.info(f"Loaded {date_str} GREEN parquet into tripdata table")
                time.sleep(20)  # Sleep to avoid overwhelming the server
        logger.info("All GREEN parquet files loaded successfully")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
