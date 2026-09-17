import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

DB_PATH = 'emissions.duckdb'
BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
YEAR = 2024
MONTHS = range(1, 13)

TAXI_TYPES = {
    'yellow': 'tpep',
    'green': 'lpep',
}


def parquet_urls(taxi_type, year=YEAR, months=MONTHS):
    return [f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet" for month in months]


def create_trip_table(con, taxi_type):
    # only the columns needed for cleaning, transforming, and analysis are kept.
    table = f"{taxi_type}_trips"
    con.execute(f"""
        CREATE OR REPLACE TABLE {table} (
            VendorID INTEGER,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count BIGINT,
            trip_distance DOUBLE,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            total_amount DOUBLE
        );
    """)
    logger.info(f"Created table {table}")
    return table


def load_trip_table(con, taxi_type, prefix, year=YEAR, months=MONTHS):
    table = create_trip_table(con, taxi_type)
    for url in parquet_urls(taxi_type, year, months):
        try:
            con.execute(f"""
                INSERT INTO {table}
                SELECT
                    VendorID,
                    {prefix}_pickup_datetime,
                    {prefix}_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    PULocationID,
                    DOLocationID,
                    total_amount
                FROM read_parquet('{url}');
            """)
            logger.info(f"Inserted {url} into {table}")
        except Exception as e:
            logger.error(f"Failed to insert {url} into {table}: {e}")
            raise
    return table


def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Connected to DuckDB instance")

        for taxi_type, prefix in TAXI_TYPES.items():
            load_trip_table(con, taxi_type, prefix)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection")


if __name__ == "__main__":
    load_parquet_files()
