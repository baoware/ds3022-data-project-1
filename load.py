import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

DB_PATH = 'emissions.duckdb'
BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data' # beginning to every parquet url
YEAR = 2024 # desired year
MONTHS = range(1, 13) # month indexing
EMISSIONS_CSV = os.path.join('data', 'vehicle_emissions.csv') # existing csv for emissions

# color and prefix for generating parquet links
TAXI_TYPES = {
    'yellow': 'tpep',
    'green': 'lpep',
}

# function to format the parquet urls
def parquet_urls(taxi_type, year=YEAR, months=MONTHS):
    return [f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet" for month in months]

# function for creating the table
def create_trip_table(con, taxi_type):
    # after looking ahead in the assignment,
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

# function taking the DB instance, taxi color, prefix, desired year, and months
def load_trip_table(con, taxi_type, prefix, year=YEAR, months=MONTHS):
    # create the table
    table = create_trip_table(con, taxi_type)
    # returns all the formed parquet_urls for the taxi color 
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


def load_emissions_table(con, csv_path=EMISSIONS_CSV):
    con.execute(f"""
        CREATE OR REPLACE TABLE vehicle_emissions AS
        SELECT * FROM read_csv_auto('{csv_path}', header=True);
    """)
    logger.info(f"Loaded vehicle_emissions from {csv_path}")
    return 'vehicle_emissions'

# function for reporting
def report_row_counts(con, tables):
    # go through each table
    for table in tables:
        # simply return the row count
        count = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        print(f"{table}: {count:,} raw rows")
        logger.info(f"{table}: {count} raw rows")


def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Connected to DuckDB instance")

        tables = []
        # iterate through the two taxi colors
        for taxi_type, prefix in TAXI_TYPES.items():
            # append the returned table
            tables.append(load_trip_table(con, taxi_type, prefix))
        # append the emissions table separately
        tables.append(load_emissions_table(con))

        # run the report
        report_row_counts(con, tables) 

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection")


if __name__ == "__main__":
    load_parquet_files()
