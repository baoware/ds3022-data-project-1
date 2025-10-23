import duckdb
from prefect import task, flow
from prefect.logging import get_run_logger
import os
import logging
import time

@task(name="connect_and_setup_tables")
def connect_and_setup_tables(db_path: str):
    """Create connection and set up tables"""
    logger = get_run_logger()
    con = duckdb.connect(db_path)
    logger.info("Connected to DuckDB instance")

    try:
        con.execute(f"""
            -- SQL goes here
            CREATE OR REPLACE TABLE tripdata (
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                trip_distance DOUBLE,
                PULocationID INT,
                DOLocationID INT,
                fare_amount DOUBLE,
                passenger_count INT,
                taxi_type VARCHAR,
                trip_duration INTERVAL,
                trip_co2_kgs DOUBLE,
                avg_mph DOUBLE,
                hour_of_day INTEGER,
                day_of_week INTEGER,
                week_of_year INTEGER,
                month_of_year INTEGER
            );
            """)
        logger.info("Created primary table")
        con.close()
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        con.close()
        return None

@task(name="load_parquet_files", retries=5, retry_delay_seconds=60)
def load_parquet_files(db_path: str):
    """Load parquet files into database"""
    logger = get_run_logger()
    con = duckdb.connect(db_path)
    yellow_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    green_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"

    try:
        # import YELLOW taxi data from 2024 to 2025
        # loop through years and months
        for year in range(2024, 2025):
            for month in range(1, 2):
                date_str = f"{year}-{month:02d}"
                con.execute(f"""
                    INSTALL httpfs;
                    LOAD httpfs;
                    SET http_retries = 5;
                    SET http_retry_wait_ms = 30000;
                    -- SET http_retry_backoff_factor = 2.0;
                    INSERT INTO tripdata
                    SELECT tpep_pickup_datetime AS pickup_datetime, 
                        tpep_dropoff_datetime AS dropoff_datetime, 
                        trip_distance, 
                        PULocationID, 
                        DOLocationID, 
                        fare_amount,
                        passenger_count,
                        'yellow_taxi' AS taxi_type,
                        NULL,0,0,0,0,0,0  -- placeholders for new columns
                    FROM read_parquet('{yellow_base_url}{date_str}.parquet');
                """)
                logger.info(f"Loaded {date_str} YELLOW parquet into tripdata table")
                time.sleep(20)  # Sleep to avoid overwhelming the server
        logger.info("All YELLOW parquet files loaded successfully")

        # import GREEN taxi data from 2024 to 2025
        # loop through years and months
        for year in range(2024, 2025):
            for month in range(1, 2):
                date_str = f"{year}-{month:02d}"
                con.execute(f"""
                    INSTALL httpfs;
                    LOAD httpfs;
                    SET http_retries = 5;
                    SET http_retry_wait_ms = 30000;
                    -- SET http_retry_backoff_factor = 2.0;
                    INSERT INTO tripdata
                    SELECT lpep_pickup_datetime AS pickup_datetime, 
                        lpep_dropoff_datetime AS dropoff_datetime, 
                        trip_distance, 
                        PULocationID, 
                        DOLocationID, 
                        fare_amount,
                        passenger_count,
                        'green_taxi' AS taxi_type,
                        NULL,0,0,0,0,0,0  -- placeholders for new columns
                    FROM read_parquet('{green_base_url}{date_str}.parquet');
                """)
                logger.info(f"Loaded {date_str} GREEN parquet into tripdata table")
                time.sleep(20)  # Sleep to avoid overwhelming the server
        logger.info("All GREEN parquet files loaded successfully")
        con.close()

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        con.close()

@task(name="load_other")
def load_other(db_path: str):
    """Load other tables into database"""
    logger = get_run_logger()
    try:
        con = duckdb.connect(db_path)
        con.execute(f"""
            CREATE OR REPLACE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('./data/vehicle_emissions.csv');
        """)
        logger.info("created vehicle_emissions table")

        con.execute(f"""
            -- SQL goes here
            CREATE OR REPLACE TABLE analytics (
                id INT PRIMARY KEY,
                -- TOP
                largest_carbon_yellow TIMESTAMP,
                largest_carbon_green TIMESTAMP,
                -- HOUR
                most_carbon_heavy_hour_yellow INTEGER,
                most_carbon_heavy_hour_green INTEGER,
                least_carbon_heavy_hour_yellow INTEGER,
                least_carbon_heavy_hour_green INTEGER,
                -- DOW
                most_carbon_heavy_day_of_week_yellow INTEGER,
                most_carbon_heavy_day_of_week_green INTEGER,
                least_carbon_heavy_day_of_week_yellow INTEGER,
                least_carbon_heavy_day_of_week_green INTEGER,
                -- WEEK
                most_carbon_heavy_week_of_year_yellow INTEGER,
                most_carbon_heavy_week_of_year_green INTEGER,
                least_carbon_heavy_week_of_year_yellow INTEGER,
                least_carbon_heavy_week_of_year_green INTEGER,
                -- MONTH
                most_carbon_heavy_month_of_year_yellow INTEGER,
                most_carbon_heavy_month_of_year_green INTEGER,
                least_carbon_heavy_month_of_year_yellow INTEGER,
                least_carbon_heavy_month_of_year_green INTEGER
            );
        """)
        logger.info("Created data analytics table")
        con.close()

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        con.close()

# Child flow to load data
@flow(name="ingest_data", description="Load data into DuckDB")
def data_intake(db_path: str):
    connect_and_setup_tables(db_path)
    load_parquet_files(db_path)
    load_other(db_path)

""" ^^^ End of ingest_data flow """
""" ^^^ Begin clean_data flow """

@task(name="clean_table", description="Clean table in DuckDB", log_prints=True)
def clean_table(db_path: str):
    logger = get_run_logger()
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(db_path)
        logger.info("Connected to DuckDB instance")

        duplicate_trips = con.execute(f"""
            -- SQL goes here
            SELECT
                *,
                COUNT(*) AS duplicate_count
            FROM
                tripdata
            GROUP BY
                pickup_datetime,
                dropoff_datetime,
                trip_distance,
                PULocationID,
                DOLocationID,
                fare_amount,
                passenger_count,
                taxi_type,
                trip_duration,
                trip_co2_kgs,
                avg_mph,
                hour_of_day,
                day_of_week,
                week_of_year,
                month_of_year
            HAVING
                COUNT(*) > 1;
        """)
        print(f"Duplicate Trips: {duplicate_trips.fetchall()[0]}")
        logger.info(f"Duplicate trips: {duplicate_trips.fetchall()[0]}")

        zero_distance = con.execute(f"""
            -- SQL goes here
            SELECT COUNT(*) FROM tripdata
            WHERE trip_distance = 0;
        """)
        print(f"Distance of 0: {zero_distance.fetchone()[0]}")
        logger.info(f"Trip distances of 0 miles: {zero_distance.fetchone()[0]}")

        hundred_distance = con.execute(f"""
            -- SQL goes here
            SELECT COUNT(trip_distance) FROM tripdata
            WHERE trip_distance > 100;
        """)
        # print(f"Distance of 100: {hundred_distance.fetchone()[0]}")
        logger.info(f"Trip distances over 100 miles: {hundred_distance.fetchone()[0]}")

        con.execute(f"""
            DELETE FROM tripdata
            WHERE trip_distance = 0;
            DELETE FROM tripdata
            WHERE trip_distance > 100;
            DELETE FROM tripdata
            WHERE fare_amount < 0;
            DELETE FROM tripdata
            WHERE passenger_count = 0 OR passenger_count IS NULL;
            DELETE FROM tripdata
            WHERE EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) > 86400;
            DELETE FROM tripdata
            WHERE (pickup_datetime IS NULL OR dropoff_datetime IS NULL);
        ;
        """)
        logger.info("Cleaning steps completed.")
        con.close()
        return True

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        con.close()
        return None

# Child flow to clean data
@flow(name="clean_data", description="Clean data in DuckDB")
def clean_data(db_path: str):
    clean_table(db_path)




# Child flow to transform data
@flow(name="transform_data", description="Transform data in DuckDB")
def transform_data(db_path: str):
    # transform_table(db_path)
    pass



# Main flow to orchestrate data loading and transformation
@flow(name="main_flow", description="Main flow to orchestrate data loading and transformation")
def main_flow(db_path: str):
    """Main flow to orchestrate data loading and transformation"""
    data_intake(db_path)
    clean_data(db_path)
    transform_data(db_path)

# Default handler for script execution w/ keyboard interrupt
if __name__ == "__main__":
    try:
        main_flow(db_path="emissions.duckdb")

    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Exiting...")
        sys.exit(1)

