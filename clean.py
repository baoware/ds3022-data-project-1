import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_table():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
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
        # logger.info(f"Trip distances over 100 miles: {hundred_distance.fetchone()[0]}")

        con.execute(f"""
            DELETE FROM tripdata
            WHERE trip_distance = 0;
            DELETE FROM tripdata
            WHERE trip_distance > 100;
            DELETE FROM tripdata
            WHERE fare_amount < 0;
            DELETE FROM tripdata
            WHERE passenger_count = 0;
            DELETE FROM tripdata
            WHERE EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) > 86400;
            DELETE FROM tripdata
            WHERE (pickup_datetime IS NULL OR dropoff_datetime IS NULL);
        ;
        """)
        logger.info("Cleaning steps completed.")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_table()
