import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)

def transform_table():
    con = None
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute(f"""
            UPDATE tripdata
            SET avg_mph = trip_distance / (EXTRACT(EPOCH FROM trip_duration) / 3600),
                hour_of_day = EXTRACT(HOUR FROM pickup_datetime),
                day_of_week = EXTRACT(DOW FROM pickup_datetime),
                week_of_year = EXTRACT(WEEK FROM pickup_datetime),
                month_of_year = EXTRACT(MONTH FROM pickup_datetime);
        """)
        logger.info("Transformation completed successfully.")

        con.execute(f"""
            UPDATE tripdata
            SET trip_co2_kgs = trip_distance * 
                (SELECT co2_grams_per_mile FROM vehicle_emissions 
                WHERE vehicle_type = tripdata.taxi_type) / 1000;
        """)
        logger.info("Trip CO2 calculation completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_table()
