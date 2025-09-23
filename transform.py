import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)

"""
Calculate total CO2 output per trip by multiplying the trip_distance 
    by the co2_grams_per_mile value in the vehicle_emissions lookup table, 
    then dividing by 1000 (to calculate Kg). Insert that value as a new 
    column named trip_co2_kgs. This calculation should be based upon a 
    real-time lookup from the vehicle_emissions table and not hard-coded 
    as a numeric figure.
Calculate average miles per hour based on distance divided by the 
    duration of the trip, and insert that value as a new column avg_mph.
Extract the HOUR of the day from the pickup_time and insert it as a 
    new column hour_of_day.
Extract the DAY OF WEEK from the pickup time and insert it as a new 
    column day_of_week.
Extract the WEEK NUMBER from the pickup time and insert it as a new 
    column week_of_year.
Extract the MONTH from the pickup time and insert it as a new column 
    month_of_year.
"""

def transform_table():
    con = None
    try:
        # Connect to local DuckDB instance
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
        print("Transformation completed successfully.")
        logger.info("Transformation completed successfully.")

        con.execute(f"""
            UPDATE tripdata
            SET trip_co2_kgs = trip_distance * 
                (SELECT co2_grams_per_mile FROM vehicle_emissions 
                WHERE vehicle_type = tripdata.taxi_type) / 1000;
        """)
        print("Trip CO2 calculation completed successfully.")
        logger.info("Trip CO2 calculation completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_table()
