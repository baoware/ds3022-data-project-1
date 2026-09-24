import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)

DB_PATH = 'emissions.duckdb'
TRIP_TABLES = {
    'yellow_trips': 'yellow_taxi',
    'green_trips': 'green_taxi',
}

# new columns to be added
NEW_COLUMNS = {
    'trip_co2_kgs': 'DOUBLE',
    'avg_mph': 'DOUBLE',
    'hour_of_day': 'INTEGER',
    'day_of_week': 'INTEGER',
    'week_of_year': 'INTEGER',
    'month_of_year': 'INTEGER',
}

# seconds between pickup and dropoff for calculating avg_mph
DURATION_SECS = "date_diff('second', pickup_datetime, dropoff_datetime)"


def add_columns(con, table):
    # IF NOT EXISTS so the script can be re-run later
    # loop to add each new column
    for column, dtype in NEW_COLUMNS.items():
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {dtype};")
    logger.info(f"{table}: added columns {', '.join(NEW_COLUMNS)}")


def check_emissions_lookup(con, taxi_type):
    # the lookup must match exactly one row or the co2 join would be wrong
    found = con.execute(
        "SELECT COUNT(*) FROM vehicle_emissions WHERE vehicle_type = ?;", [taxi_type]
    ).fetchone()[0]
    if found != 1:
        raise ValueError(f"expected 1 vehicle_emissions row for {taxi_type}, found {found}")


def update_trip_co2(con, table, taxi_type):
    # co2 factor is looked up from vehicle_emissions at run time, not hard-coded
    check_emissions_lookup(con, taxi_type)
    # use UPDATE then fill in the trip_co2_kgs with tbe calculation
    con.execute(f"""
        UPDATE {table}
        SET trip_co2_kgs = {table}.trip_distance * e.co2_grams_per_mile / 1000
        FROM vehicle_emissions e
        WHERE e.vehicle_type = '{taxi_type}';
    """)
    logger.info(f"{table}: calculated trip_co2_kgs using {taxi_type}")


def update_avg_mph(con, table):
    # trips with zero or negative duration get NULL instead of dividing by zero
    con.execute(f"""
        UPDATE {table}
        SET avg_mph = CASE
            WHEN {DURATION_SECS} > 0 THEN trip_distance / ({DURATION_SECS} / 3600.0)
            ELSE NULL
        END;
    """)
    logger.info(f"{table}: calculated avg_mph")


def update_time_parts(con, table):
    # hour 0-23, day of week 0 (Sun) - 6 (Sat), ISO week 1-53, month 1-12
    # set all the values at once using the in-built functions
    con.execute(f"""
        UPDATE {table}
        SET hour_of_day = hour(pickup_datetime),
            day_of_week = dayofweek(pickup_datetime),
            week_of_year = week(pickup_datetime),
            month_of_year = month(pickup_datetime);
    """)
    logger.info(f"{table}: extracted hour_of_day, day_of_week, week_of_year, month_of_year")


# function to print and log summary stats proving the new columns were filled
def report_transform(con, table):
    stats = con.execute(f"""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (trip_co2_kgs IS NULL),
            ROUND(SUM(trip_co2_kgs), 1),
            COUNT(*) FILTER (avg_mph IS NULL),
            MIN(hour_of_day), MAX(hour_of_day),
            MIN(day_of_week), MAX(day_of_week),
            MIN(week_of_year), MAX(week_of_year),
            MIN(month_of_year), MAX(month_of_year)
        FROM {table};
    """).fetchone()
    lines = [
        f"{table}: {stats[0]:,} rows",
        f"{table}: trip_co2_kgs NULL = {stats[1]:,}, total = {stats[2]:,} kg",
        f"{table}: avg_mph NULL (non-positive duration) = {stats[3]:,}",
        f"{table}: hour_of_day {stats[4]}-{stats[5]}, day_of_week {stats[6]}-{stats[7]}, "
        f"week_of_year {stats[8]}-{stats[9]}, month_of_year {stats[10]}-{stats[11]}",
    ]
    for line in lines:
        print(line)
        logger.info(line)


# main function: add and fill the new columns for each trip table
def transform_trips():

    con = None
    
    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Connected to DuckDB instance")

        # for the two taxi color tables
        for table, taxi_type in TRIP_TABLES.items():
            # first add the new columns
            add_columns(con, table)
            # call the remaining built functions to update
            update_trip_co2(con, table, taxi_type)
            update_avg_mph(con, table)
            update_time_parts(con, table)
            report_transform(con, table)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        # non-zero exit so run_pipeline.py stops at the failed stage
        raise SystemExit(1)

    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection")


if __name__ == "__main__":
    transform_trips()
