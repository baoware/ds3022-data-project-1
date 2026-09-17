import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

DB_PATH = 'emissions.duckdb'
YEAR = 2024
TRIP_TABLES = ['yellow_trips', 'green_trips']

# each rule is a WHERE condition identifying bad rows
CLEANING_RULES = {
    'zero passengers': "passenger_count = 0",
    'zero miles': "trip_distance = 0",
    'over 100 miles': "trip_distance > 100",
    'over 1 day long': "date_diff('second', pickup_datetime, dropoff_datetime) > 86400",
}


def row_count(con, table):
    return con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

def remove_duplicates(con, table):
    before = row_count(con, table)
    con.execute(f"""
        CREATE OR REPLACE TABLE {table}_dedup AS
        SELECT DISTINCT * FROM {table};

        DROP TABLE {table};
        ALTER TABLE {table}_dedup RENAME TO {table};
    """)
    removed = before - row_count(con, table)
    print(f"{table}: removed {removed:,} duplicate trips")
    logger.info(f"{table}: removed {removed} duplicate trips")


def apply_rules(con, table):
    for name, condition in CLEANING_RULES.items():
        removed = con.execute(f"DELETE FROM {table} WHERE {condition};").fetchone()[0]
        print(f"{table}: removed {removed:,} trips ({name})")
        logger.info(f"{table}: removed {removed} trips ({name})")


def verify_clean(con, table):
    # raises if any duplicate or rule-violating rows remain
    failures = []

    distinct = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table});").fetchone()[0]
    dups = row_count(con, table) - distinct
    print(f"{table}: check duplicates remaining = {dups}")
    logger.info(f"{table}: check duplicates remaining = {dups}")
    if dups:
        failures.append('duplicates')

    for name, condition in CLEANING_RULES.items():
        bad = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition};").fetchone()[0]
        print(f"{table}: check {name} remaining = {bad}")
        logger.info(f"{table}: check {name} remaining = {bad}")
        if bad:
            failures.append(name)

    if failures:
        raise ValueError(f"{table} failed cleaning checks: {', '.join(failures)}")
    logger.info(f"{table}: all cleaning checks passed")


def clean_parquet():

    con = None

    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Connected to DuckDB instance")

        for table in TRIP_TABLES:
            before = row_count(con, table)
            remove_duplicates(con, table)
            apply_rules(con, table)
            verify_clean(con, table)
            after = row_count(con, table)
            print(f"{table}: {before:,} -> {after:,} rows ({before - after:,} removed)")
            logger.info(f"{table}: {before} -> {after} rows ({before - after} removed)")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection")


if __name__ == "__main__":
    clean_parquet()
