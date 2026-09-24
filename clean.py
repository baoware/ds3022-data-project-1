import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

DB_PATH = 'emissions.duckdb'
YEAR = 2024
# name of two created tables
TRIP_TABLES = ['yellow_trips', 'green_trips']

# each rule is a WHERE condition identifying bad rows
CLEANING_RULES = {
    'zero passengers': "passenger_count = 0",
    'zero miles': "trip_distance = 0",
    'over 100 miles': "trip_distance > 100",
    'over 1 day long': "date_diff('second', pickup_datetime, dropoff_datetime) > 86400",
}

# reporting function
def row_count(con, table):
    return con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

def remove_duplicates(con, table):
    # grab the row count before
    before = row_count(con, table)
    # execute deduplication SQL
    # make new table from distinct elements
    # drop the old one and rename the new table
    con.execute(f"""
        CREATE OR REPLACE TABLE {table}_dedup AS
        SELECT DISTINCT * FROM {table};

        DROP TABLE {table};
        ALTER TABLE {table}_dedup RENAME TO {table};
    """)
    # calculate number of removed rows
    removed = before - row_count(con, table)
    print(f"{table}: removed {removed:,} duplicate trips")
    logger.info(f"{table}: removed {removed} duplicate trips")


# function for apply the rules
def apply_rules(con, table):
    # each cleaning rule item is name then the condition so grab those two
    for name, condition in CLEANING_RULES.items():
        # execute the simple condition in the passed table
        removed = con.execute(f"DELETE FROM {table} WHERE {condition};").fetchone()[0]
        print(f"{table}: removed {removed:,} trips ({name})")
        logger.info(f"{table}: removed {removed} trips ({name})")


def verify_clean(con, table):
    # raises if any duplicate or rule-violating rows remain
    # array to keep any failures
    failures = []

    # grab the distinct rows count
    distinct = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table});").fetchone()[0]
    # if distinct is less than the actual row count then there are duplicates
    dups = row_count(con, table) - distinct
    print(f"{table}: check duplicates remaining = {dups}")
    logger.info(f"{table}: check duplicates remaining = {dups}")
    if dups:
        failures.append('duplicates')

    # same loop
    for name, condition in CLEANING_RULES.items():
        # instead of DELETE, COUNT the rows still satisfying the removal condition
        bad = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition};").fetchone()[0]
        print(f"{table}: check {name} remaining = {bad}")
        logger.info(f"{table}: check {name} remaining = {bad}")
        if bad:
            failures.append(name)

    if failures:
        raise ValueError(f"{table} failed cleaning checks: {', '.join(failures)}")
    logger.info(f"{table}: all cleaning checks passed")


# main function: dedup, apply rules, and verify each trip table
def clean_parquet():

    con = None

    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Connected to DuckDB instance")

        # only two tables
        for table in TRIP_TABLES:
            # count rows
            before = row_count(con, table)
            # run dedup
            remove_duplicates(con, table)
            # apply rules
            apply_rules(con, table)
            # then clean
            verify_clean(con, table)
            # then count change
            after = row_count(con, table)
            print(f"{table}: {before:,} -> {after:,} rows ({before - after:,} removed)")
            logger.info(f"{table}: {before} -> {after} rows ({before - after} removed)")

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
    clean_parquet()
