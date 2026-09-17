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


def clean_parquet():

    con = None

    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Connected to DuckDB instance")

        for table in TRIP_TABLES:
            before = row_count(con, table)
            remove_duplicates(con, table)
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
