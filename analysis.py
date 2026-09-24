import duckdb
import logging
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

DB_PATH = 'emissions.duckdb'
PLOT_PATH = 'co2_by_month.png'

# trip table
TRIP_TABLES = {
    'yellow_trips': 'YELLOW',
    'green_trips': 'GREEN',
}

# arrays for days and months
DAY_NAMES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def report(line):
    print(line)
    logger.info(line)


def largest_trip(con, table, label):
    # question 1: single largest carbon producing trip of the year
    row = con.execute(f"""
        SELECT trip_co2_kgs, pickup_datetime, trip_distance
        FROM {table}
        WHERE trip_co2_kgs IS NOT NULL
        ORDER BY trip_co2_kgs DESC
        LIMIT 1;
    """).fetchone()
    report(f"{label} largest carbon producing trip: {row[0]:,.2f} kg CO2 "
           f"({row[2]:,.2f} miles, picked up {row[1]})")


def heavy_and_light(con, table, label, column, question, formatter=str):
    # questions 2-5: heaviest and lightest bucket by AVERAGE co2 per trip
    # average the trip_co2_kgs of each group within a column
    rows = con.execute(f"""
        SELECT {column}, AVG(trip_co2_kgs) AS avg_co2
        FROM {table}
        WHERE trip_co2_kgs IS NOT NULL
        GROUP BY {column}
        ORDER BY avg_co2 DESC;
    """).fetchall()
    heavy, light = rows[0], rows[-1]
    report(f"{label} most carbon heavy {question}: {formatter(heavy[0])} "
           f"({heavy[1]:.4f} kg CO2 per trip on average)")
    report(f"{label} most carbon light {question}: {formatter(light[0])} "
           f"({light[1]:.4f} kg CO2 per trip on average)")


def monthly_totals(con, table):
    # question 6: monthly co2 totals used for the plot
    # sum total trip_co2_kgs by month
    rows = con.execute(f"""
        SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
        FROM {table}
        WHERE trip_co2_kgs IS NOT NULL AND month_of_year BETWEEN 1 AND 12
        GROUP BY month_of_year
        ORDER BY month_of_year;
    """).fetchall()
    totals = dict(rows)
    return [totals.get(month, 0) for month in range(1, 13)]


def plot_monthly_totals(totals_by_label, path=PLOT_PATH):
    fig, ax = plt.subplots(figsize=(10, 6))
    for label, totals in totals_by_label.items():
        ax.plot(MONTH_NAMES, totals, marker='o', label=f"{label} taxi")
    ax.set_title('2024 NYC taxi CO2 output by month')
    ax.set_xlabel('Month')
    ax.set_ylabel('Total CO2 (kg)')
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    report(f"Wrote monthly CO2 plot to {path}")


def analyze_trips():

    con = None

    try:
        con = duckdb.connect(database=DB_PATH, read_only=True)
        logger.info("Connected to DuckDB instance")

        totals_by_label = {}
        for table, label in TRIP_TABLES.items():
            # call all the tables
            largest_trip(con, table, label)
            heavy_and_light(con, table, label, 'hour_of_day', 'hour of day (0-23)')
            heavy_and_light(con, table, label, 'day_of_week', 'day of week',
                            formatter=lambda d: DAY_NAMES[d])
            heavy_and_light(con, table, label, 'week_of_year', 'week of year')
            heavy_and_light(con, table, label, 'month_of_year', 'month of year',
                            formatter=lambda m: MONTH_NAMES[m - 1])
            totals = monthly_totals(con, table)
            report(f"{label} total CO2 for 2024: {sum(totals):,.1f} kg")
            totals_by_label[label] = totals

        plot_monthly_totals(totals_by_label)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection")


if __name__ == "__main__":
    analyze_trips()
