
from pathlib import Path

from load_month import get_connection


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FILE_PATH = PROJECT_ROOT / "data" / "taxi_zone_lookup.csv"

STAGE = "NYC_TAXI.RAW.TLC_STAGE"
TABLE = "NYC_TAXI.RAW.TAXI_ZONE_LOOKUP"
FILE_FORMAT = "NYC_TAXI.RAW.CSV_FF"


def load_zones():
    """Upload and load the TLC taxi zone lookup."""

    if not FILE_PATH.is_file():
        raise FileNotFoundError(FILE_PATH)

    with get_connection() as conn:
        with conn.cursor() as cursor:

            # Upload the CSV to the internal stage
            print("Uploading taxi zones...")

            cursor.execute(
                f"PUT 'file://{FILE_PATH.resolve()}' "
                f"@{STAGE} "
                "AUTO_COMPRESS=FALSE "
                "OVERWRITE=FALSE"
            )

            print("PUT:", cursor.fetchall())

            # Load the CSV into RAW
            print("Loading taxi zones into Snowflake...")

            cursor.execute(f"""
                COPY INTO {TABLE}
                FROM @{STAGE}
                FILES = ('taxi_zone_lookup.csv')
                FILE_FORMAT = (
                    FORMAT_NAME = {FILE_FORMAT}
                )
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                INCLUDE_METADATA = (
                    _source_file = METADATA$FILENAME,
                    _loaded_at = METADATA$START_SCAN_TIME
                )
                ON_ERROR = ABORT_STATEMENT
            """)

            print("COPY:", cursor.fetchall())

            # Verify the loaded row count
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {TABLE}
            """)

            count = cursor.fetchone()[0]
            print(f"Total zones: {count}")

            if count != 265:
                raise ValueError(
                    f"Expected 265 zones, found {count}"
                )


if __name__ == "__main__":
    load_zones()
