
import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
FILE_PATH = (
    PROJECT_ROOT
    / "data"
    / "yellow_tripdata_2025-01.parquet"
)

# Snowflake private key
KEY_PATH = Path.home() / ".ssh/snowflake/rsa_key.p8"

private_key = serialization.load_pem_private_key(
    KEY_PATH.read_bytes(),
    password=None,
)

if not FILE_PATH.is_file():
    raise FileNotFoundError(FILE_PATH)

# Connect to Snowflake
with snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user="AIRFLOW_SVC",
    private_key=private_key,
    role="TRANSFORMER",
    warehouse="NYC_TAXI_WH",
    database="NYC_TAXI",
    schema="RAW",
) as conn:

    with conn.cursor() as cursor:

        # 1. Upload Parquet to internal stage
        print("Uploading January file...")

        cursor.execute(
            f"PUT 'file://{FILE_PATH.resolve()}' "
            "@NYC_TAXI.RAW.TLC_STAGE "
            "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
        )

        print("PUT:", cursor.fetchall())

        # 2. Load stage file into RAW table
        print("Loading January into RAW...")

        cursor.execute("""
            COPY INTO NYC_TAXI.RAW.YELLOW_TRIPDATA
            FROM @NYC_TAXI.RAW.TLC_STAGE
            FILES = ('yellow_tripdata_2025-01.parquet')
            FILE_FORMAT = (
                FORMAT_NAME = NYC_TAXI.RAW.PARQUET_FF
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (
                _source_file = METADATA$FILENAME,
                _loaded_at = METADATA$START_SCAN_TIME
            )
            ON_ERROR = ABORT_STATEMENT
        """)

        print("COPY:", cursor.fetchall())

        # 3. Count loaded rows
        cursor.execute("""
            SELECT COUNT(*)
            FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
        """)

        count = cursor.fetchone()[0]
        print(f"Total RAW rows: {count:,}")
