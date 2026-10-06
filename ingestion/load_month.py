
import argparse
import os
import re
from pathlib import Path

import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data"
)

STAGE = "NYC_TAXI.RAW.TLC_STAGE"
TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"


def download_month(month: str) -> Path:
    """Download the Parquet file for a given month."""

    if not re.fullmatch(r"2025-(01|02|03)", month):
        raise ValueError(
            "Month must be 2025-01, 2025-02 or 2025-03"
        )

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"yellow_tripdata_{month}.parquet"
    destination = DATA_DIR / filename

    if destination.is_file():
        print(f"File already exists: {destination}")
        return destination

    url = f"{BASE_URL}/{filename}"
    temporary = destination.with_suffix(".parquet.part")

    print(f"Downloading {url}")

    try:
        with requests.get(
            url, stream=True, timeout=120
        ) as response:
            response.raise_for_status()

            with temporary.open("wb") as file:
                for chunk in response.iter_content(
                    chunk_size=8 * 1024 * 1024
                ):
                    if chunk:
                        file.write(chunk)

        temporary.replace(destination)

    finally:
        temporary.unlink(missing_ok=True)

    return destination


def get_connection():
    """Connect to Snowflake with key-pair authentication."""

    key_path = (
        Path.home() / ".ssh/snowflake/rsa_key.p8"
    )

    private_key = serialization.load_pem_private_key(
        key_path.read_bytes(),
        password=None,
    )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user="AIRFLOW_SVC",
        private_key=private_key,
        role="TRANSFORMER",
        warehouse="NYC_TAXI_WH",
        database="NYC_TAXI",
        schema="RAW",
    )


def load_month(month: str):
    """Upload and load one month into the RAW table."""

    filepath = download_month(month)

    with get_connection() as conn:
        with conn.cursor() as cursor:

            print(f"Uploading {filepath.name}")

            cursor.execute(
                f"PUT 'file://{filepath.resolve()}' "
                f"@{STAGE} "
                "AUTO_COMPRESS=FALSE "
                "OVERWRITE=FALSE"
            )

            print("PUT:", cursor.fetchall())

            print(f"Loading {month} into Snowflake")

            cursor.execute(f"""
                COPY INTO {TABLE}
                FROM @{STAGE}
                FILES = ('{filepath.name}')
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

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {TABLE}
                WHERE _source_file = %s
                """,
                (filepath.name,),
            )

            count = cursor.fetchone()[0]

            print(
                f"Rows for {month}: {count:,}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load NYC Yellow Taxi monthly data"
    )

    parser.add_argument(
        "month",
        help="Month in YYYY-MM format",
    )

    args = parser.parse_args()
    load_month(args.month)
